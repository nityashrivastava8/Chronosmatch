"""ChronosMatch engine process: creates the shared rings, drains orders into the
matching engine, streams trades to the ledger, shows a live dashboard.

ONE-COMMAND DEMO (engine + 100k orders/s firehose + SQLite ledger):
    python engine_runner.py --spawn-feed --spawn-ledger --feed-duration 30

Manual (3 terminals):
    python engine_runner.py                       # 1) engine + dashboard (press q to quit)
    python market_firehose.py --rate 100000       # 2) market feed
    python ledger_writer.py                       # 3) ledger

Headless / no curses:  add --no-ui --duration 20     (Windows UI: pip install windows-curses)
"""
import argparse
import json
import os
import subprocess
import sys
import time

from config import (DEFAULT_CAPACITY, DEFAULT_DB, DEFAULT_TRADE_CAPACITY,
                    default_ring_path, default_trade_ring_path)
from engine_loader import BACKEND, MatchingEngine, clock_ns
from ring_buffer import RingBuffer, TradeRing

BATCH = 20_000              # orders drained per loop pass (keeps the UI responsive)
HERE = os.path.dirname(os.path.abspath(__file__))
WHALE_BANNER_NS = 4_000_000_000


def us(ns):
    return f"{ns / 1000:,.1f}"


def latency_line(stats):
    m = stats["match"]
    if not stats["measure"] or m["count"] == 0:
        return "latency: (no samples)"
    return (f"engine latency us   p50 {us(m['p50'])}   p99 {us(m['p99'])}   "
            f"p99.9 {us(m['p999'])}   max {us(m['max'])}   mean {us(m['mean'])}")


def e2e_line(stats):
    e = stats["e2e"]
    if e["count"] == 0:
        return "end-to-end us      (no samples)"
    return f"end-to-end us      p50 {us(e['p50'])}   p99 {us(e['p99'])}   max {us(e['max'])}   (producer -> matched)"


def drain_loop(engine, ring, tring, feed, duration, draw):
    """Shared by the UI and headless modes. `draw(now)` returns True to quit."""
    tbuf = tring.mm if tring is not None else None
    t0 = time.perf_counter()
    next_draw = 0.0
    while True:
        n = engine.drain(ring.mm, BATCH, tbuf)
        now = time.perf_counter()
        if n == 0:
            time.sleep(0.0005)
            if draw is None and feed is not None and feed.poll() is not None and ring.pending() == 0:
                return                                   # headless: feed finished and fully drained
        if draw is not None and now >= next_draw:
            next_draw = now + 0.1
            if draw(now):
                return
        if duration and now - t0 >= duration:
            return


def make_dashboard(engine, ring, tring, feed):
    import curses

    def run(stdscr):
        curses.curs_set(0)
        stdscr.nodelay(True)
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_GREEN, -1)
        curses.init_pair(2, curses.COLOR_RED, -1)
        curses.init_pair(3, curses.COLOR_CYAN, -1)
        curses.init_pair(4, curses.COLOR_WHITE, curses.COLOR_RED)       # whale banner
        curses.init_pair(5, curses.COLOR_YELLOW, -1)
        G, R, C, W, Y = (curses.color_pair(i) for i in (1, 2, 3, 4, 5))
        state = {"last_t": time.perf_counter(), "last_n": 0, "rate": 0.0, "stats": engine.latency_stats(),
                 "stats_t": 0.0}

        def put(y, x, text, attr=0):
            try:
                stdscr.addstr(y, x, text, attr)
            except curses.error:
                pass                                    # terminal too small - ignore

        def draw(now):
            if stdscr.getch() in (ord("q"), ord("Q")):
                return True
            n = engine.orders_processed
            if now - state["last_t"] >= 0.5:
                state["rate"] = (n - state["last_n"]) / (now - state["last_t"])
                state["last_t"], state["last_n"] = now, n
            if now - state["stats_t"] >= 0.5:
                state["stats"], state["stats_t"] = engine.latency_stats(), now

            bids, asks = engine.depth(10)
            bb, ba = engine.best_bid, engine.best_ask
            spread = (ba - bb) if (bb is not None and ba is not None) else None
            whales = engine.recent_whales(1)
            whale_live = bool(whales) and clock_ns() - whales[0][6] < WHALE_BANNER_NS
            feed_state = "" if feed is None else ("  FEED DONE" if feed.poll() is not None else "  feed running")

            stdscr.erase()
            put(0, 0, f" CHRONOSMATCH  engine={BACKEND}{feed_state}   (q = quit)", C | curses.A_BOLD)
            put(1, 0, f" orders {n:>12,}  trades {engine.trades_count:>10,}  rate {state['rate']:>10,.0f}/s  "
                      f"ring backlog {ring.pending():>7,}")
            led = f"  ledger backlog {tring.pending():>7,}  dropped {engine.trades_dropped}" if tring else "  (no ledger)"
            put(2, 0, f" best bid {bb if bb is not None else '-':>6}  best ask {ba if ba is not None else '-':>6}  "
                      f"spread {spread if spread is not None else '-'} ticks{led}")
            put(3, 0, " " + latency_line(state["stats"]), Y)
            put(4, 0, " " + e2e_line(state["stats"]), Y)
            if whale_live:
                tk, side, lv, q, p1, p2, _ = whales[0]
                put(5, 0, f" !!! WHALE {'BUY' if side == 0 else 'SELL'} order #{tk} cleared {lv} price levels "
                          f"({p1} -> {p2}), {q:,} shares !!! ".ljust(100), W | curses.A_BOLD)
            else:
                put(5, 0, f" whales so far: {engine.whale_count:,} (orders clearing >= 3 price levels)", curses.A_DIM)
            put(7, 2, "BIDS  price     qty", G | curses.A_BOLD)
            put(7, 40, "ASKS  price     qty", R | curses.A_BOLD)
            top = max([q for _, q in bids + asks] or [1])
            for i, (p, q) in enumerate(bids):
                put(8 + i, 2, f"     {p:>7} {q:>7} " + "#" * max(1, 14 * q // top), G)
            for i, (p, q) in enumerate(asks):
                put(8 + i, 40, f"     {p:>7} {q:>7} " + "#" * max(1, 14 * q // top), R)
            put(20, 2, "LAST TRADES   taker   maker   price   qty", C | curses.A_BOLD)
            whale_taker = whales[0][0] if whale_live else None
            for i, (tk, mk, px, q, _) in enumerate(engine.recent_trades(8)):
                attr = (W | curses.A_BOLD) if tk == whale_taker else 0
                put(21 + i, 2, f"             {tk:>7} {mk:>7} {px:>7} {q:>5}", attr)
            stdscr.refresh()
            return False

        drain_loop(engine, ring, tring, feed, 0, draw)

    return run


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--path", default=default_ring_path())
    ap.add_argument("--trade-path", default=default_trade_ring_path())
    ap.add_argument("--capacity", type=int, default=DEFAULT_CAPACITY)
    ap.add_argument("--trade-capacity", type=int, default=DEFAULT_TRADE_CAPACITY)
    ap.add_argument("--no-ui", action="store_true")
    ap.add_argument("--no-ledger", action="store_true", help="do not create the trade ring")
    ap.add_argument("--no-measure", action="store_true", help="switch off per-order latency timing")
    ap.add_argument("--duration", type=float, default=0, help="headless seconds (0 = until feed ends / Ctrl-C)")
    ap.add_argument("--spawn-feed", action="store_true", help="start market_firehose.py automatically")
    ap.add_argument("--spawn-ledger", action="store_true", help="start ledger_writer.py automatically")
    ap.add_argument("--rate", type=int, default=100_000)
    ap.add_argument("--feed-duration", type=float, default=30)
    ap.add_argument("--whale-prob", type=float, default=0.00002)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--summary", default="run_summary.json")
    a = ap.parse_args()

    ring = RingBuffer(a.path, a.capacity, create=True)
    tring = None if a.no_ledger else TradeRing(a.trade_path, a.trade_capacity, create=True)
    engine = MatchingEngine(measure=not a.no_measure)
    print(f"engine backend: {BACKEND}; order ring: {a.path}"
          + ("" if tring is None else f"; trade ring: {a.trade_path}; ledger db: {a.db}"))

    py = sys.executable
    ledger = feed = None
    try:
        if a.spawn_ledger and tring is not None:
            ledger = subprocess.Popen([py, os.path.join(HERE, "ledger_writer.py"), "--db", a.db,
                                       "--path", a.trade_path], cwd=os.getcwd())
        if a.spawn_feed:
            feed = subprocess.Popen([py, os.path.join(HERE, "market_firehose.py"), "--rate", str(a.rate),
                                     "--duration", str(a.feed_duration), "--path", a.path,
                                     "--whale-prob", str(a.whale_prob)], cwd=os.getcwd())
        try:
            if a.no_ui:
                t0 = time.perf_counter()
                drain_loop(engine, ring, tring, feed, a.duration, None)
                print(f"drained for {time.perf_counter() - t0:.1f}s")
            else:
                import curses
                curses.wrapper(make_dashboard(engine, ring, tring, feed))
        except KeyboardInterrupt:
            pass
        finally:
            if feed is not None and feed.poll() is None:
                feed.terminate()
            while engine.drain(ring.mm, BATCH, tring.mm if tring else None):
                pass                                  # last orders -> trades -> trade ring
            if tring is not None:
                tring.mark_closed()                   # tell the ledger: nothing more is coming
                if ledger is not None:
                    try:
                        ledger.wait(timeout=120)
                    except subprocess.TimeoutExpired:
                        ledger.terminate()
    finally:
        stats = engine.latency_stats()
        summary = {"backend": BACKEND, "orders_processed": engine.orders_processed,
                   "trades_count": engine.trades_count, "trades_dropped": engine.trades_dropped,
                   "rejected": engine.rejected, "whales": engine.whale_count, "latency_ns": stats}
        with open(a.summary, "w") as f:
            json.dump(summary, f, indent=2)
        ring.close(unlink=True)
        if tring is not None:
            tring.close(unlink=True)

    m = stats["match"]
    print(f"[{BACKEND}] processed {engine.orders_processed:,} orders, {engine.trades_count:,} trades, "
          f"{engine.whale_count:,} whales, {engine.trades_dropped} trades dropped")
    if stats["measure"] and m["count"]:
        print(f"engine latency (us): p50 {us(m['p50'])}  p99 {us(m['p99'])}  p99.9 {us(m['p999'])}  max {us(m['max'])}")
        e = stats["e2e"]
        if e["count"]:
            print(f"end-to-end   (us): p50 {us(e['p50'])}  p99 {us(e['p99'])}  max {us(e['max'])}")
    bids, asks = engine.depth(3)
    print("best bid/ask:", engine.best_bid, engine.best_ask, "| top bids", bids, "| top asks", asks)
    print(f"summary written to {a.summary}")


if __name__ == "__main__":
    main()
