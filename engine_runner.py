"""Consumer side: creates the ring buffer, drains orders into the matching
engine and shows a live order-book dashboard (curses).

    python engine_runner.py              # live dashboard, press q to quit
    python engine_runner.py --no-ui --duration 15   # headless, prints a summary
(Windows: pip install windows-curses)
"""
import argparse
import time

from config import DEFAULT_CAPACITY, default_ring_path
from engine_loader import BACKEND, MatchingEngine
from ring_buffer import RingBuffer

BATCH = 20_000     # orders drained per loop pass (keeps the UI responsive)


def headless(engine, ring, duration):
    t0 = time.perf_counter()
    while duration == 0 or time.perf_counter() - t0 < duration:
        if engine.drain(ring.mm, BATCH) == 0:
            time.sleep(0.0005)
    dt = time.perf_counter() - t0
    bids, asks = engine.depth(5)
    print(f"[{BACKEND}] processed {engine.orders_processed:,} orders, "
          f"{engine.trades_count:,} trades in {dt:.1f}s")
    print("best bid/ask:", engine.best_bid, engine.best_ask)
    print("top bids:", bids)
    print("top asks:", asks)


def dashboard(stdscr, engine, ring):
    import curses
    curses.curs_set(0)
    stdscr.nodelay(True)
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(1, curses.COLOR_GREEN, -1)
    curses.init_pair(2, curses.COLOR_RED, -1)
    curses.init_pair(3, curses.COLOR_CYAN, -1)
    GREEN, RED, CYAN = curses.color_pair(1), curses.color_pair(2), curses.color_pair(3)

    def put(y, x, text, attr=0):
        try:
            stdscr.addstr(y, x, text, attr)
        except curses.error:
            pass                                    # terminal too small - ignore

    last_t, last_n, rate = time.perf_counter(), 0, 0.0
    next_draw = 0.0
    while True:
        if engine.drain(ring.mm, BATCH) == 0:
            time.sleep(0.0005)
        now = time.perf_counter()
        if now < next_draw:
            continue
        next_draw = now + 0.1                       # redraw 10x per second
        if stdscr.getch() in (ord("q"), ord("Q")):
            return
        n = engine.orders_processed
        if now - last_t >= 0.5:
            rate, last_t, last_n = (n - last_n) / (now - last_t), now, n

        bids, asks = engine.depth(10)
        bb, ba = engine.best_bid, engine.best_ask
        spread = (ba - bb) if (bb is not None and ba is not None) else None
        stdscr.erase()
        put(0, 0, f" CHRONOSMATCH  engine={BACKEND}  (q = quit)", CYAN | curses.A_BOLD)
        put(1, 0, f" orders {n:>12,}   trades {engine.trades_count:>10,}   "
                  f"rate {rate:>10,.0f}/s   ring backlog {ring.pending():>8,}")
        put(2, 0, f" best bid {bb if bb is not None else '-':>7}   "
                  f"best ask {ba if ba is not None else '-':>7}   "
                  f"spread {spread if spread is not None else '-'} ticks")
        put(4, 2, "BIDS  price     qty", GREEN | curses.A_BOLD)
        put(4, 40, "ASKS  price     qty", RED | curses.A_BOLD)
        top = max([q for _, q in bids + asks] or [1])
        for i, (p, q) in enumerate(bids):
            put(5 + i, 2, f"     {p:>7} {q:>7} " + "#" * max(1, 14 * q // top), GREEN)
        for i, (p, q) in enumerate(asks):
            put(5 + i, 40, f"     {p:>7} {q:>7} " + "#" * max(1, 14 * q // top), RED)
        put(17, 2, "LAST TRADES   taker   maker   price   qty", CYAN | curses.A_BOLD)
        for i, (tk, mk, px, q, _) in enumerate(engine.recent_trades(8)):
            put(18 + i, 2, f"             {tk:>7} {mk:>7} {px:>7} {q:>5}")
        stdscr.refresh()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--path", default=default_ring_path())
    ap.add_argument("--capacity", type=int, default=DEFAULT_CAPACITY)
    ap.add_argument("--no-ui", action="store_true")
    ap.add_argument("--duration", type=float, default=0, help="headless seconds (0 = until Ctrl-C)")
    a = ap.parse_args()

    ring = RingBuffer(a.path, a.capacity, create=True)
    engine = MatchingEngine()
    print(f"engine backend: {BACKEND}; ring: {a.path}; waiting for orders...")
    try:
        if a.no_ui:
            headless(engine, ring, a.duration)
        else:
            import curses
            curses.wrapper(dashboard, engine, ring)
            print(f"processed {engine.orders_processed:,} orders, {engine.trades_count:,} trades")
    except KeyboardInterrupt:
        pass
    finally:
        ring.close(unlink=True)


if __name__ == "__main__":
    main()
