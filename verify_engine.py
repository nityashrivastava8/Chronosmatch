"""MID-REVIEW 'ENGINE VERIFICATION': prove the order book matches correctly.

    python verify_engine.py            # runs the full suite
The suite runs against the pure-Python engine always, and ALSO against the
compiled Cython engine when it has been built (python setup.py build_ext --inplace).
"""
import gc
import random
import sys
import threading
import time
import unittest

from config import BUY, SELL
from ring_buffer import RingBuffer, TradeRing
import engine_py
import ledger_writer

try:
    import matching_engine as cy
except ImportError:
    cy = None

SMALL = dict(levels=20_000, max_orders=200_000, trade_log=1 << 16)


class NaiveBook:
    """Deliberately dumb O(n) reference: scan every resting order each time.
    Independent of both real engines, so agreement means something."""
    def __init__(self):
        self.bids, self.asks, self.seq, self.trades = [], [], 0, []

    def submit(self, oid, price, qty, side):
        self.seq += 1
        mine, other = (self.bids, self.asks) if side == BUY else (self.asks, self.bids)
        while qty > 0 and other:
            if side == BUY:
                best = min(other, key=lambda o: (o[0], o[1]))
                if best[0] > price:
                    break
            else:
                best = min(other, key=lambda o: (-o[0], o[1]))
                if best[0] < price:
                    break
            fill = min(qty, best[3])
            self.trades.append((oid, best[2], best[0], fill))
            qty -= fill
            best[3] -= fill
            if best[3] == 0:
                other.remove(best)
        if qty > 0:
            mine.append([price, self.seq, oid, qty])

    def depth(self):
        def agg(book, rev):
            d = {}
            for p, _, _, q in book:
                d[p] = d.get(p, 0) + q
            return sorted(d.items(), reverse=rev)
        return agg(self.bids, True), agg(self.asks, False)


class EngineTests:
    Engine = None

    def new(self):
        return self.Engine(**SMALL)

    def test_buy_matches_resting_sell(self):
        e = self.new()
        e.submit(1, 10_000, 10, SELL)
        self.assertEqual(e.submit(2, 10_000, 10, BUY), 1)
        self.assertEqual(e.recent_trades(1)[0][:4], (2, 1, 10_000, 10))
        self.assertEqual(e.depth(5), ([], []))          # book is empty again

    def test_no_cross_rests_in_book(self):
        e = self.new()
        e.submit(1, 10_000, 5, SELL)
        self.assertEqual(e.submit(2, 9_990, 5, BUY), 0)
        self.assertEqual((e.best_bid, e.best_ask), (9_990, 10_000))

    def test_partial_fill_rests_remainder(self):
        e = self.new()
        e.submit(1, 10_000, 4, SELL)
        e.submit(2, 10_000, 10, BUY)
        self.assertEqual(e.depth(5), ([(10_000, 6)], []))

    def test_price_improvement_executes_at_maker_price(self):
        e = self.new()
        e.submit(1, 10_000, 5, SELL)
        e.submit(2, 10_050, 5, BUY)                      # willing to pay more
        self.assertEqual(e.recent_trades(1)[0][2], 10_000)

    def test_time_priority_at_same_price(self):
        e = self.new()
        e.submit(1, 10_000, 5, SELL)                     # older
        e.submit(2, 10_000, 5, SELL)
        e.submit(3, 10_000, 5, BUY)
        self.assertEqual(e.recent_trades(1)[0][:2], (3, 1))   # order 1 fills first

    def test_price_priority(self):
        e = self.new()
        e.submit(1, 10_010, 5, SELL)
        e.submit(2, 10_000, 5, SELL)                     # better price, arrived later
        e.submit(3, 10_010, 5, BUY)
        self.assertEqual(e.recent_trades(1)[0][:3], (3, 2, 10_000))

    def test_whale_sweeps_multiple_levels(self):
        e = self.new()
        for i, px in enumerate([10_000, 10_001, 10_002]):
            e.submit(i + 1, px, 10, SELL)
        self.assertEqual(e.submit(99, 10_002, 100, BUY), 3)
        self.assertEqual(e.depth(5), ([(10_002, 70)], []))     # remainder rests as a bid

    def test_sell_side_symmetry(self):
        e = self.new()
        e.submit(1, 10_000, 10, BUY)
        e.submit(2, 9_999, 10, BUY)
        e.submit(3, 9_999, 15, SELL)
        self.assertEqual([t[:4] for t in e.recent_trades(2)[::-1]],
                         [(3, 1, 10_000, 10), (3, 2, 9_999, 5)])

    def test_rejects_bad_orders(self):
        e = self.new()
        e.submit(1, 10_000, 0, BUY)
        e.submit(2, -5, 10, BUY)
        e.submit(3, 10**9, 10, SELL)
        self.assertEqual(e.rejected, 3)

    def test_fuzz_against_naive_reference(self):
        rng = random.Random(1234)
        e, ref = self.new(), NaiveBook()
        for oid in range(1, 6_001):
            side = rng.choice((BUY, SELL))
            price = 10_000 + rng.randint(-10, 10)        # tight range -> lots of crossing
            qty = rng.randint(1, 60)
            e.submit(oid, price, qty, side)
            ref.submit(oid, price, qty, side)
        got = [t[:4] for t in e.recent_trades(10**6)[::-1]]
        self.assertEqual(got, ref.trades)
        self.assertEqual(e.depth(10**6), ref.depth())

    # ------------------------------------------------------------------ Week 3
    def test_latency_histogram(self):
        e = self.new()
        rng = random.Random(5)
        for oid in range(1, 1_001):
            e.submit(oid, 10_000 + rng.randint(-5, 5), rng.randint(1, 20), rng.choice((0, 1)),
                     self.clock() - 1_000)                      # pretend created 1 us earlier
        st = e.latency_stats()
        self.assertEqual(st["match"]["count"], 1_000)
        self.assertEqual(st["e2e"]["count"], 1_000)
        m = st["match"]
        self.assertLessEqual(m["p50"], m["p99"])
        self.assertLessEqual(m["p99"], m["p999"])
        self.assertLessEqual(m["p999"], m["max"])
        self.assertGreaterEqual(st["e2e"]["p50"], 1_000 * 7 // 8)   # >= the 1 us we added (12% bucket error)
        e.reset_latency()
        self.assertEqual(e.latency_stats()["match"]["count"], 0)

    def test_latency_can_be_switched_off(self):
        e = self.Engine(measure=False, **SMALL)
        for oid in range(1, 101):
            e.submit(oid, 10_000, 5, oid & 1, self.clock())
        self.assertEqual(e.latency_stats()["match"]["count"], 0)
        self.assertEqual(e.orders_processed, 100)

    def test_bad_timestamps_do_not_poison_e2e(self):
        e = self.new()
        e.submit(1, 10_000, 5, BUY, self.clock() + 10**12)      # "from the future"
        e.submit(2, 10_000, 5, BUY, 1)                          # ancient / other clock
        st = e.latency_stats()
        self.assertEqual(st["e2e"]["count"], 0)
        self.assertEqual(st["e2e_skipped"], 2)

    # ------------------------------------------------------------------ Week 4
    def test_whale_detection(self):
        e = self.Engine(whale_levels=3, whale_qty=50, **SMALL)
        for i, px in enumerate([10_000, 10_001, 10_002, 10_003]):
            e.submit(i + 1, px, 100, SELL)
        e.submit(50, 10_003, 10, BUY)                           # small: 1 level, 10 shares
        self.assertEqual(e.whale_count, 0)
        e.submit(51, 10_003, 350, BUY)                          # clears 10_000 (rest), 10_001, 10_002 ...
        self.assertEqual(e.whale_count, 1)
        taker, side, levels, qty, first_px, last_px, _ = e.recent_whales(1)[0]
        self.assertEqual((taker, side, levels, qty), (51, BUY, 3, 350))
        self.assertEqual((first_px, last_px), (10_000, 10_003))

    def test_two_levels_is_not_a_whale(self):
        e = self.Engine(whale_levels=3, whale_qty=1, **SMALL)
        e.submit(1, 10_000, 10, SELL)
        e.submit(2, 10_001, 10, SELL)
        e.submit(3, 10_001, 20, BUY)
        self.assertEqual(e.whale_count, 0)

    def test_trades_stream_into_trade_ring(self):
        ring = RingBuffer("chronos_test.ring", 1 << 10, create=True)
        tring = TradeRing("chronos_test_t.ring", 1 << 10, create=True)
        try:
            e, direct = self.new(), self.new()
            rng = random.Random(11)
            for oid in range(1, 501):
                o = (oid, 10_000 + rng.randint(-5, 5), rng.randint(1, 30), rng.choice((0, 1)))
                ring.push(*o)
                direct.submit(*o)
            e.drain(ring.mm, 0, tring.mm)
            self.assertEqual(tring.pending(), direct.trades_count)
            recs, first, new_r = tring.read_batch(10_000)
            self.assertEqual(first, 0)
            self.assertEqual(len(recs), direct.trades_count)
            expected = [t[:4] for t in direct.recent_trades(10**6)[::-1]]
            self.assertEqual([(r[0], r[1], r[2], r[3]) for r in recs], expected)
            self.assertTrue(all(r[5] > 0 for r in recs))        # match_ns always stamped
            self.assertEqual(e.trades_dropped, 0)
        finally:
            ring.close(unlink=True)
            tring.close(unlink=True)

    def test_trade_ring_backpressure_with_live_consumer(self):
        """Tiny trade ring (16 slots) + a consumer thread: nothing may be lost or reordered."""
        ring = RingBuffer("chronos_test.ring", 1 << 12, create=True)
        tring = TradeRing("chronos_test_t.ring", 16, create=True)
        got, stop = [], threading.Event()

        def consumer():
            while True:
                recs, first, new_r = tring.read_batch(8)
                if recs:
                    got.extend((first + i, r[0], r[1]) for i, r in enumerate(recs))
                    tring.commit_read(new_r)
                elif stop.is_set() and tring.pending() == 0:
                    return
                else:
                    time.sleep(0)

        t = threading.Thread(target=consumer)
        try:
            e, direct = self.new(), self.new()
            rng = random.Random(3)
            for oid in range(1, 1_501):
                o = (oid, 10_000 + rng.randint(-4, 4), rng.randint(1, 40), rng.choice((0, 1)))
                ring.push(*o)
                direct.submit(*o)
            t.start()
            e.drain(ring.mm, 0, tring.mm)
            stop.set()
            t.join(timeout=30)
            self.assertFalse(t.is_alive())
            self.assertEqual(e.trades_dropped, 0)
            self.assertEqual(len(got), direct.trades_count)
            self.assertEqual([g[0] for g in got], list(range(direct.trades_count)))   # in order
            expected = [(t_[0], t_[1]) for t_ in direct.recent_trades(10**6)[::-1]]
            self.assertEqual([(g[1], g[2]) for g in got], expected)
        finally:
            stop.set()
            ring.close(unlink=True)
            tring.close(unlink=True)

    def test_ledger_stores_every_trade_exactly_once(self):
        ring = RingBuffer("chronos_test.ring", 1 << 11, create=True)
        tring = TradeRing("chronos_test_t.ring", 1 << 12, create=True)
        con = ledger_writer.connect(":memory:")
        try:
            e = self.new()
            rng = random.Random(21)
            for oid in range(1, 1_201):
                ring.push(oid, 10_000 + rng.randint(-5, 5), rng.randint(1, 30), rng.choice((0, 1)))
            e.drain(ring.mm, 0, tring.mm)
            stored = ledger_writer.flush_once(tring, con, max_batch=100)       # several batches
            while True:
                n = ledger_writer.flush_once(tring, con, max_batch=100)
                if not n:
                    break
                stored += n
            self.assertEqual(stored, e.trades_count)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM trades").fetchone()[0], e.trades_count)
            lo, hi = con.execute("SELECT MIN(seq), MAX(seq) FROM trades").fetchone()
            self.assertEqual((lo, hi), (0, e.trades_count - 1))
            # crash between COMMIT and commit_read: the writer restarts from the old read index
            tring.commit_read(0)
            self.assertEqual(ledger_writer.flush_once(tring, con, max_batch=10**6), e.trades_count)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM trades").fetchone()[0], e.trades_count)
        finally:
            con.close()
            ring.close(unlink=True)
            tring.close(unlink=True)

    def test_drain_from_shared_memory_ring(self):
        ring = RingBuffer("chronos_test.ring", 1 << 10, create=True)
        try:
            e, direct = self.new(), self.new()
            rng = random.Random(7)
            for oid in range(1, 801):
                o = (oid, 10_000 + rng.randint(-5, 5), rng.randint(1, 30), rng.choice((0, 1)))
                self.assertTrue(ring.push(*o))
                direct.submit(*o)
            self.assertEqual(e.drain(ring.mm), 800)
            self.assertEqual(ring.pending(), 0)
            self.assertEqual(e.trades_count, direct.trades_count)
            self.assertEqual(e.depth(50), direct.depth(50))
        finally:
            ring.close(unlink=True)


class TestPythonEngine(EngineTests, unittest.TestCase):
    Engine = engine_py.MatchingEngine
    clock = staticmethod(engine_py.clock_ns)


@unittest.skipIf(cy is None, "Cython engine not built (python setup.py build_ext --inplace)")
class TestCythonEngine(EngineTests, unittest.TestCase):
    Engine = cy.MatchingEngine if cy else None
    clock = staticmethod(cy.clock_ns) if cy else None

    def test_clock_matches_python_perf_counter(self):
        """Producer (Python) and engine (C) must share a clock, or end-to-end latency is meaningless."""
        a, b, c = time.perf_counter_ns(), cy.clock_ns(), time.perf_counter_ns()
        self.assertLessEqual(a - 1_000_000, b)
        self.assertLessEqual(b, c + 1_000_000)

    def test_drain_allocates_no_python_objects(self):
        ring = RingBuffer("chronos_test.ring", 1 << 15, create=True)
        try:
            for oid in range(1, 20_001):
                ring.push(oid, 10_000 + (oid % 9) - 4, 1 + oid % 50, oid & 1)
            e = self.new()
            gc.collect()
            before = sys.getallocatedblocks()
            e.drain(ring.mm)
            after = sys.getallocatedblocks()
            self.assertLess(abs(after - before), 50)
        finally:
            ring.close(unlink=True)


class TestRingBuffer(unittest.TestCase):
    def test_wraparound_and_full(self):
        ring = RingBuffer("chronos_test2.ring", 8, create=True)
        try:
            for i in range(8):
                self.assertTrue(ring.push(i, 100, 1, 0))
            self.assertFalse(ring.push(8, 100, 1, 0))        # full -> back-pressure
            self.assertEqual(ring.pop()[0], 0)               # free one slot
            self.assertTrue(ring.push(8, 100, 1, 0))         # now it fits
            nxt = 1
            for i in range(9, 200):                          # many laps around the ring
                self.assertEqual(ring.pop()[0], nxt)
                nxt += 1
                self.assertTrue(ring.push(i, 100, 1, 0))
            self.assertEqual(ring.pending(), 8)
        finally:
            ring.close(unlink=True)

    def test_trade_ring_roundtrip_wrap_and_close_flag(self):
        tr = TradeRing("chronos_test3.ring", 8, create=True)
        try:
            self.assertFalse(tr.is_closed())
            seq = 0
            for i in range(50):                              # 50 records through 8 slots
                self.assertTrue(tr.push(i, i + 1, 10_000 + i, 5, 111, 222))
                recs, first, new_r = tr.read_batch(100)
                self.assertEqual((first, len(recs)), (seq, 1))
                self.assertEqual(recs[0], (i, i + 1, 10_000 + i, 5, 111, 222))
                tr.commit_read(new_r)
                seq += 1
            for i in range(8):
                self.assertTrue(tr.push(i, 0, 1, 1, 0, 0))
            self.assertFalse(tr.push(9, 0, 1, 1, 0, 0))     # full
            tr.mark_closed()
            self.assertTrue(tr.is_closed())
            self.assertGreater(tr.run_id, 0)
        finally:
            tr.close(unlink=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
