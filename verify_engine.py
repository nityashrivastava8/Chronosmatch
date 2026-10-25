"""MID-REVIEW 'ENGINE VERIFICATION': prove the order book matches correctly.

    python verify_engine.py            # runs the full suite
The suite runs against the pure-Python engine always, and ALSO against the
compiled Cython engine when it has been built (python setup.py build_ext --inplace).
"""
import random
import unittest

from config import BUY, SELL
from ring_buffer import RingBuffer
import engine_py

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


@unittest.skipIf(cy is None, "Cython engine not built (python setup.py build_ext --inplace)")
class TestCythonEngine(EngineTests, unittest.TestCase):
    Engine = cy.MatchingEngine if cy else None


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


if __name__ == "__main__":
    unittest.main(verbosity=2)
