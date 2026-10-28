"""Pure-Python limit order book (price-time priority).

Jobs:
  1. BASELINE for the Cython speed-up numbers (benchmark.py).
  2. REFERENCE implementation - verify_engine.py runs every test against it.
  3. FALLBACK so every script still runs before the Cython build works.

Exposes exactly the same API as matching_engine.pyx, including latency
histograms, whale detection and the trade-ring output.
"""
import heapq
import struct
import time
from collections import deque

from config import (HEADER_SIZE, PRICE_LEVELS, RECORD_FORMAT, RECORD_SIZE,
                    TRADE_FORMAT, TRADE_SIZE, WHALE_LEVELS, WHALE_MIN_QTY)

_U64 = struct.Struct("<Q")
_REC = struct.Struct(RECORD_FORMAT)
_TREC = struct.Struct(TRADE_FORMAT)

clock_ns = time.perf_counter_ns
_NBUCKETS = 512
_MAX_E2E_NS = 60_000_000_000


# ---- log-linear latency histogram (same maths as the C version) --------------
def bucket(v):
    if v < 8:
        return v
    msb = v.bit_length() - 1
    return (msb - 2) * 8 + ((v >> (msb - 3)) & 7)


def bucket_floor(i):
    if i < 8:
        return i
    return (8 + (i % 8)) << (i // 8 + 2 - 3)


def summarize(h, n, total, mx):
    if n == 0:
        return {"count": 0, "mean": 0.0, "p50": 0, "p99": 0, "p999": 0, "max": 0}
    targets = [(n * 50 + 99) // 100, (n * 99 + 99) // 100, (n * 999 + 999) // 1000]
    vals, cum, k = [0, 0, 0], 0, 0
    for i in range(_NBUCKETS):
        cum += h[i]
        while k < 3 and cum >= targets[k]:
            vals[k] = bucket_floor(i)
            k += 1
        if k == 3:
            break
    return {"count": n, "mean": total / n, "p50": vals[0], "p99": vals[1],
            "p999": vals[2], "max": mx}


class _TradeSink:
    """Writes trades straight into the shared trade ring (mmap)."""
    SPINS = 20_000

    def __init__(self, buf):
        self.buf = buf
        self.cap = _U64.unpack_from(buf, 8)[0]
        self.mask = self.cap - 1
        self.w = _U64.unpack_from(buf, 0)[0]
        self.r = _U64.unpack_from(buf, 64)[0]
        self.dropped = 0

    def write(self, taker, maker, price, qty, ts, t_enter):
        if self.w - self.r >= self.cap:
            _U64.pack_into(self.buf, 0, self.w)          # let the consumer see what is there
            spins = 0
            while True:
                self.r = _U64.unpack_from(self.buf, 64)[0]
                if self.w - self.r < self.cap:
                    break
                spins += 1
                if spins > self.SPINS:
                    self.dropped += 1
                    return
                time.sleep(0)
        _TREC.pack_into(self.buf, HEADER_SIZE + (self.w & self.mask) * TRADE_SIZE,
                        taker, maker, price, qty, ts, t_enter)
        self.w += 1

    def publish(self):
        _U64.pack_into(self.buf, 0, self.w)


class MatchingEngine:
    def __init__(self, levels=PRICE_LEVELS, max_orders=1 << 21, trade_log=1 << 20,
                 measure=True, whale_levels=WHALE_LEVELS, whale_qty=WHALE_MIN_QTY):
        self.levels = levels
        self.measure = measure
        self.whale_levels = whale_levels
        self.whale_qty = whale_qty
        self.bids, self.asks = {}, {}           # price -> deque([order_id, qty])
        self.bid_qty, self.ask_qty = {}, {}
        self._bid_heap, self._ask_heap = [], []  # bids stored negated -> max-heap
        self._trades = deque(maxlen=trade_log)
        self._whales = deque(maxlen=1024)
        self._sink = None
        self.orders_processed = 0
        self.rejected = 0
        self.trades_count = 0
        self.trades_dropped = 0
        self.whale_count = 0
        self.reset_latency()

    # ------------------------------------------------------------- latency
    def reset_latency(self):
        self._h_match = [0] * _NBUCKETS
        self._h_e2e = [0] * _NBUCKETS
        self._m_n = self._m_sum = self._m_max = 0
        self._e_n = self._e_sum = self._e_max = 0
        self.e2e_skipped = 0

    def latency_stats(self):
        """All values in nanoseconds. match = time inside the engine for one order;
        e2e = from the producer's timestamp to the end of matching."""
        return {"measure": self.measure,
                "match": summarize(self._h_match, self._m_n, self._m_sum, self._m_max),
                "e2e": summarize(self._h_e2e, self._e_n, self._e_sum, self._e_max),
                "e2e_skipped": self.e2e_skipped}

    # ---------------------------------------------------------------- core
    def submit(self, order_id, price, qty, side, ts_ns=0):
        """Process one order. Returns the number of fills it produced."""
        return self._process(order_id, price, qty, side, ts_ns)

    def _process(self, oid, price, qty, side, ts):
        t0 = time.perf_counter_ns()
        fills = self._match(oid, price, qty, side, ts, t0)
        if self.measure:
            t1 = time.perf_counter_ns()
            d = t1 - t0
            self._h_match[bucket(d)] += 1
            self._m_n += 1
            self._m_sum += d
            if d > self._m_max:
                self._m_max = d
            if ts:
                if t1 >= ts and t1 - ts < _MAX_E2E_NS:
                    e = t1 - ts
                    self._h_e2e[bucket(e)] += 1
                    self._e_n += 1
                    self._e_sum += e
                    if e > self._e_max:
                        self._e_max = e
                else:
                    self.e2e_skipped += 1
        return fills

    def _emit(self, taker, maker, price, qty, ts, t_enter):
        self.trades_count += 1
        self._trades.append((taker, maker, price, qty, ts))
        if self._sink is not None:
            self._sink.write(taker, maker, price, qty, ts, t_enter)

    def _match(self, order_id, price, qty, side, ts, t_enter):
        self.orders_processed += 1
        if qty <= 0 or price < 0 or price >= self.levels:
            self.rejected += 1
            return 0
        fills = filled = cleared = 0
        first_px = last_px = -1
        if side == 0:        # BUY sweeps the asks
            book, qtys, heap, sgn = self.asks, self.ask_qty, self._ask_heap, 1
            rest = (self.bids, self.bid_qty, self._bid_heap, -price)
        else:                # SELL sweeps the bids
            book, qtys, heap, sgn = self.bids, self.bid_qty, self._bid_heap, -1
            rest = (self.asks, self.ask_qty, self._ask_heap, price)
        while qty > 0 and heap:
            lvl = sgn * heap[0]
            if (lvl > price) if side == 0 else (lvl < price):
                break
            if first_px < 0:
                first_px = lvl
            last_px = lvl
            q = book[lvl]
            while q and qty > 0:
                maker = q[0]
                fill = min(qty, maker[1])
                self._emit(order_id, maker[0], lvl, fill, ts, t_enter)
                fills += 1
                filled += fill
                qty -= fill
                maker[1] -= fill
                qtys[lvl] -= fill
                if maker[1] == 0:
                    q.popleft()
            if not q:
                heapq.heappop(heap)
                del book[lvl]
                del qtys[lvl]
                cleared += 1
        if cleared >= self.whale_levels and filled >= self.whale_qty:
            self.whale_count += 1
            self._whales.append((order_id, side, cleared, filled, first_px, last_px, t_enter))
        if qty > 0:
            rbook, rqtys, rheap, rkey = rest
            if price not in rbook:
                rbook[price] = deque()
                rqtys[price] = 0
                heapq.heappush(rheap, rkey)
            rbook[price].append([order_id, qty])
            rqtys[price] += qty
        return fills

    # ------------------------------------------------------------- IPC drain
    def drain(self, buf, max_orders=0, tbuf=None):
        """Consume pending orders straight out of the shared mmap; write the
        resulting trades into the trade ring (tbuf) if one is given."""
        w = _U64.unpack_from(buf, 0)[0]
        r = _U64.unpack_from(buf, 64)[0]
        mask = _U64.unpack_from(buf, 8)[0] - 1
        sink = self._sink = _TradeSink(tbuf) if tbuf is not None else None
        n = 0
        process = self._process
        while r < w and (max_orders == 0 or n < max_orders):
            oid, price, qty, side, ts = _REC.unpack_from(buf, HEADER_SIZE + (r & mask) * RECORD_SIZE)
            process(oid, price, qty, side, ts)
            r += 1
            n += 1
        _U64.pack_into(buf, 64, r)
        if sink is not None:
            sink.publish()
            self.trades_dropped += sink.dropped
        self._sink = None
        return n

    # ------------------------------------------------------------ inspection
    @property
    def best_bid(self):
        return -self._bid_heap[0] if self._bid_heap else None

    @property
    def best_ask(self):
        return self._ask_heap[0] if self._ask_heap else None

    def depth(self, n=10):
        """([(price, qty)... best bid first], [(price, qty)... best ask first])"""
        bids = [(p, self.bid_qty[p]) for p in sorted(self.bid_qty, reverse=True)[:n]]
        asks = [(p, self.ask_qty[p]) for p in sorted(self.ask_qty)[:n]]
        return bids, asks

    def recent_trades(self, n=10):
        """Newest first: (taker_id, maker_id, price, qty, ts_ns)."""
        return list(self._trades)[::-1][:n]

    def recent_whales(self, n=5):
        """Newest first: (taker_id, side, levels_cleared, qty, first_px, last_px, match_ns)."""
        return list(self._whales)[::-1][:n]
