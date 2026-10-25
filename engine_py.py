"""Pure-Python limit order book (price-time priority).

Two jobs:
  1. WEEK-2 BASELINE: the number you compare the Cython engine against.
  2. REFERENCE IMPLEMENTATION: verify_engine.py fuzz-tests the Cython engine
     against this one, so a mismatch means a bug in one of them.

It exposes exactly the same API as matching_engine.pyx.
"""
import heapq
import struct
from collections import deque

from config import HEADER_SIZE, PRICE_LEVELS, RECORD_FORMAT, RECORD_SIZE

_U64 = struct.Struct("<Q")
_REC = struct.Struct(RECORD_FORMAT)


class MatchingEngine:
    def __init__(self, levels=PRICE_LEVELS, max_orders=1 << 21, trade_log=1 << 20):
        self.levels = levels
        self.bids = {}            # price -> deque([order_id, qty])
        self.asks = {}
        self.bid_qty = {}         # price -> total resting qty
        self.ask_qty = {}
        self._bid_heap = []       # max-heap via negated prices
        self._ask_heap = []
        self._trades = deque(maxlen=trade_log)
        self.orders_processed = 0
        self.rejected = 0
        self.trades_count = 0

    # ------------------------------------------------------------------ core
    def submit(self, order_id, price, qty, side, ts_ns=0):
        """Process one order. Returns the number of fills it produced."""
        self.orders_processed += 1
        if qty <= 0 or price < 0 or price >= self.levels:
            self.rejected += 1
            return 0
        fills = 0
        if side == 0:   # BUY: match against asks while ask price <= limit
            book, qtys, heap = self.asks, self.ask_qty, self._ask_heap
            while qty > 0 and heap and heap[0] <= price:
                lvl = heap[0]
                q = book[lvl]
                while q and qty > 0:
                    maker = q[0]
                    fill = min(qty, maker[1])
                    self._trades.append((order_id, maker[0], lvl, fill, ts_ns))
                    self.trades_count += 1
                    fills += 1
                    qty -= fill
                    maker[1] -= fill
                    qtys[lvl] -= fill
                    if maker[1] == 0:
                        q.popleft()
                if not q:
                    heapq.heappop(heap)
                    del book[lvl]
                    del qtys[lvl]
            if qty > 0:
                self._rest(self.bids, self.bid_qty, self._bid_heap, -price, order_id, price, qty)
        else:           # SELL: match against bids while bid price >= limit
            book, qtys, heap = self.bids, self.bid_qty, self._bid_heap
            while qty > 0 and heap and -heap[0] >= price:
                lvl = -heap[0]
                q = book[lvl]
                while q and qty > 0:
                    maker = q[0]
                    fill = min(qty, maker[1])
                    self._trades.append((order_id, maker[0], lvl, fill, ts_ns))
                    self.trades_count += 1
                    fills += 1
                    qty -= fill
                    maker[1] -= fill
                    qtys[lvl] -= fill
                    if maker[1] == 0:
                        q.popleft()
                if not q:
                    heapq.heappop(heap)
                    del book[lvl]
                    del qtys[lvl]
            if qty > 0:
                self._rest(self.asks, self.ask_qty, self._ask_heap, price, order_id, price, qty)
        return fills

    @staticmethod
    def _rest(book, qtys, heap, heap_key, order_id, price, qty):
        if price not in book:
            book[price] = deque()
            qtys[price] = 0
            heapq.heappush(heap, heap_key)
        book[price].append([order_id, qty])
        qtys[price] += qty

    # ------------------------------------------------------------- IPC drain
    def drain(self, buf, max_orders=0):
        """Consume pending orders straight out of the shared mmap."""
        w = _U64.unpack_from(buf, 0)[0]
        r = _U64.unpack_from(buf, 64)[0]
        mask = _U64.unpack_from(buf, 8)[0] - 1
        n = 0
        while r < w and (max_orders == 0 or n < max_orders):
            oid, price, qty, side, ts = _REC.unpack_from(buf, HEADER_SIZE + (r & mask) * RECORD_SIZE)
            self.submit(oid, price, qty, side, ts)
            r += 1
            n += 1
        _U64.pack_into(buf, 64, r)
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
