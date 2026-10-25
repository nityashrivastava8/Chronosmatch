# cython: language_level=3
# cython: boundscheck=False, wraparound=False, cdivision=True, initializedcheck=False
"""ChronosMatch matching engine -- Limit Order Book, price-time priority.

Design (everything inside the hot path is plain C -- no Python objects):
  * Prices are integer ticks; each tick is an index into flat C arrays, so
    finding a price level is O(1).
  * Every price level is a FIFO linked list (oldest order first = time
    priority). The list nodes live in ONE pre-allocated pool, so a trade
    never calls malloc and never creates a Python object.
  * Trades are written into a pre-allocated circular C array.
  * drain() reads orders straight out of the shared-memory ring buffer via
    a C pointer -> the "zero-copy" part of the project.

Requires Cython >= 3.0 (uses the `noexcept` keyword).
"""
from libc.stdint cimport int32_t, int64_t, uint8_t, uint32_t, uint64_t
from libc.stdlib cimport calloc, free, malloc

cdef extern from *:
    """
    #if defined(_MSC_VER)
    #include <intrin.h>
    #define CM_BARRIER() _ReadWriteBarrier()
    #else
    #define CM_BARRIER() __sync_synchronize()
    #endif
    """
    void CM_BARRIER() noexcept nogil


cdef struct Node:            # one resting order
    uint64_t order_id
    uint32_t qty
    int32_t next             # next node in the same price level, -1 = end

cdef struct Trade:
    uint64_t taker_id
    uint64_t maker_id
    int64_t price
    uint32_t qty
    uint64_t ts_ns

cdef struct RawOrder:        # MUST match config.RECORD_FORMAT (32 bytes)
    uint64_t order_id
    int64_t price
    uint32_t qty
    uint8_t side
    uint8_t pad0
    uint8_t pad1
    uint8_t pad2
    uint64_t ts_ns


cdef class MatchingEngine:
    cdef int64_t levels
    cdef Node* nodes
    cdef int32_t free_head
    cdef int32_t* bid_head
    cdef int32_t* bid_tail
    cdef int32_t* ask_head
    cdef int32_t* ask_tail
    cdef uint64_t* bid_qty
    cdef uint64_t* ask_qty
    cdef int64_t best_bid_px      # -1 when the bid side is empty
    cdef int64_t best_ask_px      # == levels when the ask side is empty
    cdef Trade* tlog
    cdef uint64_t tlog_cap
    cdef uint64_t _trades_count
    cdef uint64_t _orders_processed
    cdef uint64_t _rejected

    def __cinit__(self, int64_t levels=100000, int32_t max_orders=2097152,
                  uint64_t trade_log=1048576):
        cdef int64_t i
        if trade_log == 0 or (trade_log & (trade_log - 1)) != 0:
            raise ValueError("trade_log must be a power of two")
        self.levels = levels
        self.tlog_cap = trade_log
        self.nodes = <Node*> malloc(max_orders * sizeof(Node))
        self.bid_head = <int32_t*> malloc(levels * sizeof(int32_t))
        self.bid_tail = <int32_t*> malloc(levels * sizeof(int32_t))
        self.ask_head = <int32_t*> malloc(levels * sizeof(int32_t))
        self.ask_tail = <int32_t*> malloc(levels * sizeof(int32_t))
        self.bid_qty = <uint64_t*> calloc(levels, sizeof(uint64_t))
        self.ask_qty = <uint64_t*> calloc(levels, sizeof(uint64_t))
        self.tlog = <Trade*> calloc(trade_log, sizeof(Trade))
        if (self.nodes == NULL or self.bid_head == NULL or self.bid_tail == NULL
                or self.ask_head == NULL or self.ask_tail == NULL
                or self.bid_qty == NULL or self.ask_qty == NULL or self.tlog == NULL):
            raise MemoryError()
        for i in range(levels):
            self.bid_head[i] = -1
            self.bid_tail[i] = -1
            self.ask_head[i] = -1
            self.ask_tail[i] = -1
        for i in range(max_orders - 1):          # build the free list
            self.nodes[i].next = <int32_t>(i + 1)
        self.nodes[max_orders - 1].next = -1
        self.free_head = 0
        self.best_bid_px = -1
        self.best_ask_px = levels
        self._trades_count = 0
        self._orders_processed = 0
        self._rejected = 0

    def __dealloc__(self):
        free(self.nodes)
        free(self.bid_head)
        free(self.bid_tail)
        free(self.ask_head)
        free(self.ask_tail)
        free(self.bid_qty)
        free(self.ask_qty)
        free(self.tlog)

    # ------------------------------------------------------------ C-level core
    cdef inline void _record_trade(self, uint64_t taker, uint64_t maker,
                                   int64_t price, uint32_t qty, uint64_t ts) noexcept nogil:
        cdef Trade* t = &self.tlog[self._trades_count & (self.tlog_cap - 1)]
        t.taker_id = taker
        t.maker_id = maker
        t.price = price
        t.qty = qty
        t.ts_ns = ts
        self._trades_count += 1

    cdef inline bint _rest(self, uint64_t oid, int64_t price, uint32_t qty,
                           uint8_t side) noexcept nogil:
        cdef int32_t idx = self.free_head
        if idx == -1:
            return False                         # node pool exhausted
        self.free_head = self.nodes[idx].next
        self.nodes[idx].order_id = oid
        self.nodes[idx].qty = qty
        self.nodes[idx].next = -1
        if side == 0:
            if self.bid_tail[price] == -1:
                self.bid_head[price] = idx
            else:
                self.nodes[self.bid_tail[price]].next = idx
            self.bid_tail[price] = idx
            self.bid_qty[price] += qty
            if price > self.best_bid_px:
                self.best_bid_px = price
        else:
            if self.ask_tail[price] == -1:
                self.ask_head[price] = idx
            else:
                self.nodes[self.ask_tail[price]].next = idx
            self.ask_tail[price] = idx
            self.ask_qty[price] += qty
            if price < self.best_ask_px:
                self.best_ask_px = price
        return True

    cdef void _submit(self, uint64_t oid, int64_t price, uint32_t qty,
                      uint8_t side, uint64_t ts) noexcept nogil:
        cdef int64_t lvl
        cdef int32_t node, nxt
        cdef uint32_t fill
        self._orders_processed += 1
        if qty == 0 or price < 0 or price >= self.levels:
            self._rejected += 1
            return

        if side == 0:                            # ---- BUY: sweep the asks
            while qty > 0 and self.best_ask_px < self.levels and self.best_ask_px <= price:
                lvl = self.best_ask_px
                node = self.ask_head[lvl]
                while node != -1 and qty > 0:
                    fill = qty
                    if self.nodes[node].qty < fill:
                        fill = self.nodes[node].qty
                    self._record_trade(oid, self.nodes[node].order_id, lvl, fill, ts)
                    qty -= fill
                    self.nodes[node].qty -= fill
                    self.ask_qty[lvl] -= fill
                    if self.nodes[node].qty == 0:    # maker fully filled -> recycle node
                        nxt = self.nodes[node].next
                        self.nodes[node].next = self.free_head
                        self.free_head = node
                        node = nxt
                        self.ask_head[lvl] = nxt
                if self.ask_head[lvl] == -1:         # level empty -> move best ask up
                    self.ask_tail[lvl] = -1
                    lvl += 1
                    while lvl < self.levels and self.ask_head[lvl] == -1:
                        lvl += 1
                    self.best_ask_px = lvl
        else:                                    # ---- SELL: sweep the bids
            while qty > 0 and self.best_bid_px >= 0 and self.best_bid_px >= price:
                lvl = self.best_bid_px
                node = self.bid_head[lvl]
                while node != -1 and qty > 0:
                    fill = qty
                    if self.nodes[node].qty < fill:
                        fill = self.nodes[node].qty
                    self._record_trade(oid, self.nodes[node].order_id, lvl, fill, ts)
                    qty -= fill
                    self.nodes[node].qty -= fill
                    self.bid_qty[lvl] -= fill
                    if self.nodes[node].qty == 0:
                        nxt = self.nodes[node].next
                        self.nodes[node].next = self.free_head
                        self.free_head = node
                        node = nxt
                        self.bid_head[lvl] = nxt
                if self.bid_head[lvl] == -1:
                    self.bid_tail[lvl] = -1
                    lvl -= 1
                    while lvl >= 0 and self.bid_head[lvl] == -1:
                        lvl -= 1
                    self.best_bid_px = lvl

        if qty > 0:                              # unfilled remainder rests in the book
            if not self._rest(oid, price, qty, side):
                self._rejected += 1

    # --------------------------------------------------------- Python-facing API
    def submit(self, uint64_t order_id, int64_t price, uint32_t qty, uint8_t side,
               uint64_t ts_ns=0):
        """Process one order. Returns the number of fills it produced."""
        cdef uint64_t before = self._trades_count
        self._submit(order_id, price, qty, side, ts_ns)
        return self._trades_count - before

    def drain(self, unsigned char[::1] buf, uint64_t max_orders=0):
        """Match every pending order in the shared ring buffer (pass ring.mm).
        The whole loop runs with the GIL released and reads the orders through
        a raw C pointer into the shared memory -- nothing is copied."""
        cdef unsigned char* base = &buf[0]
        cdef uint64_t* hdr = <uint64_t*> base      # hdr[0]=write_idx hdr[1]=capacity hdr[8]=read_idx
        cdef uint64_t mask = hdr[1] - 1
        cdef uint64_t r = hdr[8]
        cdef uint64_t w
        cdef uint64_t n = 0
        cdef RawOrder* rec
        with nogil:
            CM_BARRIER()
            w = hdr[0]
            CM_BARRIER()
            while r < w and (max_orders == 0 or n < max_orders):
                rec = <RawOrder*> (base + 128 + (r & mask) * 32)
                self._submit(rec.order_id, rec.price, rec.qty, rec.side, rec.ts_ns)
                r += 1
                n += 1
            CM_BARRIER()
            hdr[8] = r
        return n

    @property
    def best_bid(self):
        return None if self.best_bid_px < 0 else self.best_bid_px

    @property
    def best_ask(self):
        return None if self.best_ask_px >= self.levels else self.best_ask_px

    @property
    def orders_processed(self):
        return self._orders_processed

    @property
    def rejected(self):
        return self._rejected

    @property
    def trades_count(self):
        return self._trades_count

    def depth(self, int n=10):
        """([(price, qty)... best bid first], [(price, qty)... best ask first])"""
        cdef int64_t p
        cdef int count
        bids = []
        asks = []
        p = self.best_bid_px
        count = 0
        while p >= 0 and count < n:
            if self.bid_head[p] != -1:
                bids.append((p, self.bid_qty[p]))
                count += 1
            p -= 1
        p = self.best_ask_px
        count = 0
        while p < self.levels and count < n:
            if self.ask_head[p] != -1:
                asks.append((p, self.ask_qty[p]))
                count += 1
            p += 1
        return bids, asks

    def recent_trades(self, int n=10):
        """Newest first: (taker_id, maker_id, price, qty, ts_ns)."""
        cdef uint64_t i
        cdef uint64_t avail = self._trades_count
        cdef Trade* t
        out = []
        if avail > self.tlog_cap:
            avail = self.tlog_cap
        for i in range(min(<uint64_t>n, avail)):
            t = &self.tlog[(self._trades_count - 1 - i) & (self.tlog_cap - 1)]
            out.append((t.taker_id, t.maker_id, t.price, t.qty, t.ts_ns))
        return out
