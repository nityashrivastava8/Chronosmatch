# cython: language_level=3
# cython: boundscheck=False, wraparound=False, cdivision=True, initializedcheck=False
"""ChronosMatch matching engine -- Limit Order Book, price-time priority.

Everything inside the hot path is plain C -- no Python objects, no malloc:
  * prices are integer ticks used as indexes into flat C arrays (O(1) lookup)
  * every price level is a FIFO linked list (oldest first = time priority)
    whose nodes come from ONE pre-allocated pool
  * trades go to a pre-allocated circular C array AND (optionally) straight
    into the shared-memory trade ring that the ledger process reads
  * a C monotonic clock timestamps every order; latencies go into a fixed
    log-linear histogram (no allocation, ~12% resolution, p50/p99/p99.9/max)
  * drain() runs with the GIL released and reads orders through a raw C
    pointer into the shared ring buffer -> zero copy

Requires Cython >= 3.0 (uses the `noexcept` keyword).
"""
from libc.stdint cimport int32_t, int64_t, uint8_t, uint32_t, uint64_t
from libc.stdlib cimport calloc, free, malloc

cdef extern from *:
    """
    #include <stdint.h>
    #if defined(_WIN32)
      #include <windows.h>
      #include <intrin.h>
      #define CM_BARRIER() MemoryBarrier()
      static inline uint64_t cm_now_ns(void) {
          static LARGE_INTEGER freq;
          LARGE_INTEGER c;
          if (freq.QuadPart == 0) QueryPerformanceFrequency(&freq);
          QueryPerformanceCounter(&c);
          return (uint64_t)(c.QuadPart / freq.QuadPart) * 1000000000ULL
               + (uint64_t)(c.QuadPart % freq.QuadPart) * 1000000000ULL / (uint64_t)freq.QuadPart;
      }
      static inline int cm_msb(uint64_t x) { unsigned long i; _BitScanReverse64(&i, x); return (int)i; }
    #else
      #include <time.h>
      #define CM_BARRIER() __sync_synchronize()
      static inline uint64_t cm_now_ns(void) {
          struct timespec ts;
          clock_gettime(CLOCK_MONOTONIC, &ts);
          return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
      }
      static inline int cm_msb(uint64_t x) { return 63 - __builtin_clzll(x); }
    #endif
    """
    void CM_BARRIER() noexcept nogil
    uint64_t cm_now_ns() noexcept nogil
    int cm_msb(uint64_t x) noexcept nogil


cdef struct Node:            # one resting order
    uint64_t order_id
    uint32_t qty
    int32_t next             # next node in the same price level, -1 = end

cdef struct Trade:           # 48 bytes - identical layout to config.TRADE_FORMAT
    uint64_t taker_id
    uint64_t maker_id
    int64_t price
    uint32_t qty
    uint32_t pad
    uint64_t ts_ns           # when the order was created (producer clock)
    uint64_t match_ns        # when the order entered the engine

cdef struct Whale:           # an order that cleared >= whale_levels price levels
    uint64_t taker_id
    int64_t first_px
    int64_t last_px
    uint64_t qty
    uint32_t levels
    uint32_t side
    uint64_t match_ns

cdef struct RawOrder:        # MUST match config.RECORD_FORMAT (32 bytes)
    uint64_t order_id
    int64_t price
    uint32_t qty
    uint8_t side
    uint8_t pad0
    uint8_t pad1
    uint8_t pad2
    uint64_t ts_ns



# ---- latency histogram helpers (same maths as engine_py.py) ----------------------
cdef inline int _bucket(uint64_t v) noexcept nogil:
    cdef int msb
    if v < 8:
        return <int> v
    msb = cm_msb(v)
    return (msb - 2) * 8 + <int> ((v >> (msb - 3)) & 7)

cdef inline uint64_t _bucket_floor(int idx) noexcept nogil:
    if idx < 8:
        return <uint64_t> idx
    return (<uint64_t> (8 + (idx % 8))) << (idx // 8 + 2 - 3)

cdef dict _summarize(uint64_t* h, uint64_t n, uint64_t total, uint64_t mx):
    cdef int i
    cdef uint64_t cum = 0
    cdef uint64_t t50, t99, t999
    cdef uint64_t v50 = 0, v99 = 0, v999 = 0
    cdef bint d50 = False, d99 = False, d999 = False
    if n == 0:
        return {"count": 0, "mean": 0.0, "p50": 0, "p99": 0, "p999": 0, "max": 0}
    t50 = (n * 50 + 99) // 100
    t99 = (n * 99 + 99) // 100
    t999 = (n * 999 + 999) // 1000
    for i in range(512):
        cum += h[i]
        if (not d50) and cum >= t50:
            v50 = _bucket_floor(i)
            d50 = True
        if (not d99) and cum >= t99:
            v99 = _bucket_floor(i)
            d99 = True
        if (not d999) and cum >= t999:
            v999 = _bucket_floor(i)
            d999 = True
    return {"count": n, "mean": (<double> total) / (<double> n), "p50": v50,
            "p99": v99, "p999": v999, "max": mx}


def clock_ns():
    """The engine's C clock (CLOCK_MONOTONIC / QueryPerformanceCounter).
    Same clock domain as Python's time.perf_counter_ns()."""
    return cm_now_ns()


cdef class MatchingEngine:
    cdef int64_t levels
    cdef int32_t whale_levels
    cdef uint64_t whale_qty
    cdef public bint measure
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
    cdef Whale* wlog
    cdef uint64_t _whale_count
    cdef uint64_t _trades_count
    cdef uint64_t _orders_processed
    cdef uint64_t _rejected
    # latency histograms
    cdef uint64_t* h_match
    cdef uint64_t* h_e2e
    cdef uint64_t match_n, match_sum, match_max
    cdef uint64_t e2e_n, e2e_sum, e2e_max, e2e_skipped
    # trade ring (only valid while drain() runs)
    cdef unsigned char* _tr_base
    cdef uint64_t _tr_cap, _tr_mask, _tr_w, _tr_r, _tr_dropped

    def __cinit__(self, int64_t levels=100000, int32_t max_orders=2097152,
                  uint64_t trade_log=1048576, bint measure=True, int32_t whale_levels=3,
                  uint64_t whale_qty=1000):
        cdef int64_t i
        if trade_log == 0 or (trade_log & (trade_log - 1)) != 0:
            raise ValueError("trade_log must be a power of two")
        self.levels = levels
        self.measure = measure
        self.whale_levels = whale_levels
        self.whale_qty = whale_qty
        self.tlog_cap = trade_log
        self.nodes = <Node*> malloc(max_orders * sizeof(Node))
        self.bid_head = <int32_t*> malloc(levels * sizeof(int32_t))
        self.bid_tail = <int32_t*> malloc(levels * sizeof(int32_t))
        self.ask_head = <int32_t*> malloc(levels * sizeof(int32_t))
        self.ask_tail = <int32_t*> malloc(levels * sizeof(int32_t))
        self.bid_qty = <uint64_t*> calloc(levels, sizeof(uint64_t))
        self.ask_qty = <uint64_t*> calloc(levels, sizeof(uint64_t))
        self.tlog = <Trade*> calloc(trade_log, sizeof(Trade))
        self.wlog = <Whale*> calloc(1024, sizeof(Whale))
        self.h_match = <uint64_t*> calloc(512, sizeof(uint64_t))
        self.h_e2e = <uint64_t*> calloc(512, sizeof(uint64_t))
        if (self.nodes == NULL or self.bid_head == NULL or self.bid_tail == NULL
                or self.ask_head == NULL or self.ask_tail == NULL
                or self.bid_qty == NULL or self.ask_qty == NULL or self.tlog == NULL
                or self.wlog == NULL or self.h_match == NULL or self.h_e2e == NULL):
            raise MemoryError()
        for i in range(levels):
            self.bid_head[i] = -1
            self.bid_tail[i] = -1
            self.ask_head[i] = -1
            self.ask_tail[i] = -1
        for i in range(max_orders - 1):          # build the free list
            self.nodes[i].next = <int32_t> (i + 1)
        self.nodes[max_orders - 1].next = -1
        self.free_head = 0
        self.best_bid_px = -1
        self.best_ask_px = levels
        self._tr_base = NULL

    def __dealloc__(self):
        free(self.nodes)
        free(self.bid_head)
        free(self.bid_tail)
        free(self.ask_head)
        free(self.ask_tail)
        free(self.bid_qty)
        free(self.ask_qty)
        free(self.tlog)
        free(self.wlog)
        free(self.h_match)
        free(self.h_e2e)

    # ------------------------------------------------------------ C-level core
    cdef inline void _emit_trade(self, Trade* t) noexcept nogil:
        """Copy one trade into the shared trade ring (ledger process reads it)."""
        cdef Trade* rec
        cdef uint64_t spins = 0
        while self._tr_w - self._tr_r >= self._tr_cap:       # ring full
            CM_BARRIER()
            (<uint64_t*> self._tr_base)[0] = self._tr_w      # let the consumer see what is there
            CM_BARRIER()
            self._tr_r = (<uint64_t*> self._tr_base)[8]
            if self._tr_w - self._tr_r < self._tr_cap:
                break
            spins += 1
            if spins > 2000000:                              # consumer is dead/stuck: drop, count it
                self._tr_dropped += 1
                return
        rec = <Trade*> (self._tr_base + 128 + (self._tr_w & self._tr_mask) * 48)
        rec[0] = t[0]
        self._tr_w += 1

    cdef inline void _record_trade(self, uint64_t taker, uint64_t maker, int64_t price,
                                   uint32_t qty, uint64_t ts, uint64_t t_enter) noexcept nogil:
        cdef Trade* t = &self.tlog[self._trades_count & (self.tlog_cap - 1)]
        t.taker_id = taker
        t.maker_id = maker
        t.price = price
        t.qty = qty
        t.pad = 0
        t.ts_ns = ts
        t.match_ns = t_enter
        self._trades_count += 1
        if self._tr_base != NULL:
            self._emit_trade(t)

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

    cdef inline void _record_whale(self, uint64_t oid, uint8_t side, uint32_t levels,
                                   uint64_t qty, int64_t first_px, int64_t last_px,
                                   uint64_t t_enter) noexcept nogil:
        cdef Whale* w = &self.wlog[self._whale_count & 1023]
        w.taker_id = oid
        w.first_px = first_px
        w.last_px = last_px
        w.qty = qty
        w.levels = levels
        w.side = side
        w.match_ns = t_enter
        self._whale_count += 1

    cdef void _submit(self, uint64_t oid, int64_t price, uint32_t qty, uint8_t side,
                      uint64_t ts, uint64_t t_enter) noexcept nogil:
        cdef int64_t lvl
        cdef int64_t first_px = -1
        cdef int64_t last_px = -1
        cdef int32_t node, nxt
        cdef uint32_t fill
        cdef uint32_t cleared = 0
        cdef uint64_t filled = 0
        self._orders_processed += 1
        if qty == 0 or price < 0 or price >= self.levels:
            self._rejected += 1
            return

        if side == 0:                            # ---- BUY: sweep the asks
            while qty > 0 and self.best_ask_px < self.levels and self.best_ask_px <= price:
                lvl = self.best_ask_px
                if first_px < 0:
                    first_px = lvl
                last_px = lvl
                node = self.ask_head[lvl]
                while node != -1 and qty > 0:
                    fill = qty
                    if self.nodes[node].qty < fill:
                        fill = self.nodes[node].qty
                    self._record_trade(oid, self.nodes[node].order_id, lvl, fill, ts, t_enter)
                    filled += fill
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
                    cleared += 1
                    self.ask_tail[lvl] = -1
                    lvl += 1
                    while lvl < self.levels and self.ask_head[lvl] == -1:
                        lvl += 1
                    self.best_ask_px = lvl
        else:                                    # ---- SELL: sweep the bids
            while qty > 0 and self.best_bid_px >= 0 and self.best_bid_px >= price:
                lvl = self.best_bid_px
                if first_px < 0:
                    first_px = lvl
                last_px = lvl
                node = self.bid_head[lvl]
                while node != -1 and qty > 0:
                    fill = qty
                    if self.nodes[node].qty < fill:
                        fill = self.nodes[node].qty
                    self._record_trade(oid, self.nodes[node].order_id, lvl, fill, ts, t_enter)
                    filled += fill
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
                    cleared += 1
                    self.bid_tail[lvl] = -1
                    lvl -= 1
                    while lvl >= 0 and self.bid_head[lvl] == -1:
                        lvl -= 1
                    self.best_bid_px = lvl

        if cleared >= <uint32_t> self.whale_levels and filled >= self.whale_qty:
            self._record_whale(oid, side, cleared, filled, first_px, last_px, t_enter)
        if qty > 0:                              # unfilled remainder rests in the book
            if not self._rest(oid, price, qty, side):
                self._rejected += 1

    cdef inline void _process(self, uint64_t oid, int64_t price, uint32_t qty, uint8_t side,
                              uint64_t ts) noexcept nogil:
        """One order, timed: entry stamp -> match -> exit stamp -> histograms."""
        cdef uint64_t t0 = cm_now_ns()
        cdef uint64_t t1, d
        cdef uint64_t limit_ns = 60000000000
        self._submit(oid, price, qty, side, ts, t0)
        if self.measure:
            t1 = cm_now_ns()
            d = 0
            if t1 >= t0:
                d = t1 - t0
            self.h_match[_bucket(d)] += 1
            self.match_n += 1
            self.match_sum += d
            if d > self.match_max:
                self.match_max = d
            if ts != 0:
                if t1 >= ts and (t1 - ts) < limit_ns:
                    d = t1 - ts
                    self.h_e2e[_bucket(d)] += 1
                    self.e2e_n += 1
                    self.e2e_sum += d
                    if d > self.e2e_max:
                        self.e2e_max = d
                else:
                    self.e2e_skipped += 1

    # --------------------------------------------------------- Python-facing API
    def submit(self, uint64_t order_id, int64_t price, uint32_t qty, uint8_t side,
               uint64_t ts_ns=0):
        """Process one order. Returns the number of fills it produced."""
        cdef uint64_t before = self._trades_count
        self._process(order_id, price, qty, side, ts_ns)
        return self._trades_count - before

    def drain(self, unsigned char[::1] buf, uint64_t max_orders=0,
              unsigned char[::1] tbuf=None):
        """Match every pending order in the shared order ring (pass ring.mm).
        If tbuf (the trade ring's mm) is given, every trade is also written there.
        The whole loop runs with the GIL released and reads orders through a raw C
        pointer into shared memory -- nothing is copied, no Python object is made."""
        cdef unsigned char* base = &buf[0]
        cdef uint64_t* hdr = <uint64_t*> base      # hdr[0]=write_idx hdr[1]=capacity hdr[8]=read_idx
        cdef uint64_t mask = hdr[1] - 1
        cdef uint64_t r = hdr[8]
        cdef uint64_t w
        cdef uint64_t n = 0
        cdef RawOrder* rec
        cdef uint64_t* thdr
        if tbuf is not None:
            self._tr_base = &tbuf[0]
            thdr = <uint64_t*> self._tr_base
            self._tr_cap = thdr[1]
            self._tr_mask = thdr[1] - 1
            self._tr_w = thdr[0]
            self._tr_r = thdr[8]
        else:
            self._tr_base = NULL
        with nogil:
            CM_BARRIER()
            w = hdr[0]
            CM_BARRIER()
            while r < w and (max_orders == 0 or n < max_orders):
                rec = <RawOrder*> (base + 128 + (r & mask) * 32)
                self._process(rec.order_id, rec.price, rec.qty, rec.side, rec.ts_ns)
                r += 1
                n += 1
            CM_BARRIER()
            hdr[8] = r
            if self._tr_base != NULL:
                CM_BARRIER()
                (<uint64_t*> self._tr_base)[0] = self._tr_w     # publish the new trades
                CM_BARRIER()
        self._tr_base = NULL          # never keep a pointer into memory we do not own
        return n

    # ---- counters / book inspection -------------------------------------------
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

    @property
    def trades_dropped(self):
        return self._tr_dropped

    @property
    def whale_count(self):
        return self._whale_count

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
        for i in range(min(<uint64_t> n, avail)):
            t = &self.tlog[(self._trades_count - 1 - i) & (self.tlog_cap - 1)]
            out.append((t.taker_id, t.maker_id, t.price, t.qty, t.ts_ns))
        return out

    def recent_whales(self, int n=5):
        """Newest first: (taker_id, side, levels_cleared, qty, first_px, last_px, match_ns)."""
        cdef uint64_t i
        cdef uint64_t avail = self._whale_count
        cdef Whale* w
        out = []
        if avail > 1024:
            avail = 1024
        for i in range(min(<uint64_t> n, avail)):
            w = &self.wlog[(self._whale_count - 1 - i) & 1023]
            out.append((w.taker_id, w.side, w.levels, w.qty, w.first_px, w.last_px, w.match_ns))
        return out

    # ---- latency --------------------------------------------------------------
    def reset_latency(self):
        cdef int i
        for i in range(512):
            self.h_match[i] = 0
            self.h_e2e[i] = 0
        self.match_n = 0
        self.match_sum = 0
        self.match_max = 0
        self.e2e_n = 0
        self.e2e_sum = 0
        self.e2e_max = 0
        self.e2e_skipped = 0

    def latency_stats(self):
        """All values in nanoseconds. match = time inside the engine for one order;
        e2e = from the producer's timestamp to the end of matching."""
        return {"measure": bool(self.measure),
                "match": _summarize(self.h_match, self.match_n, self.match_sum, self.match_max),
                "e2e": _summarize(self.h_e2e, self.e2e_n, self.e2e_sum, self.e2e_max),
                "e2e_skipped": self.e2e_skipped}
