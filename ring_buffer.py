"""Zero-copy IPC buses built on memory-mapped files.

RingBuffer : orders, producer (firehose) -> engine          (32-byte records)
TradeRing  : trades, engine -> ledger writer                (48-byte records)

Both are single-producer / single-consumer rings. Two *different* processes map
the same file, so they see the same physical RAM pages. Records are written
with struct.pack_into (no intermediate bytes, no pickle) or, in the Cython
engine, through a C pointer.
"""
import mmap
import os
import struct
import time

from config import (CAP_OFF, CLOSED_OFF, DEFAULT_CAPACITY, DEFAULT_TRADE_CAPACITY,
                    HEADER_SIZE, MAGIC, MAGIC_OFF, MAGIC_TRADE, READ_OFF,
                    RECORD_FORMAT, RECORD_SIZE, RUN_OFF, TRADE_FORMAT, TRADE_SIZE,
                    WRITE_OFF, default_ring_path, default_trade_ring_path)

_U64 = struct.Struct("<Q")
_REC = struct.Struct(RECORD_FORMAT)
_TREC = struct.Struct(TRADE_FORMAT)
assert _REC.size == RECORD_SIZE and _TREC.size == TRADE_SIZE


class _Ring:
    """Shared plumbing: file + mmap + header."""
    REC_SIZE = RECORD_SIZE
    MAGIC = MAGIC

    def __init__(self, path, capacity, create):
        self.path = path
        if create:
            if capacity & (capacity - 1):
                raise ValueError("capacity must be a power of two")
            size = HEADER_SIZE + capacity * self.REC_SIZE
            with open(self.path, "w+b") as f:
                f.truncate(size)
            self._f = open(self.path, "r+b")
            self.mm = mmap.mmap(self._f.fileno(), size)
            _U64.pack_into(self.mm, WRITE_OFF, 0)
            _U64.pack_into(self.mm, READ_OFF, 0)
            _U64.pack_into(self.mm, CAP_OFF, capacity)
            _U64.pack_into(self.mm, CLOSED_OFF, 0)
            _U64.pack_into(self.mm, RUN_OFF, time.time_ns())
            _U64.pack_into(self.mm, MAGIC_OFF, self.MAGIC)
        else:
            self._f = open(self.path, "r+b")
            self.mm = mmap.mmap(self._f.fileno(), 0)
            if _U64.unpack_from(self.mm, MAGIC_OFF)[0] != self.MAGIC:
                raise RuntimeError("wrong ring type / not a ChronosMatch ring: " + self.path)
        self.capacity = _U64.unpack_from(self.mm, CAP_OFF)[0]
        self.mask = self.capacity - 1
        self._w = _U64.unpack_from(self.mm, WRITE_OFF)[0]
        self._r = _U64.unpack_from(self.mm, READ_OFF)[0]
        self._r_seen = self._r      # producer's cached view of the consumer index
        self._w_seen = self._w      # consumer's cached view of the producer index

    def pending(self):
        return _U64.unpack_from(self.mm, WRITE_OFF)[0] - _U64.unpack_from(self.mm, READ_OFF)[0]

    def close(self, unlink=False):
        self.mm.close()
        self._f.close()
        if unlink:
            try:
                os.unlink(self.path)
            except OSError:
                pass        # Windows: file still open in another process - harmless


class RingBuffer(_Ring):
    """Orders: (order_id, price, qty, side, ts_ns)."""

    def __init__(self, path=None, capacity=DEFAULT_CAPACITY, create=False):
        super().__init__(path or default_ring_path(), capacity, create)

    def push(self, order_id, price, qty, side, ts_ns=None):
        """Write one order. Returns False if the buffer is full (back-pressure)."""
        w = self._w
        if w - self._r_seen >= self.capacity:
            self._r_seen = _U64.unpack_from(self.mm, READ_OFF)[0]
            if w - self._r_seen >= self.capacity:
                return False
        if ts_ns is None:
            ts_ns = time.perf_counter_ns()
        _REC.pack_into(self.mm, HEADER_SIZE + (w & self.mask) * RECORD_SIZE,
                       order_id, price, qty, side, ts_ns)
        w += 1
        self._w = w
        _U64.pack_into(self.mm, WRITE_OFF, w)       # publish AFTER the record is written
        return True

    def pop(self):
        """Read one order as (order_id, price, qty, side, ts_ns) or None."""
        r = self._r
        if r >= self._w_seen:
            self._w_seen = _U64.unpack_from(self.mm, WRITE_OFF)[0]
            if r >= self._w_seen:
                return None
        rec = _REC.unpack_from(self.mm, HEADER_SIZE + (r & self.mask) * RECORD_SIZE)
        r += 1
        self._r = r
        _U64.pack_into(self.mm, READ_OFF, r)
        return rec


class TradeRing(_Ring):
    """Trades: (taker_id, maker_id, price, qty, ts_ns, match_ns).
    The engine is the producer; the ledger writer is the consumer. The consumer
    frees slots only AFTER the rows are committed to the database (commit_read),
    so a crash of the ledger never loses a trade that was still in the ring."""
    REC_SIZE = TRADE_SIZE
    MAGIC = MAGIC_TRADE

    def __init__(self, path=None, capacity=DEFAULT_TRADE_CAPACITY, create=False):
        super().__init__(path or default_trade_ring_path(), capacity, create)

    # -- producer side (used by tests and by the pure-Python engine fallback) --
    def push(self, taker, maker, price, qty, ts_ns, match_ns):
        w = self._w
        if w - self._r_seen >= self.capacity:
            self._r_seen = _U64.unpack_from(self.mm, READ_OFF)[0]
            if w - self._r_seen >= self.capacity:
                return False
        _TREC.pack_into(self.mm, HEADER_SIZE + (w & self.mask) * TRADE_SIZE,
                        taker, maker, price, qty, ts_ns, match_ns)
        self._w = w + 1
        _U64.pack_into(self.mm, WRITE_OFF, w + 1)
        return True

    def mark_closed(self):
        """Engine is done - the ledger may exit once the ring is empty."""
        _U64.pack_into(self.mm, CLOSED_OFF, 1)

    # -- consumer side ---------------------------------------------------------
    def is_closed(self):
        return _U64.unpack_from(self.mm, CLOSED_OFF)[0] == 1

    @property
    def run_id(self):
        return _U64.unpack_from(self.mm, RUN_OFF)[0]

    def read_batch(self, max_n=50_000):
        """Return (records, first_seq, new_read_index) WITHOUT freeing the slots.
        Record i has sequence number  first_seq + i. Call commit_read(new_read_index)
        once the records are safely stored."""
        r = _U64.unpack_from(self.mm, READ_OFF)[0]
        w = _U64.unpack_from(self.mm, WRITE_OFF)[0]
        n = min(w - r, max_n)
        if n <= 0:
            return [], r, r
        first = min(n, self.capacity - (r & self.mask))
        off = HEADER_SIZE + (r & self.mask) * TRADE_SIZE
        out = list(_TREC.iter_unpack(self.mm[off:off + first * TRADE_SIZE]))
        if n > first:
            out.extend(_TREC.iter_unpack(self.mm[HEADER_SIZE:HEADER_SIZE + (n - first) * TRADE_SIZE]))
        return out, r, r + n

    def commit_read(self, new_r):
        """Free the slots - call only after the data is safely stored."""
        _U64.pack_into(self.mm, READ_OFF, new_r)
