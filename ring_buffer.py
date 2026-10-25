"""Zero-copy IPC bus: a single-producer / single-consumer ring buffer living in
a memory-mapped file.

Two *different* processes map the same file, so they see the same physical RAM
pages. The producer packs raw bytes into a slot with struct.pack_into (no
intermediate bytes object, no pickle); the consumer reads them with
struct.unpack_from (or, in the Cython engine, through a C pointer).
"""
import mmap
import os
import struct
import time

from config import (CAP_OFF, DEFAULT_CAPACITY, HEADER_SIZE, MAGIC, MAGIC_OFF,
                    READ_OFF, RECORD_FORMAT, RECORD_SIZE, WRITE_OFF,
                    default_ring_path)

_U64 = struct.Struct("<Q")
_REC = struct.Struct(RECORD_FORMAT)
assert _REC.size == RECORD_SIZE


class RingBuffer:
    def __init__(self, path=None, capacity=DEFAULT_CAPACITY, create=False):
        self.path = path or default_ring_path()
        if create:
            if capacity & (capacity - 1):
                raise ValueError("capacity must be a power of two")
            size = HEADER_SIZE + capacity * RECORD_SIZE
            with open(self.path, "w+b") as f:
                f.truncate(size)
            self._f = open(self.path, "r+b")
            self.mm = mmap.mmap(self._f.fileno(), size)
            _U64.pack_into(self.mm, WRITE_OFF, 0)
            _U64.pack_into(self.mm, READ_OFF, 0)
            _U64.pack_into(self.mm, CAP_OFF, capacity)
            _U64.pack_into(self.mm, MAGIC_OFF, MAGIC)
        else:
            self._f = open(self.path, "r+b")
            self.mm = mmap.mmap(self._f.fileno(), 0)
            if _U64.unpack_from(self.mm, MAGIC_OFF)[0] != MAGIC:
                raise RuntimeError("not a ChronosMatch ring buffer: " + self.path)
        self.capacity = _U64.unpack_from(self.mm, CAP_OFF)[0]
        self.mask = self.capacity - 1
        # local copies of our own index avoid a shared-memory read per call
        self._w = _U64.unpack_from(self.mm, WRITE_OFF)[0]
        self._r = _U64.unpack_from(self.mm, READ_OFF)[0]
        self._r_seen = self._r      # producer's cached view of consumer index
        self._w_seen = self._w      # consumer's cached view of producer index

    # ---------------------------------------------------------------- producer
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
        # publish AFTER the record bytes are written
        _U64.pack_into(self.mm, WRITE_OFF, w)
        return True

    # ---------------------------------------------------------------- consumer
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
        return rec[0], rec[1], rec[2], rec[3], rec[4]

    def pending(self):
        return _U64.unpack_from(self.mm, WRITE_OFF)[0] - _U64.unpack_from(self.mm, READ_OFF)[0]

    def close(self, unlink=False):
        self.mm.close()
        self._f.close()
        if unlink:
            try:
                os.unlink(self.path)
            except OSError:
                pass        # Windows: file still open in the other process - harmless
