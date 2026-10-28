"""Shared constants for ChronosMatch. Every module imports from here so the
binary layout of an order record is defined in exactly ONE place."""
import os
import tempfile

# ---- Order record layout (32 bytes, little-endian) -------------------------
#   offset  size  field
#     0      8    order_id   uint64
#     8      8    price      int64   (in ticks, 1 tick = 1 cent)
#    16      4    qty        uint32
#    20      1    side       uint8   (0 = BUY, 1 = SELL)
#    21      3    padding
#    24      8    ts_ns      uint64  (time.perf_counter_ns() at creation)
RECORD_FORMAT = "<QqIB3xQ"
RECORD_SIZE = 32

BUY = 0
SELL = 1

# ---- Ring buffer header (128 bytes) ----------------------------------------
#   0   write_idx (uint64)  <- only the producer writes this
#   8   capacity  (uint64)  <- number of slots, power of two
#  16   magic     (uint64)
#  64   read_idx  (uint64)  <- only the consumer writes this
# write_idx and read_idx sit on different 64-byte cache lines (avoids false sharing).
HEADER_SIZE = 128
WRITE_OFF = 0
CAP_OFF = 8
MAGIC_OFF = 16
READ_OFF = 64
MAGIC = 0xC4120905

DEFAULT_CAPACITY = 1 << 20          # 1,048,576 slots = 32 MiB

# ---- Market parameters -----------------------------------------------------
PRICE_LEVELS = 100_000              # engine supports prices 0 .. 99,999 ticks
MID_PRICE = 10_000                  # $100.00


# ---- Trade ring (engine -> ledger writer), 48-byte records ------------------
#   taker_id u64 | maker_id u64 | price i64 | qty u32 | pad u32 |
#   ts_ns u64 (when the order was created) | match_ns u64 (when it entered the engine)
TRADE_FORMAT = "<QQqI4xQQ"
TRADE_SIZE = 48
MAGIC_TRADE = 0xC4120906
CLOSED_OFF = 24                     # header: 1 = engine finished, ledger may exit when empty
RUN_OFF = 32                        # header: run id (time.time_ns() when the ring was created)
DEFAULT_TRADE_CAPACITY = 1 << 18    # 262,144 slots = 12 MiB

WHALE_LEVELS = 3                    # a "whale" clears >= 3 price levels ...
WHALE_MIN_QTY = 1000                # ... AND fills at least this many shares
DEFAULT_DB = "trades.db"


def default_trade_ring_path() -> str:
    return default_ring_path() + ".trades"


def default_ring_path() -> str:
    """Shared-memory file. /dev/shm is a RAM-backed filesystem on Linux; on
    macOS/Windows we fall back to the temp dir (still a memory-mapped file)."""
    base = "/dev/shm" if os.path.isdir("/dev/shm") else tempfile.gettempdir()
    return os.path.join(base, "chronosmatch.ring")
