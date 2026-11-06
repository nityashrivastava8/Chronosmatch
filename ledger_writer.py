"""Resiliency: a background process that moves matched trades from the shared
trade ring into a permanent SQLite ledger for auditing.

Guarantees
  * The engine never waits for the database - it only writes to shared memory.
  * Ring slots are freed ONLY AFTER the rows are committed, so a crash of this
    process loses nothing: on restart it re-reads the uncommitted trades.
  * Rows are keyed by (run_id, seq); re-inserting after a crash is a no-op
    (INSERT OR IGNORE) -> every trade is stored exactly once.
  * Batched transactions + WAL mode -> hundreds of thousands of rows/second.

Run (the engine_runner can also start it for you with --spawn-ledger):
    python ledger_writer.py --db trades.db
"""
import argparse
import sqlite3
import time

from config import DEFAULT_DB, default_trade_ring_path
from ring_buffer import TradeRing

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    run_id      INTEGER NOT NULL,   -- which engine run (ring creation time)
    seq         INTEGER NOT NULL,   -- position in the trade ring, 0,1,2,...
    taker_id    INTEGER NOT NULL,   -- aggressing order
    maker_id    INTEGER NOT NULL,   -- resting order
    price       INTEGER NOT NULL,   -- ticks
    qty         INTEGER NOT NULL,
    order_ts_ns INTEGER NOT NULL,   -- producer clock when the order was created
    match_ns    INTEGER NOT NULL,   -- engine clock when the order entered the engine
    PRIMARY KEY (run_id, seq)
) WITHOUT ROWID;
"""


def connect(db):
    con = sqlite3.connect(db, isolation_level=None)      # we manage transactions ourselves
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.executescript(SCHEMA)
    return con


def flush_once(ring, con, max_batch=50_000):
    """Read one batch from the ring, commit it, THEN free the ring slots.
    Returns the number of trades stored."""
    batch, first_seq, new_r = ring.read_batch(max_batch)
    if not batch:
        return 0
    run_id = ring.run_id
    rows = [(run_id, first_seq + i, t[0], t[1], t[2], t[3], t[4], t[5])
            for i, t in enumerate(batch)]
    con.execute("BEGIN")
    con.executemany("INSERT OR IGNORE INTO trades VALUES (?,?,?,?,?,?,?,?)", rows)
    con.execute("COMMIT")
    ring.commit_read(new_r)             # durable -> now the engine may reuse the slots
    return len(rows)


def run(db, path, max_batch=50_000, idle_exit=0, quiet=False):
    ring = TradeRing(path)
    con = connect(db)
    total, t0, last_data = 0, time.perf_counter(), time.perf_counter()
    try:
        while True:
            n = flush_once(ring, con, max_batch)
            if n:
                total += n
                last_data = time.perf_counter()
                continue
            if ring.is_closed() and ring.pending() == 0:
                break                                    # engine finished, everything stored
            if idle_exit and time.perf_counter() - last_data > idle_exit:
                break
            time.sleep(0.002)
    except KeyboardInterrupt:
        while flush_once(ring, con, max_batch):          # store what is left, then stop
            pass
    finally:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        con.close()
        ring.close()
    dt = time.perf_counter() - t0
    if not quiet:
        print(f"[ledger] stored {total:,} trades in {dt:.1f}s -> {db}")
    return total


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--path", default=default_trade_ring_path())
    ap.add_argument("--batch", type=int, default=50_000)
    ap.add_argument("--idle-exit", type=float, default=0, help="exit after N idle seconds (0 = wait for engine to close the ring)")
    a = ap.parse_args()
    # the engine creates the ring - wait for it
    for _ in range(200):
        try:
            TradeRing(a.path).close()
            break
        except (FileNotFoundError, RuntimeError):
            time.sleep(0.1)
    run(a.db, a.path, a.batch, a.idle_exit)


if __name__ == "__main__":
    main()
