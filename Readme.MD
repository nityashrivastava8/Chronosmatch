# ChronosMatch - Zero-Copy High-Frequency Trading Engine (Weeks 1-4)

## 0. Setup
```
pip install "cython>=3.0" setuptools        # Windows UI also: pip install windows-curses
python setup.py build_ext --inplace         # compiles matching_engine.pyx (needs a C compiler)
python verify_engine.py                     # 42 tests; none skipped once Cython is built
```
Windows C compiler: "Build Tools for Visual Studio" -> "Desktop development with C++".
Without the build every script still runs on the pure-Python engine (the baseline).

## 1. Architecture
```
market_firehose.py --(mmap order ring, 32-byte records)--> engine (Cython LOB) --(mmap trade ring, 48-byte)--> ledger_writer.py --> trades.db
   asyncio, 100k/s         zero-copy, no pickle           price-time priority, no GC          exactly-once SQLite, WAL
                                                          latency histogram + whale detector -> curses dashboard
```

## 2. Files
| File | Purpose | Week |
|---|---|---|
| config.py, ring_buffer.py | record layouts, order ring + trade ring | 1, 4 |
| market_firehose.py | asyncio order generator | 1 |
| matching_engine.pyx, engine_py.py | Cython engine / pure-Python baseline + fallback | 2-4 |
| engine_runner.py | engine process + curses dashboard (latency, ledger lag, WHALE banner) | 2-4 |
| ipc_audit.py, verify_engine.py | mid-review proofs | 2 |
| gc_proof.py | proves no Python objects / no GC during matching | 3 |
| benchmark.py | orders/s, latency p50/p99/p99.9/max -> RESULTS.md | 3 |
| ledger_writer.py, audit_ledger.py | SQLite ledger + audit | 4 |

## 3. Commands
```
python engine_runner.py --spawn-feed --spawn-ledger --feed-duration 30   # ONE-COMMAND DEMO (q to quit)
python engine_runner.py --no-ui --spawn-feed --spawn-ledger --feed-duration 10   # headless
python audit_ledger.py                      # ledger rows == engine trades, no gaps
python gc_proof.py                          # Cython: PASS = 0 objects, 0 GC runs
python benchmark.py --n 500000              # writes RESULTS.md + benchmark_results.json
python ipc_audit.py                         # 1M orders between 2 processes
python verify_engine.py
python acceptance_tests.py
```

## 4. What "correct" looks like
- `verify_engine.py` -> `OK`, 0 skipped. `gc_proof.py` (Cython) -> `PASS`.
- `audit_ledger.py` -> three `[PASS]` lines and `AUDIT PASSED`.
- engine_runner summary: `0 trades dropped`; ledger "stored N trades" equals engine trades.
- Dashboard: engine latency in microseconds (p50 / p99 / p99.9 / max), ledger backlog near 0,
  red `!!! WHALE ... cleared N price levels !!!` banner for ~4 s after each whale.

## 5. Week 3-4 design notes (for the report)
- **C-level optimization:** the matching loop uses only C structs/arrays; `cython -a` (setup.py writes
  `matching_engine.html`) should show no yellow lines inside `_submit`. gc_proof.py checks it at runtime.
- **Metrics:** C monotonic clock (same domain as `time.perf_counter_ns()`), stamped when an order enters
  and leaves the engine. Fixed log-linear histogram (512 buckets, ~12% resolution): no allocation per sample.
  `match` = time inside the engine; `e2e` = producer timestamp -> matched (includes ring wait).
- **Resiliency:** engine writes trades to a shared-memory trade ring and never waits for the database.
  The ledger commits to SQLite first and frees ring slots afterwards, rows keyed by (run_id, seq) with
  INSERT OR IGNORE -> a crash/restart cannot lose or duplicate a trade. If the ledger is dead the engine
  drops after a bounded wait and counts it (`trades_dropped`), the audit then accounts for it.
- **Whale:** an order that clears >= 3 price levels and fills >= 1000 shares.

## 6. Known limits
- One producer / one consumer per ring; no cancel/modify; prices 0..99,999 ticks.
- End-to-end latency includes time spent waiting in the ring while the engine handles a batch
  (20,000 orders per drain); lower BATCH in engine_runner.py for lower e2e at some throughput cost.
- On very fast machines the producer's Python `push` limits the firehose, not the engine.
- ClickHouse is not implemented (SQLite only).
