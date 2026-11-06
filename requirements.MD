# ChronosMatch - Requirements Traceability

Project: **ChronosMatch: Zero-Copy High-Frequency Trading Engine** (Infotact Solutions, Ref. ITS/PY/1161)

Run everything with one command: `python acceptance_tests.py` (add `--full` for the 1,000,000-order audit).

| # | Requirement (from the project document) | Evidence / test | Expected result |
|---|---|---|---|
| 1 | Zero-copy IPC bus: ring buffer on `mmap` + `struct` holding Order ID, Price, Quantity | `acceptance_tests.py` #1, `ring_buffer.py` | fields intact over 5,000 writes incl. wrap-around |
| 2 | Market simulator (asyncio): 100,000 mock orders per second into the IPC bus | `acceptance_tests.py` #2, `market_firehose.py` | about 100,000 orders/s, 0 ring-full stalls |
| 3 | **Mid-review - IPC audit:** 1M orders between two Python processes without pickling | `python ipc_audit.py`, `acceptance_tests.py --full` #3 | integrity OK (order + checksum), faster than `multiprocessing.Queue` |
| 4 | Limit Order Book, Price-Time Priority, in Cython | `python verify_engine.py`, `matching_engine.pyx` | 42 tests OK, 0 skipped after the Cython build |
| 5 | **Mid-review - engine verification:** Buy order matches a Sell order instantly | `acceptance_tests.py` #5, `verify_engine.py` | 1 trade, book empty |
| 6 | Order book / bid-ask spread shown in real time on a terminal dashboard (curses) | live: `python engine_runner.py --spawn-feed --spawn-ledger` | bids green, asks red, best bid < best ask, spread shown |
| 7 | Matches trades in under 50 microseconds | `acceptance_tests.py` #6, `benchmark.py` | engine p99 < 50 us (Cython) |
| 8 | Python garbage collector never triggers during a trade | `python gc_proof.py`, `acceptance_tests.py` #8 | 0 objects allocated, 0 GC runs (Cython) |
| 9 | `time.perf_counter_ns()` timestamps measure when a trade enters and exits the engine | `acceptance_tests.py` #9, dashboard latency panel | p50 / p99 / p99.9 / max per order |
| 10 | Background process flushes matched trades to a permanent SQLite ledger for auditing | `python audit_ledger.py`, `acceptance_tests.py` #10 | ledger rows = engine trades, no gaps or duplicates, 0 dropped |
| 11 | Terminal UI highlights when a "Whale" order clears multiple price levels | `acceptance_tests.py` #11, live dashboard banner | red WHALE banner; latest whale cleared >= 3 levels |
| 12 | Final result: sub-millisecond execution times | `acceptance_tests.py` #7, `RESULTS.md` | engine p99.9 < 1 ms |
| 13 | Deliverables: source code, documentation, report, PPT, evaluation results, demo | repository, `RESULTS.md`, `docs/` | see repo |

## Live demo script (about 3 minutes)
1. `python acceptance_tests.py` - table of PASS results.
2. `python engine_runner.py --spawn-feed --spawn-ledger --feed-duration 30` - dashboard: order book, latency, ledger backlog, WHALE banner. Press `q`.
3. `python audit_ledger.py` - `AUDIT PASSED`.
4. `python benchmark.py` - open `RESULTS.md` (Cython vs pure-Python).

## Limits
Single producer / single consumer per ring, no order cancel, prices 0..99,999 ticks, simulated market feed, SQLite ledger only (ClickHouse not implemented).
