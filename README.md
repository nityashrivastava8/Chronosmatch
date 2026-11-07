# ChronosMatch ⚡ Zero-Copy High-Frequency Trading Engine

![Python](https://img.shields.io/badge/python-3.9%2B-blue)
![Cython](https://img.shields.io/badge/cython-3.x-orange)
![SQLite](https://img.shields.io/badge/ledger-SQLite-lightgrey)
![Tests](https://img.shields.io/badge/tests-42-brightgreen)

A low-latency **limit order book** written in Python + Cython. Processes share data through
**memory-mapped ring buffers** (no pickling), orders are matched by **price-time priority** in C-level
code (no Python objects, no garbage-collector pauses), every trade is saved to a **SQLite ledger**, and a
live terminal dashboard shows the book, latency and large "whale" orders.

> Internship project - Python Development, Infotact Solutions.

## Why this project
Python is usually dismissed for high-frequency trading: the garbage collector causes latency spikes, the GIL limits
speed, and passing data between processes needs slow serialization (JSON/pickle). ChronosMatch shows how to work
around all three with `mmap`, `struct` and Cython.

## Features
- **Zero-copy IPC:** 32-byte order records in a shared-memory ring buffer (single producer / single consumer, lock-free)
- **Cython matching engine:** price-time priority, partial fills, multi-level sweeps, pre-allocated order pool
- **Latency metrics:** C monotonic clock, p50 / p99 / p99.9 / max per order (histogram, no per-order allocation)
- **Resilient ledger:** trades stream through a second shared-memory ring to SQLite; exactly-once, crash-safe
- **Live dashboard:** curses UI with bid/ask depth, spread, throughput, latency and a red **WHALE** banner
- **Proof tools:** unit + fuzz tests, GC proof, 1M-order IPC audit, ledger audit, benchmark

## Architecture
```mermaid
flowchart LR
    F["Market firehose<br/>asyncio, 100k orders/s"] -->|"mmap order ring<br/>32-byte records"| E["Matching engine<br/>Cython LOB, price-time priority"]
    E -->|"mmap trade ring<br/>48-byte records"| L["Ledger writer<br/>SQLite, WAL"]
    E --> D["Terminal dashboard<br/>book, latency, whales"]
```

## Tech stack
| Area | Tools |
|---|---|
| Language | Python 3.9+, Cython 3.x (C extension) |
| IPC | `mmap`, `struct`, shared-memory ring buffers |
| Concurrency | `asyncio` (market feed), `multiprocessing` / `subprocess` |
| Storage | SQLite (WAL mode) |
| UI | `curses` (`windows-curses` on Windows) |
| Testing | `unittest`, fuzz test vs naive reference, `gc` / `sys.getallocatedblocks` |

## Project structure
```
config.py              record layouts and constants
ring_buffer.py         RingBuffer (orders) and TradeRing (trades) on mmap
market_firehose.py     asyncio order generator          order_gen.py   deterministic generator
matching_engine.pyx    Cython limit order book          engine_py.py   pure-Python baseline / fallback
engine_loader.py       picks Cython if built, else Python
engine_runner.py       engine process + dashboard       ledger_writer.py   ring -> SQLite
verify_engine.py       42 unit/fuzz tests               acceptance_tests.py   one check per requirement
ipc_audit.py           1M-order IPC audit               gc_proof.py        no-GC proof
audit_ledger.py        ledger audit                     ledger_report.py   summary + small CSV sample
benchmark.py           Python vs Cython benchmark       REQUIREMENTS.md    requirement -> evidence table
```

## Installation
```powershell
git clone https://github.com/nityashrivastava8/Chronosmatch.git
cd Chronosmatch
python -m venv venv
venv\Scripts\Activate.ps1                      # Linux/macOS: source venv/bin/activate
pip install "cython>=3.0" setuptools
pip install windows-curses                     # Windows only (for the dashboard)
python setup.py build_ext --inplace            # optional: build the Cython engine
```
The Cython build needs a C compiler (Windows: *Build Tools for Visual Studio* with the C++ workload;
Linux: `build-essential`). **Without it everything still runs** on the pure-Python engine, which is the baseline.

## Running
```bash
# one-command demo: engine + 100k orders/s feed + SQLite ledger (press q to quit)
python engine_runner.py --spawn-feed --spawn-ledger --feed-duration 30

# no dashboard (e.g. no curses)
python engine_runner.py --no-ui --spawn-feed --spawn-ledger --feed-duration 10

python audit_ledger.py            # ledger rows == engine trades, no gaps
python ledger_report.py           # ledger summary
python verify_engine.py           # tests
python acceptance_tests.py        # one PASS/FAIL line per requirement
python gc_proof.py                # no Python objects / GC runs while matching (Cython)
python benchmark.py               # writes RESULTS.md
python ipc_audit.py               # 1M orders between two processes vs pickle
```
`trades.db` is created by each run and is not committed (see `docs/sample_trades.csv` for a sample).

## Screenshots

### Live dashboard
Order book, spread, throughput, latency in microseconds, ledger backlog and the red **WHALE** alert (100,000 orders/s):

[Live dashboard with whale alert]<img width="891" height="692" alt="dashboard_2" src="https://github.com/user-attachments/assets/7b34c496-373c-41b0-87b3-14fb56d17cae" />


### Run summary (3,000,000 orders, 30 s)
[Run summary]<img width="988" height="429" alt="dashboard_3" src="https://github.com/user-attachments/assets/ed7e20ba-0605-40e6-a570-fa6f2a3e5ed4" />


### Verification and audits
| Unit tests (42) | Acceptance tests |
|---|---|
| [Unit tests]<img width="992" height="700" alt="verify_engine" src="https://github.com/user-attachments/assets/79d8043c-4be2-4f0d-8cbe-d3159f5a122c" />
 | [Acceptance tests]<img width="972" height="958" alt="acceptance_tests" src="https://github.com/user-attachments/assets/f64428a0-154d-491a-b720-b0af5f330e92" />
 |

| Ledger audit | IPC audit (1M orders) |
|---|---|
| [Ledger audit]<img width="993" height="461" alt="audit_check" src="https://github.com/user-attachments/assets/a7789fe2-a808-428c-ad10-a4087253ebe0" />
 | [IPC audit]<img width="991" height="464" alt="ipc_audit" src="https://github.com/user-attachments/assets/5d7b4549-a243-422c-822b-2befebfc5925" />
 |

| Benchmark | GC proof (pure-Python baseline) |
|---|---|
| [Benchmark]<img width="970" height="509" alt="benchmark" src="https://github.com/user-attachments/assets/973a14ca-f9ec-4522-b2b6-14d23626d237" />
 | [GC proof]<img width="986" height="483" alt="gc_proof" src="https://github.com/user-attachments/assets/8b9085d1-ff4b-43d7-b4af-9474dfe5f303" />
 |

> These runs used the pure-Python engine (`engine=python`); the Cython engine could not be built on this machine.
> The GC-proof screenshot therefore shows the *baseline* (objects allocated, GC runs) that the Cython engine is designed to avoid.

## Results
Measured on my laptop (12 CPU cores, Windows, Python engine). Full output in `RESULTS.md`.

| Metric | Result |
|---|---|
| Live run | 3,000,000 orders at ~100,000/s, 2,027,156 trades, **0 trades dropped** |
| Ledger audit | 2,027,156 rows in SQLite = engine trades, no gaps or duplicates (**PASS**) |
| Engine latency (live run) | p50 1.0 us, p99 7.2 us, p99.9 24.6 us (rare pauses reach milliseconds in pure Python) |
| Engine throughput (`benchmark.py`, 500k orders) | 616,658 orders/s, p99 3.1 us |
| IPC audit | 1,000,000 orders between 2 processes in 0.58 s, integrity OK, **12.7x faster** than a pickled `multiprocessing.Queue` |
| Tests | 42 unit/fuzz tests OK; acceptance tests 10 passed, 0 failed, 1 skipped (needs Cython) |
| Cython engine | implemented (`matching_engine.pyx`) but not benchmarked - C build not available on this machine |

## Scope
**In scope:** shared-memory IPC, limit order book (buy/sell, price-time priority), latency measurement, ledger and
audit, live terminal dashboard, tests and benchmarks.
**Out of scope:** real exchange connectivity (the feed is simulated), order cancel/modify, multiple symbols,
multiple producers/consumers, ClickHouse (SQLite only), production hardening.

## Future work
Order cancel/modify, multi-symbol books, real market-data adapter, ClickHouse ledger, Prometheus metrics.

## Author
NITYA SHRIVASTAV - Python Development Intern, Infotact Solutions
