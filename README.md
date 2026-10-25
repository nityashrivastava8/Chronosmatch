# ChronosMatch - Mid-Project Review Kit (Weeks 1-2)

## 0. Setup 
```
python --version                      # 3.9+
pip install "cython>=3.0" setuptools  # Windows also: pip install windows-curses
python setup.py build_ext --inplace   # compiles matching_engine.pyx -> .so/.pyd
```
Linux/macOS need a C compiler (`sudo apt install build-essential` / `xcode-select --install`);
Windows needs "Build Tools for Visual Studio" (C++). Without the build, every script
still runs on the pure-Python engine (`engine_py.py`) - that is also your BASELINE.

## 1. Files
| File | Role | Review item |
|---|---|---|
| `config.py` | one place for record layout, constants | - |
| `ring_buffer.py` | mmap + struct ring buffer (Order ID, Price, Qty, ...) | Week 1 Memory Mapping |
| `market_firehose.py` | asyncio, 100,000 orders/s into the ring | Week 1 Market Firehose |
| `matching_engine.pyx` + `setup.py` | Cython Limit Order Book, price-time priority | Week 2 Cython Engine |
| `engine_py.py` | pure-Python engine (baseline + reference) | comparison |
| `engine_runner.py` | drains ring -> engine, curses Bid/Ask dashboard | Week 2 Latency Dashboard |
| `ipc_audit.py` | 1M orders between 2 processes vs pickling | Mid-review: IPC Audit |
| `verify_engine.py` | unit + fuzz tests of matching | Mid-review: Engine Verification |

## 2. Step-by-step plan
1. **Memory mapping (ring_buffer.py).** Read it top to bottom; be able to explain: 32-byte
   fixed record, header with write/read indices on separate cache lines, power-of-two
   capacity (`idx & mask` instead of modulo), producer publishes the index AFTER writing the record.
2. **Firehose.** Start the engine first (it creates the ring), then the firehose:
   `python engine_runner.py` (terminal 1) and `python market_firehose.py --rate 100000 --duration 20` (terminal 2).
3. **Cython engine.** Build it, read `matching_engine.pyx`. Key ideas: prices are array indices,
   each level is a linked-list FIFO (time priority), orders come from a pre-allocated node pool
   (no malloc, no Python objects, no GC during matching), `drain()` runs under `with nogil`
   and reads the shared memory through a C pointer.
4. **Dashboard.** `engine_runner.py` already shows top-10 bids/asks, spread, rate, trades. Press `q` to quit.
5. **Verify.** `python verify_engine.py` - all tests must pass for BOTH backends (23 tests, none skipped once Cython is built).
6. **Audit.** `python ipc_audit.py` (1M orders). Save the output for your report.
7. **Benchmark.** Run `python engine_runner.py --no-ui --duration 15` once with the .so
   present and once after temporarily renaming it; note orders/s for each.

.
