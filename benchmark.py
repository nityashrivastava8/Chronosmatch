"""Benchmark + latency report for the final write-up ("model evaluation results").

Pre-loads N orders into the ring, then times engine.drain() for every engine that
is available (pure Python always, Cython if you built it), with latency
measurement on and off. Writes benchmark_results.json and RESULTS.md.

    python benchmark.py [--n 500000]
"""
import argparse
import json
import platform
import time

from order_gen import gen_orders
from ring_buffer import RingBuffer

import engine_py
try:
    import matching_engine as cy
except ImportError:
    cy = None


def run_one(engine_cls, name, orders, measure, path):
    cap = 1
    while cap < len(orders) + 1:
        cap <<= 1
    ring = RingBuffer(path, cap, create=True)
    try:
        for o in orders:
            ring.push(*o, 0)                    # ts=0 -> end-to-end latency skipped here
        eng = engine_cls(measure=measure)
        t0 = time.perf_counter()
        n = eng.drain(ring.mm)
        dt = time.perf_counter() - t0
    finally:
        ring.close(unlink=True)
    lat = eng.latency_stats()["match"]
    return {"engine": name, "measure": measure, "orders": n, "trades": eng.trades_count,
            "seconds": dt, "orders_per_s": n / dt, "ns_per_order": dt / n * 1e9,
            "whales": eng.whale_count,
            "p50_ns": lat["p50"], "p99_ns": lat["p99"], "p999_ns": lat["p999"], "max_ns": lat["max"]}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--n", type=int, default=500_000)
    ap.add_argument("--path", default="chronos_bench.ring")
    a = ap.parse_args()

    orders = list(gen_orders(a.n, whale_prob=0.0002))
    engines = [(engine_py.MatchingEngine, "python")]
    if cy is not None:
        engines.append((cy.MatchingEngine, "cython"))
    else:
        print("(Cython engine not built - benchmarking the pure-Python engine only)\n")

    results = []
    for cls, name in engines:
        for measure in (False, True):
            r = run_one(cls, name, orders, measure, a.path)
            results.append(r)
            print(f"{name:7s} measure={str(measure):5s} {r['orders_per_s']:>12,.0f} orders/s "
                  f"{r['ns_per_order']:>9,.0f} ns/order  match p50 {r['p50_ns']:,} ns  p99 {r['p99_ns']:,} ns  max {r['max_ns']:,} ns")

    py = next(r for r in results if r["engine"] == "python" and not r["measure"])
    lines = ["# ChronosMatch benchmark results", "",
             f"{a.n:,} orders, deterministic seed, {platform.system()} {platform.machine()}, Python {platform.python_version()}", "",
             "| engine | latency timing | orders/s | ns/order | match p50 (ns) | p99 (ns) | p99.9 (ns) | max (ns) | trades | whales |",
             "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for r in results:
        f = (lambda v: f"{v:,}") if r["measure"] else (lambda v: "-")
        lines.append(f"| {r['engine']} | {'on' if r['measure'] else 'off'} | {r['orders_per_s']:,.0f} | {r['ns_per_order']:,.0f} | "
                     f"{f(r['p50_ns'])} | {f(r['p99_ns'])} | {f(r['p999_ns'])} | {f(r['max_ns'])} | {r['trades']:,} | {r['whales']:,} |")
    cyr = next((r for r in results if r["engine"] == "cython" and not r["measure"]), None)
    cym = next((r for r in results if r["engine"] == "cython" and r["measure"]), None)
    if cyr and cym:
        sp = cyr["orders_per_s"] / py["orders_per_s"]
        lines += ["", f"**Cython speed-up over pure Python: {sp:.1f}x** (latency timing off).",
                  f"Sub-millisecond check (Cython, timing on): p99.9 = {cym['p999_ns']:,} ns -> "
                  f"{'PASS' if cym['p999_ns'] < 1_000_000 else 'FAIL'}; worst single order = {cym['max_ns']:,} ns "
                  "(rare OS-scheduler pauses can exceed this and are not engine time)."]
    lines += ["", "Latency percentiles come from a log-linear histogram (about 12% resolution);",
              "'match' = time spent inside the engine for one order (enter -> exit)."]
    open("RESULTS.md", "w").write("\n".join(lines) + "\n")
    json.dump(results, open("benchmark_results.json", "w"), indent=2)
    print("\nwrote RESULTS.md and benchmark_results.json")


if __name__ == "__main__":
    main()
