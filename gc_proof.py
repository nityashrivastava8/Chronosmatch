"""WEEK 3 PROOF: the matching loop never touches the Python garbage collector.

We pre-load 200,000 orders into the ring, then call engine.drain() once and check
what Python's memory manager did while it ran:
  1. sys.getallocatedblocks() - how many Python objects were created (delta)
  2. gc.callbacks             - did the cyclic garbage collector run at all?

Cython engine : expect ~0 new objects and 0 collections  -> PASS
Python engine : creates tuples/lists for every trade       -> shown for contrast

    python gc_proof.py [--n 200000]
"""
import argparse
import gc
import sys
import time

from engine_loader import BACKEND, MatchingEngine
from order_gen import gen_orders
from ring_buffer import RingBuffer


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--n", type=int, default=200_000)
    ap.add_argument("--path", default="chronos_gc.ring")
    a = ap.parse_args()

    cap = 1
    while cap < a.n + 1:
        cap <<= 1
    ring = RingBuffer(a.path, cap, create=True)
    try:
        for o in gen_orders(a.n, whale_prob=0.001):
            ring.push(*o)
        engine = MatchingEngine()

        collections = []
        gc.callbacks.append(lambda phase, info: collections.append(phase) if phase == "start" else None)
        gc.collect()
        del collections[:]
        gc.freeze()                         # ignore objects that already exist

        before = sys.getallocatedblocks()
        t0 = time.perf_counter()
        n = engine.drain(ring.mm)
        dt = time.perf_counter() - t0
        after = sys.getallocatedblocks()
        gc_runs = len(collections)
        gc.unfreeze()
    finally:
        ring.close(unlink=True)

    delta = after - before
    print(f"backend: {BACKEND}")
    print(f"drained {n:,} orders -> {engine.trades_count:,} trades in {dt * 1000:,.0f} ms")
    print(f"Python objects allocated during drain : {delta:+,}")
    print(f"garbage-collector runs during drain   : {gc_runs}")
    ok = abs(delta) <= 50 and gc_runs == 0
    if BACKEND == "cython":
        print("RESULT:", "PASS - zero-allocation matching loop, the GC never triggers during trades" if ok else "FAIL")
        sys.exit(0 if ok else 1)
    print("RESULT: (pure-Python baseline - allocations expected; build the Cython engine to see PASS)")


if __name__ == "__main__":
    main()
