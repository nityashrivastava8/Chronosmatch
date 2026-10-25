"""MID-REVIEW 'IPC AUDIT': send 1,000,000 orders between two Python processes
through the zero-copy ring buffer and compare against the classic approach
(multiprocessing.Queue, which pickles every message).

    python ipc_audit.py                   # 1M orders
    python ipc_audit.py --n 1000000 --baseline-n 200000   # shorter baseline
"""
import argparse
import multiprocessing as mp
import os
import pickle
import struct
import time

from config import RECORD_FORMAT, default_ring_path
from engine_loader import BACKEND, MatchingEngine
from ring_buffer import RingBuffer


# ----------------------------------------------------------- worker processes
def ring_producer(path, n):
    ring = RingBuffer(path)
    push, sleep = ring.push, time.sleep
    for i in range(1, n + 1):
        while not push(i, 10_000 + (i % 100), 1 + (i % 50), i & 1):
            sleep(0)                                  # ring full -> yield CPU


def queue_producer(q, n):
    for i in range(1, n + 1):
        q.put((i, 10_000 + (i % 100), 1 + (i % 50), i & 1, time.perf_counter_ns()))
    q.put(None)


# --------------------------------------------------------------------- tests
def audit_ring(n, path, capacity):
    """Producer process -> ring -> consumer (this process). Verifies integrity."""
    ctx = mp.get_context("spawn")
    ring = RingBuffer(path, capacity, create=True)
    proc = ctx.Process(target=ring_producer, args=(path, n))
    t0 = time.perf_counter()
    proc.start()
    expected, checksum = 1, 0
    pop, sleep = ring.pop, time.sleep
    while expected <= n:
        rec = pop()
        if rec is None:
            sleep(0)
            continue
        assert rec[0] == expected, f"order lost/reordered at {expected}: got {rec[0]}"
        checksum += rec[2]
        expected += 1
    dt = time.perf_counter() - t0
    proc.join()
    ring.close(unlink=True)
    return dt, checksum


def audit_ring_engine(n, path, capacity):
    """Same, but the consumer is the matching engine reading the shared memory."""
    ctx = mp.get_context("spawn")
    ring = RingBuffer(path, capacity, create=True)
    engine = MatchingEngine()
    proc = ctx.Process(target=ring_producer, args=(path, n))
    t0 = time.perf_counter()
    proc.start()
    while engine.orders_processed < n:
        if engine.drain(ring.mm, 50_000) == 0:
            time.sleep(0)
    dt = time.perf_counter() - t0
    proc.join()
    ring.close(unlink=True)
    return dt, engine.orders_processed, engine.trades_count


def audit_queue(n):
    """Baseline: multiprocessing.Queue (pickle + pipe)."""
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    proc = ctx.Process(target=queue_producer, args=(q, n))
    t0 = time.perf_counter()
    proc.start()
    got = 0
    while q.get() is not None:
        got += 1
    dt = time.perf_counter() - t0
    proc.join()
    assert got == n
    return dt


def serialization_cost(n):
    """Isolated cost of turning an order into bytes and back."""
    order = (123456789, 10_050, 25, 1, 987654321012)
    t = time.perf_counter()
    for _ in range(n):
        pickle.loads(pickle.dumps(order))
    t_pickle = time.perf_counter() - t
    rec = struct.Struct(RECORD_FORMAT)
    buf = bytearray(rec.size)
    t = time.perf_counter()
    for _ in range(n):
        rec.pack_into(buf, 0, *order)
        rec.unpack_from(buf, 0)
    t_struct = time.perf_counter() - t
    return t_pickle, t_struct


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--n", type=int, default=1_000_000)
    ap.add_argument("--baseline-n", type=int, default=None, help="orders for the Queue baseline (default = --n)")
    ap.add_argument("--capacity", type=int, default=1 << 16, help="small on purpose -> forces wrap-around")
    ap.add_argument("--path", default=default_ring_path() + ".audit")
    a = ap.parse_args()
    bn = a.baseline_n or a.n
    print(f"CPU cores: {os.cpu_count()}   engine backend: {BACKEND}\n")

    dt_ring, checksum = audit_ring(a.n, a.path, a.capacity)
    expected_sum = sum(1 + (i % 50) for i in range(1, a.n + 1))
    assert checksum == expected_sum, "payload corrupted!"
    print(f"[1] zero-copy ring, 2 processes : {a.n:,} orders in {dt_ring:6.2f}s "
          f"= {a.n / dt_ring:>10,.0f} orders/s  ({dt_ring / a.n * 1e9:,.0f} ns/order)  "
          f"integrity OK (ordering + checksum)")

    dt_eng, done, trades = audit_ring_engine(a.n, a.path, a.capacity)
    print(f"[2] ring -> matching engine     : {done:,} orders in {dt_eng:6.2f}s "
          f"= {done / dt_eng:>10,.0f} orders/s  ({trades:,} trades)")

    dt_q = audit_queue(bn)
    print(f"[3] baseline mp.Queue (pickle)  : {bn:,} orders in {dt_q:6.2f}s "
          f"= {bn / dt_q:>10,.0f} orders/s  ({dt_q / bn * 1e9:,.0f} ns/order)")

    m = min(a.n, 500_000)
    tp, ts = serialization_cost(m)
    print(f"\nSerialization cost only ({m:,} orders): pickle round-trip {tp:.2f}s "
          f"vs struct pack/unpack {ts:.2f}s  -> {tp / ts:.1f}x cheaper")
    print(f"\nSPEEDUP of zero-copy ring vs pickled Queue: "
          f"{(dt_q / bn) / (dt_ring / a.n):.1f}x per order")


if __name__ == "__main__":
    main()
