"""Market simulator: an asyncio 'firehose' that blasts mock BUY/SELL orders
into the shared-memory ring buffer at a target rate (default 100,000/s).

Start engine_runner.py FIRST (it creates the ring), then:
    python market_firehose.py --rate 100000 --duration 10
"""
import argparse
import asyncio
import random
import time

from config import BUY, MID_PRICE, SELL, default_ring_path
from ring_buffer import RingBuffer


async def firehose(ring, rate, total, seed=42, whale_prob=0.00002):
    rng = random.Random(seed)
    rand, gauss, randint = rng.random, rng.gauss, rng.randint
    push = ring.push
    start = time.perf_counter()
    sent = 0
    order_id = 1
    stalls = 0
    while total == 0 or sent < total:
        owed = int((time.perf_counter() - start) * rate) - sent
        if total:
            owed = min(owed, total - sent)
        owed = min(owed, 20_000)            # never hog the event loop for long
        for _ in range(owed):
            side = BUY if rand() < 0.5 else SELL
            # buys cluster slightly below mid, sells slightly above -> some crossing
            price = MID_PRICE + round(gauss(-2 if side == BUY else 2, 10))
            qty = randint(1, 100)
            if rand() < whale_prob:         # occasional "whale" that sweeps levels
                qty = randint(2_000, 5_000)
                price += 40 if side == BUY else -40
            if not push(order_id, price, qty, side):
                stalls += 1                 # ring full: consumer is behind
                break
            order_id += 1
            sent += 1
        await asyncio.sleep(0.0005)         # let other coroutines run
    elapsed = time.perf_counter() - start
    return sent, elapsed, stalls


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rate", type=int, default=100_000, help="orders per second")
    ap.add_argument("--duration", type=float, default=10, help="seconds (0 = forever)")
    ap.add_argument("--path", default=default_ring_path())
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--whale-prob", type=float, default=0.00002, help="chance an order is a whale (default ~2 per second at 100k/s)")
    a = ap.parse_args()

    try:
        ring = RingBuffer(a.path)
    except FileNotFoundError:
        raise SystemExit("Ring buffer not found - start engine_runner.py first.")
    total = int(a.rate * a.duration) if a.duration else 0
    sent, elapsed, stalls = asyncio.run(firehose(ring, a.rate, total, a.seed, a.whale_prob))
    print(f"sent {sent:,} orders in {elapsed:.2f}s = {sent / elapsed:,.0f} orders/s "
          f"(ring-full stalls: {stalls})")


if __name__ == "__main__":
    main()
