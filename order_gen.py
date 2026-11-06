"""Deterministic mock-order generator shared by benchmark.py, gc_proof.py and tests."""
import random

from config import BUY, MID_PRICE, SELL


def gen_orders(n, seed=42, whale_prob=0.0002):
    """Yield (order_id, price, qty, side). Buys cluster just below mid and sells
    just above, so the book crosses often; a few 'whales' sweep many levels."""
    rng = random.Random(seed)
    rand, gauss, randint = rng.random, rng.gauss, rng.randint
    for oid in range(1, n + 1):
        side = BUY if rand() < 0.5 else SELL
        price = MID_PRICE + round(gauss(-2 if side == BUY else 2, 10))
        qty = randint(1, 100)
        if rand() < whale_prob:
            qty = randint(2_000, 5_000)
            price += 40 if side == BUY else -40
        yield oid, price, qty, side
