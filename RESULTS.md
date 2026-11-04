# ChronosMatch benchmark results

500,000 orders, deterministic seed, Windows AMD64, Python 3.14.7

| engine | latency timing | orders/s | ns/order | match p50 (ns) | p99 (ns) | p99.9 (ns) | max (ns) | trades | whales |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| python | off | 673,236 | 1,485 | - | - | - | - | 344,169 | 79 |
| python | on | 554,666 | 1,803 | 768 | 3,584 | 18,432 | 20,881,800 | 344,169 | 79 |

Latency percentiles come from a log-linear histogram (about 12% resolution);
'match' = time spent inside the engine for one order (enter -> exit).
