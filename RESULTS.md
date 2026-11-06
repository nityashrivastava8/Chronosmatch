# ChronosMatch benchmark results

500,000 orders, deterministic seed, Windows AMD64, Python 3.14.7

| engine | latency timing | orders/s | ns/order | match p50 (ns) | p99 (ns) | p99.9 (ns) | max (ns) | trades | whales |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| python | off | 317,479 | 3,150 | - | - | - | - | 344,169 | 79 |
| python | on | 197,093 | 5,074 | 2,048 | 10,240 | 49,152 | 35,089,400 | 344,169 | 79 |

Latency percentiles come from a log-linear histogram (about 12% resolution);
'match' = time spent inside the engine for one order (enter -> exit).
