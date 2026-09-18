# Reusable dedicated transport results

Protocol v5 returns a dedicated data connection to the pool after both directions finish cleanly.
The old protocol closed it and opened and HMAC-authenticated a replacement.

The table reports medians from paired direct and tunnel runs on the isolated loopback harness.
Because host throughput varied between runs, the comparison column uses tunnel throughput divided
by its adjacent direct result. Fresh and concurrency-one include eight old and eleven new samples;
the other cases include three old samples and three to six new samples.

| Case | Old tunnel | New tunnel | Old tunnel/direct | New tunnel/direct | Change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fresh connections, c8 | 1,407 req/s | 2,247 req/s | 34.2% | 47.5% | +38.8% |
| 1 KiB keep-alive, c32 | 11,853 req/s | 9,129 req/s | 103.8% | 77.3% | -25.5% |
| 1 MiB download, c1 | 317 MiB/s | 272 MiB/s | 24.2% | 19.5% | -19.5% |
| 1 MiB download, c16 | 1,307 MiB/s | 976 MiB/s | 80.3% | 77.3% | -3.8% |
| 1 MiB download, c64 | 1,270 MiB/s | 1,220 MiB/s | 82.0% | 81.2% | -1.0% |

All measured requests succeeded and the harness reported no errors. Median/maximum sampled peak
RSS stayed in the same range: the old runs peaked at 16.6 MiB for both `tnlc` and `tnld`; the new
runs peaked at 16.5 MiB and 16.3 MiB respectively.

The result is a deliberate tradeoff: it removes connection setup from the high-churn visitor path,
but the four-byte data framing adds work to established streams. Vectored header-plus-payload
writes substantially reduced the initial bulk regression. A framing or forwarding error always
retires the carrier rather than risking desynchronisation of a later visitor stream.
