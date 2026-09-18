# Reusable dedicated transport results

Protocol v4 closed each dedicated data connection and opened and HMAC-authenticated a replacement.
Two reuse designs were measured:

- Protocol v5 framed every payload write inline.
- Protocol v6 keeps payload raw and sends byte counts over a persistent mux sideband.

The table reports medians from paired direct and tunnel runs on the isolated loopback harness.
Because host throughput varied between runs, the table uses tunnel throughput divided by its
adjacent direct result. Fresh uses eight v4, eleven v5, and six v6 samples. The v6
established-stream cases use six to eleven samples.

| Case | v4 single-use | v5 framed reuse | v6 sideband reuse |
| --- | ---: | ---: | ---: |
| Fresh connections, c8 | 34.2% | 47.5% | 38.9% |
| 1 KiB keep-alive, c32 | 103.8% | 77.3% | 101.1% |
| 1 MiB download, c1 | 24.2% | 19.5% | 22.8% |
| 1 MiB download, c16 | 80.3% | 77.3% | 75.5% |
| 1 MiB download, c64 | 82.0% | 81.2% | 80.8% |

All retained runs completed without request errors. Sampled peak RSS stayed around 17 MiB or less
for both processes in all three designs.

Inline framing produced the largest fresh-connection gain but caused a clear c1 and small-response
regression. Sideband reuse recovers most established-stream throughput and still improves fresh
connections over v4, though its boundary handshake gives up part of v5's churn gain. Any payload,
sideband, or byte-count error retires the carrier rather than risking the next visitor stream.
