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

## Emulated WAN

The loopback result is not representative of the `tnlc` to `tnld` link in normal deployments. A
user-space TCP proxy was therefore placed on that hop while the visitor and backend remained local.
Each result is the median of three paired runs. The `wan30` profile uses 30 ms RTT, 2 ms one-way
jitter, 30 ms connection setup, and a shared 200 Mbit/s limit. `wan100` uses 100 ms RTT, 5 ms jitter,
100 ms connection setup, and a shared 50 Mbit/s limit.

| Profile and case | v4 single-use | v6 sideband reuse | Change |
| --- | ---: | ---: | ---: |
| WAN30, fresh c8 | 105.1 req/s | 113.7 req/s | +8.2% |
| WAN30, 1 KiB c32 | 914.5 req/s | 918.7 req/s | +0.5% |
| WAN30, 1 MiB c1 | 106.1 Mbit/s | 106.9 Mbit/s | +0.7% |
| WAN30, 1 MiB c16 | 194.9 Mbit/s | 195.5 Mbit/s | +0.3% |
| WAN30, 1 MiB c64 | 196.1 Mbit/s | 196.5 Mbit/s | +0.2% |
| WAN100, fresh c8 | 36.70 req/s | 37.15 req/s | +1.2% |
| WAN100, 1 KiB c32 | 294.9 req/s | 295.7 req/s | +0.3% |
| WAN100, 1 MiB c1 | 29.61 Mbit/s | 29.42 Mbit/s | -0.7% |
| WAN100, 1 MiB c16 | 48.20 Mbit/s | 48.20 Mbit/s | 0.0% |
| WAN100, 1 MiB c64 | 48.55 Mbit/s, 65.2% success | 49.08 Mbit/s, 100% success | +1.1% throughput |

The old WAN100 c64 runs timed out exactly 32 requests in every repetition. Their latency numbers
have survivorship bias and are not directly comparable with v6, which completed every request.
This also uncovered and fixed benchmark accounting that had multiplied attempted request rate by
response size: failed requests could previously report payload that was never received.

The WAN runs do not reveal a broad throughput win. Reuse gives a modest fresh-connection improvement
and, under sustained pressure above the 32-carrier pool, replaces timeouts with queued completion.
That makes it primarily a churn and overload-reliability change rather than a steady-state throughput
optimization. The proxy does not emulate loss or TCP retransmission; those require `tc netem` or two
physical hosts.
