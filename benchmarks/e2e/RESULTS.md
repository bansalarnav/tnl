# End-to-end benchmark results

These are the original pre-optimization measurements. See [OPTIMIZATION_RESULTS.md](OPTIMIZATION_RESULTS.md) for the optimization experiments and final comparison.

Run on 2026-08-15 from worktree commit `b23262f` using a release-built `tnlc`, the already deployed `tnld 0.0.1`, and `oha 1.15.0`. The host had 12 logical CPUs and 47 GiB of memory. Each result below is the mean of three five-second runs; requests still active at the deadline were allowed to complete.

## Topology

The benchmark backend, load generator, `tnlc`, and production `tnld` all ran on the same machine. Visitor connections were directed to `127.0.0.1:443` while preserving the public hostname for TLS and SNI. The persistent `tnlc` control connection used the machine's public address, as it does normally.

The direct path was plain HTTP to `127.0.0.1:18080`. The tunnel path included visitor TLS, `tnld`, the muxed control connection, `tnlc`, and the same backend. Consequently the fresh-connection comparison intentionally includes TLS setup as part of tunnel overhead.

## Request results

| Case | Path | Requests/s | Mean | p50 | p95 | p99 |
|---|---|---:|---:|---:|---:|---:|
| Empty, concurrency 1, keep-alive | Direct | 5,318 | 0.189 ms | 0.152 ms | 0.400 ms | 0.963 ms |
| Empty, concurrency 1, keep-alive | Tunnel | 11.94 | 83.680 ms | 82.029 ms | 83.284 ms | 215.568 ms |
| 1 KiB, concurrency 32, keep-alive | Direct | 10,985 | 2.904 ms | 2.713 ms | 4.896 ms | 6.361 ms |
| 1 KiB, concurrency 32, keep-alive | Tunnel | 611.87 | 53.619 ms | 70.458 ms | 85.886 ms | 98.315 ms |
| 1 MiB, concurrency 16, keep-alive | Direct | 1,423.98 | 11.215 ms | 10.449 ms | 19.068 ms | 23.041 ms |
| 1 MiB, concurrency 16, keep-alive | Tunnel | 122.32 | 130.629 ms | 127.074 ms | 174.362 ms | 241.638 ms |
| Empty, concurrency 8, fresh connection | Direct | 4,474.58 | 1.778 ms | 1.669 ms | 2.660 ms | 3.824 ms |
| Empty, concurrency 8, fresh connection | Tunnel | 50.23 | 157.974 ms | 165.215 ms | 172.597 ms | 179.568 ms |

All completed requests succeeded. There were no transport or HTTP errors.

Because the bulk response is exactly 1 MiB, its tunneled result is also approximately 122 MiB/s (1.03 Gbit/s) of application payload.

## Tunnel process usage

CPU is the percentage of one logical core, averaged across the three runs. RSS is the maximum sample across those runs.

| Case | `tnlc` CPU | `tnld` CPU | `tnlc` max RSS | `tnld` max RSS |
|---|---:|---:|---:|---:|
| Empty, concurrency 1 | 0.98% | 0.92% | 19.9 MiB | 16.0 MiB |
| 1 KiB, concurrency 32 | 13.37% | 12.99% | 19.9 MiB | 16.1 MiB |
| 1 MiB, concurrency 16 | 103.85% | 142.61% | 22.3 MiB | 17.9 MiB |
| Fresh connections, concurrency 8 | 8.45% | 6.00% | 20.5 MiB | 16.2 MiB |

The freshly started `tnlc` began at about 8.0 MiB RSS and retained about 20.3 MiB after the complete run. No backend sockets remained open and its file descriptor count returned to its idle value. This looks like retained allocator/buffer high-water memory rather than live connections, but a longer soak test is needed to show that it plateaus.

## Interpretation

Bulk throughput is respectable for this host, but local small-message latency is unexpectedly high. The control socket reported a minimum RTT of 0.01 ms but an ACK timeout (`ato`) of 40 ms. Observed medians cluster around 40 ms multiples: about 82 ms for an established request and 165 ms for a fresh connection. Neither side currently enables `TCP_NODELAY` on the control connection. Together, these are strong evidence that Nagle/delayed-ACK interaction is dominating local latency; this is an inference, not yet an instrumented proof.

A useful next comparison is the same end-to-end matrix after enabling `TCP_NODELAY` on both ends of the control connection. If that diagnosis is right, established local p50 should fall dramatically without materially changing bulk throughput.
