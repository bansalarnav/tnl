# Throughput optimization results

Run on 2026-08-15 on branch `optimize/e2e-throughput`. The benchmark backend, load generator, `tnlc`, and isolated `tnld` all ran on the same 12-logical-CPU host. Visitor traffic entered `tnld` over loopback. Every result used release builds, 1 MiB responses, HTTP keep-alive, and completed in-flight requests after the timed interval.

## Outcome

The final matched comparison averaged:

| Workload | Direct | Optimized tunnel | Tunnel/direct |
|---|---:|---:|---:|
| Empty response, concurrency 1 | 5,684 req/s | 1,042 req/s | 18.3% |
| 1 KiB, concurrency 32 | 10,721 req/s | 9,360 req/s | 87.3% |
| 1 MiB, concurrency 16 | 1,528 MiB/s | 769 MiB/s | 50.3% |
| 1 MiB, concurrency 64 | 1,588 MiB/s | 861 MiB/s | 54.2% |
| Fresh connections, concurrency 8 | 3,751 req/s | 1,353 req/s | 36.1% |

All completed requests succeeded. At concurrency 64, `tnlc` averaged 3.55 cores and `tnld` 4.42 cores. Their sampled maximum RSS values were 64.4 MiB and 55.4 MiB respectively.

The exact-code isolated baseline reached 160.9 MiB/s at concurrency 64, or roughly 11% of its direct baseline. The selected implementation reaches 861 MiB/s in the final matched run, with individual experimental runs between 682 and 910 MiB/s. This is about a 5.35x improvement over the original tunnel and reduces the direct-versus-tunnel gap from roughly 9x to 1.84x.

Latency improved at the same time. The final empty-response p50 was 0.865 ms through the tunnel versus 0.144 ms direct. Before `TCP_NODELAY`, local tunneled response latency clustered around 40 ms multiples.

## Experiments

The table shows mean tunnel payload throughput. Variants before session pooling used one control session.

| Optimization | c1 | c16 | c64 | Result |
|---|---:|---:|---:|---|
| Original: 8 KiB copy buffers, 256 KiB mux window | 18.6 MiB/s | 106.3 MiB/s | 160.9 MiB/s | Baseline |
| `TCP_NODELAY` only | 81.4 MiB/s | 98.2 MiB/s | 71.1 MiB/s | Great latency/single-stream gain, severe aggregate regression from many immediate 8 KiB writes |
| 64 KiB buffers only | — | 161.2 MiB/s | 342.4 MiB/s | 2.13x baseline at c64 with moderate memory |
| 256 KiB buffers only | 20.6 MiB/s | 169.4 MiB/s | 364.1 MiB/s | Best single-session aggregate result, but more memory and still poor latency |
| `TCP_NODELAY` + 64 KiB buffers | 150.5 MiB/s | 225.5 MiB/s | 164.6 MiB/s | Removed delayed-ACK stalls and recovered most aggregate throughput |
| `TCP_NODELAY` + 256 KiB buffers | 149.3 MiB/s | 223.0 MiB/s | 187.7 MiB/s | Little improvement over 64 KiB at normal concurrency |
| Above + 1 MiB mux window | 112.7 MiB/s | 236.0 MiB/s | 215.1 MiB/s | Better at concurrency, noisy and below 4 MiB window |
| Above + 4 MiB mux window | 139.9 MiB/s | 292.2 MiB/s | 282.0 MiB/s | Flow-credit stalls reduced; memory rose at high concurrency |
| 64 KiB buffers + 4 MiB window, without `TCP_NODELAY` | 19.9 MiB/s | 191.5 MiB/s | 360.4 MiB/s | Good aggregate bandwidth, but retained the ~40 ms latency problem |
| Four sessions + `TCP_NODELAY` + 64 KiB + 4 MiB window | 138.1 MiB/s | 580.1 MiB/s | 680.1 MiB/s | Removed the single-session CPU serialization ceiling |
| Eight sessions, same settings | — | 621.0 MiB/s | 787.6 MiB/s | Used more of the 12-core host; eight was near the useful scaling limit |
| Eight sessions + thin LTO | — | — | 833.8 MiB/s | About 6% above the pre-LTO c64 mean; three-run mean |
| Directional buffers, 64 KiB request / 256 KiB response | — | — | 794.7 MiB/s | Regressed versus symmetric 64 KiB and used more memory; discarded |

Buffer size and `TCP_NODELAY` interacted strongly. Enabling `TCP_NODELAY` while retaining 8 KiB buffers made aggregate throughput worse because every small mux frame was sent immediately. Increasing the buffers first made `TCP_NODELAY` beneficial for latency without giving up normal-concurrency bandwidth.

The mux window and session pooling also complement each other. A larger window keeps each session productive, while pooling spreads the mux/TLS reader and writer work across cores. Increasing only the window cannot escape the serialized single-session ceiling.

## Selected implementation

- Use 64 KiB buffers for both directions of each forwarding loop instead of Tokio's 8 KiB defaults.
- Use a 4 MiB mux stream window instead of muxado's 256 KiB default.
- Enable `TCP_NODELAY` for accepted `tnld` sockets and the `tnlc` control sockets.
- Pool up to eight control sessions per tunnel and assign visitor streams round-robin.
- Bind pooled sessions to the same authenticated client credential.
- Negotiate the supported pool size in `X-Tnl-Control-Sessions`. The subsequent protocol-v2 pass
  made this capability header mandatory rather than defaulting for older servers.
- Build release binaries with thin LTO and one codegen unit.

Raw experiment output is stored in `/tmp/tnl-opt-*` on the benchmark host. The reusable harness is in this directory.
