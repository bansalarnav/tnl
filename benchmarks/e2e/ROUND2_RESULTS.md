# Round-two end-to-end throughput results

Run on 2026-08-15 on branch `optimize/e2e-throughput-round2`. The benchmark backend,
load generator, `tnlc`, and isolated `tnld` ran on the same 12-logical-CPU host. Visitor
traffic entered `tnld` over loopback while preserving TLS SNI. The direct path was plain
HTTP loopback; the tunnel path included visitor TLS, `tnld`, the outbound tunnel transport,
`tnlc`, and the same backend.

The initial and first-pass checkpoints are single five-second runs from commits `b23262f`
and `9c62df0`. Current and direct values are means of two matched eight-second runs. All
completed requests succeeded.

## Checkpoint comparison

For one-MiB one-way cases, requests/s equals MiB/s. Round-trip bulk sends a one-MiB request
and receives a one-MiB response, so its displayed throughput is twice its request rate.

| Scenario | Unit | Direct | Initial | First pass | Current | Current/direct |
|---|---:|---:|---:|---:|---:|---:|
| Empty response, c1 | req/s | 5,621 | 11.9 | 479 | 1,262 | 22.5% |
| 1 KiB response, c32 | req/s | 11,734 | 814 | 8,683 | 11,385 | 97.0% |
| 1 MiB download, c1 | MiB/s | 1,444 | 19.0 | 105.9 | 260.6 | 18.0% |
| 1 MiB download, c16 | MiB/s | 1,541 | 123.4 | 703.8 | 1,193.5 | 77.5% |
| 1 MiB download, c64 | MiB/s | 1,710 | 196.6 | 795.1 | 1,273.2 | 74.4% |
| 1 MiB upload, c1 | MiB/s | 909 | 20.0 | 26.8 | 232.3 | 25.6% |
| 1 MiB upload, c16 | MiB/s | 1,098 | 128.5 | 536.5 | 908.0 | 82.7% |
| 1 MiB upload, c64 | MiB/s | 1,144 | 116.0 | 677.2 | 996.8 | 87.1% |
| 1 MiB up + 1 MiB down, c16 | aggregate MiB/s | 1,499 | 165.0 | 751.3 | 1,123.2 | 74.9% |
| Fresh connection, c8 | req/s | 4,881 | 48.9 | 1,489 | 1,518 | 31.1% |

Relative to the initial implementation, current tunneled throughput is 13.7x higher for a
single download, 11.6x higher for a single upload, 6.5x higher for c64 downloads, and 8.6x
higher for c64 uploads. Relative to the first pass, those gains are 2.46x, 8.66x, 1.60x,
and 1.47x respectively.

At c64 download, `tnlc` averaged 2.96 cores and `tnld` 3.30 cores. At c64 upload they
averaged 3.24 and 1.97 cores. Maximum sampled tunnel-process RSS was 28 MiB. A cold idle
tunnel with its warm transport floor used roughly 10 MiB RSS and 49 client / 50 server file
descriptors on this host.

## Architecture and external comparison

The first pass pooled eight muxado control sessions, but every visitor connection still sent
all of its bytes through one mux stream on one control TCP connection. Additional round-robin
sessions therefore helped aggregate concurrency but could not parallelize or remove the
per-frame allocation, copying, flow-control, and scheduling cost of one stream.

The selected design separates control and data planes. `tnlc` keeps authenticated outer-TLS
data transports ready; `tnld` assigns one transport to a visitor connection and sends a core
activation marker. Application bytes then bypass muxado entirely. The mux session remains the
backward-compatible fallback and is preferred automatically for high-churn tiny connections.

This follows patterns used by established tunnel implementations:

- [bore](https://github.com/ekzhang/bore#protocol) opens a separate client-to-server TCP
  connection for each visitor connection.
- [frp](https://github.com/fatedier/frp#connection-pooling) supports pre-established work
  connections to avoid setup latency, as well as TCP multiplexing and QUIC.
- [Cloudflare Tunnel](https://developers.cloudflare.com/tunnel/) maintains multiple long-lived
  outbound connections and commonly carries independent streams over QUIC.

Byte-striping one ordered visitor TCP stream over several transport TCP connections was
rejected as the default. It would require sequencing and reassembly, and loss or RTT variance
on any lane would create cross-connection head-of-line blocking. Dedicated transports obtain
most of the aggregate gain without changing byte-stream semantics.

## Selected implementation

- Core owns authenticated transport registration, FIFO assignment, ownership checks, pool
  bounds, activation framing, availability notification, and short-connection adaptation.
- Keep 32 dedicated transports warm, but cap idle plus active transports at 64 with a client
  semaphore.
- When c64 arrives, claimed transports are replenished only while total transports remain below
  the cap. `tnld` waits up to 250 ms for burst replenishment before mux fallback.
- After four transports each finish within 100 ms and transfer at most 64 KiB, core selects the
  reusable mux data plane for two seconds. This restored fresh-connection throughput from about
  533 req/s in the unconditional raw-pool prototype to 1,518 req/s.
- Enable `TCP_NODELAY` on the `tnlc` to local-backend socket as well as the previously tuned
  control and visitor sockets.
- Prefer TLS 1.3 AES-128-GCM for the outer tunnel transport. Endpoint TLS configuration is
  unchanged.
- Negotiate the transport pool using `X-Tnl-Transport-Pool`. New clients use no dedicated pool
  with old servers; old clients ignore the new header and continue using mux streams.

## Experiments and interactions

| Experiment | Representative result | Decision |
|---|---|---|
| Fixed 64 raw transports, before backend `TCP_NODELAY` | 1,212 MiB/s download c64; 1,068 MiB/s upload c64; only 26 MiB/s upload c1 | Data-plane bypass worked, but exposed delayed ACK on the local socket |
| Add backend-facing `TCP_NODELAY` | Upload c1 rose from 26 to 169 MiB/s in adjacent prototype runs | Keep |
| Prefer outer AES-128-GCM with ring | Single-flow download/upload rose to 252/207 MiB/s in the experiment | Keep |
| Switch ring to AWS-LC | Upload c1 +11%, download c1 -8%, download c64 -14%, upload c64 -5% | Discard; mixed result and harder builds |
| Eight warm transports, uncapped immediate replenishment | 1,076 MiB/s download c64; 928 MiB/s upload c64 | Discard; TLS setup competed with payload work |
| 32 warm transports, but replenish 32 idle on top of active | 912 MiB/s download c64; 1,018 MiB/s upload c64 | Discard; handshake oversubscription |
| 32 warm, 64 total semaphore cap | 1,224 MiB/s download c64; 1,028 MiB/s upload c64 in the selection run | Keep; bounded replenishment removed the storm |
| Always use dedicated transports for fresh connections | About 533 req/s | Discard; replacement TLS dominates tiny connections |
| Core short-connection circuit breaker | 1,518 req/s final, versus 1,489 req/s first pass | Keep; preserves mux's strength for churn |

Raw checkpoint output is in `/tmp/tnl-round2-stage-*`, final output in
`/tmp/tnl-round2-final-*`, and intermediate experiments in `/tmp/tnl-round2-*` on the
benchmark host.
