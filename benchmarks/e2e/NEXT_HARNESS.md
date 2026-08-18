# Next-tier benchmark harness

Proposal, not implemented. The existing `run.sh` answered the questions of the first two
optimization passes. It cannot answer the questions the next pass will ask, most immediately
"should the data plane move to QUIC/UDP".

## What the current harness already does

Do not rebuild these:

- Direct and tunnel paths against the same backend, matched case-for-case.
- Configurable case matrix via `BENCH_CASES`, repetitions via `REPETITIONS`.
- Per-case p50, p95, and p99 latency, mean, and success rate.
- Per-process CPU as a percentage of one core and peak sampled RSS.
- Warmup before recording, and in-flight requests allowed to drain.

## Gap 1: no network conditions

Everything so far ran on loopback: no loss, no reordering, sub-millisecond RTT, 64 KiB
segments. Every TCP weakness that would motivate a transport change is invisible, and every
QUIC cost is fully exposed. A QUIC-versus-TCP decision made on this harness measures the host,
not the protocol.

Add a `netem` profile applied to the `tnld` to `tnlc` path only, leaving the visitor and
backend hops clean so the variable under test stays isolated.

```sh
# on the interface carrying tunnel transport traffic
tc qdisc add dev "$IFACE" root netem delay 30ms 5ms distribution normal loss 1% reorder 0.1%
```

Suggested profiles, each a named row in the results so runs stay comparable:

| Profile | RTT | Loss | Intent |
|---|---|---|---|
| `loopback` | ~0 | 0 | current behaviour, regression baseline |
| `lan` | 1 ms | 0 | same-datacenter |
| `wan` | 30 ms | 0 | cross-region, clean |
| `lossy` | 30 ms | 1% | the case that justifies QUIC |
| `bad` | 100 ms | 3% | mobile or congested |

The `lossy` and `bad` rows are the entire point. Head-of-line blocking on a multiplexed TCP
session, and the cost of losing a dedicated transport mid-transfer, only appear here.

## Gap 2: single host

`tnlc`, `tnld`, the backend, and the load generator all share 12 logical CPUs. Their CPU
contention is part of every number recorded so far, and process CPU columns measure a
contended scheduler rather than the work each side actually needs.

Two-host mode: load generator plus `tnld` on one machine, `tnlc` plus backend on the other.
It separates client-side from server-side cost, makes the `netem` profile physically real
rather than synthetic, and stops the direct baseline from competing with the tunnel for cores.
Keep single-host mode as the fast iteration path.

## Gap 3: metrics that decide transport questions

Latency percentiles exist but the recorded studies only ever reported throughput. The
following are not captured at all:

- **Time to first byte**, separated from total transfer time. Distinguishes connection setup
  cost from steady-state throughput, which is exactly the axis QUIC changes.
- **Connection establishment latency** as its own measurement, not folded into the
  `new_connection` throughput case. Should be reported as a distribution.
- **Data-path selection counters**: how often a visitor connection got a dedicated transport
  versus mux fallback, how often the circuit breaker engaged, how often the 250 ms
  replenishment wait expired. Currently invisible, so a change to pool sizing can only be
  evaluated by its downstream throughput effect. These need to be exported by `tnld` rather
  than inferred by the harness.
- **Latency under sustained load**, i.e. a throughput-versus-p99 curve rather than a single
  point per concurrency level. A change that raises c64 throughput while tripling p99 is
  currently indistinguishable from a clean win.

## Gap 4: workload shapes

The current matrix is uniform: every connection in a case is identical. Missing shapes that
the adaptive data-path logic is specifically sensitive to:

- **Mixed traffic**: bulk transfers concurrent with small requests on the same tunnel. The
  short-connection circuit breaker makes a per-tunnel decision, so a tunnel carrying both at
  once may flap between data planes. Nothing currently tests this.
- **Many tunnels**: all measurements used one tunnel. Pool sizing is per-tunnel, so 100
  tunnels imply up to 3,200 warm transports. Whether that is viable is untested.
- **Idle then burst**: a tunnel idle for minutes, then hit at c64. Tests whether the warm pool
  survives idle and whether replenishment keeps up from cold.
- **Slow consumer**: a visitor reading at a fixed low rate, holding a dedicated transport open.
  Tests whether the pool starves under adversarial-but-legitimate load.
- **Long-lived streaming**: minutes-long connections rather than seconds, which is where
  heartbeat and flow-control window behaviour actually shows up.

## Gap 5: run-to-run confidence

Results so far are means of two or three runs with no dispersion reported. Several accepted
and rejected experiments in `ROUND2_RESULTS.md` differ by margins that may sit inside run
variance, and there is no way to tell from the recorded data.

- Report standard deviation or min/max alongside every mean.
- Fix CPU frequency governor and pin processes where possible.
- Define a regression gate: a named profile and case subset, with a threshold, that can run
  before a release tag.

## Decision criteria for QUIC

Worth building the above before writing any QUIC code, because these are the numbers that
should decide it:

1. On `lossy` and `bad`, does mux head-of-line blocking or dedicated-transport loss recovery
   measurably hurt, and by how much? If TCP holds up, the case for QUIC is weak.
2. On `loopback` and `lan`, how much throughput would QUIC's userspace per-packet AEAD cost?
   This is the price paid for any gain in 1.
3. Does QUIC's near-free stream establishment actually remove the transport pool, circuit
   breaker, and replenishment machinery, or does the TCP fallback required for UDP-blocking
   networks mean both data planes must be maintained regardless?

Item 3 is a design question rather than a measurement, and it is probably the decisive one.
