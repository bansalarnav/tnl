# End-to-end benchmarks

This benchmark compares the same HTTP backend through one or more paths:

- Direct: load generator to the backend.
- TNLD: load generator to `tnld`, the control connection, `tnlc`, and the backend.
- Another tunnel implementation, such as Cloudflare Tunnel.

The backend provides deterministic `/bytes/<size>` responses. Start it with:

```sh
node benchmarks/e2e/backend.js
```

Expose port 18080 with `tnlc`, then run the comparison with `oha` installed:

```sh
BENCH_PROCESSES='backend=<pid> tnlc=<pid> tnld=<pid>' \
TUNNEL_CONNECT_TO='<tunnel-host>:443:127.0.0.1:443' \
OHA=/path/to/oha \
benchmarks/e2e/run.sh \
  http://127.0.0.1:18080 \
  https://<tunnel-host>
```

Use separate phases when comparing tunnel implementations. This prevents idle tunnel clients from
competing for CPU, sockets, or bandwidth. The first phase creates the result files. Later phases set
`BENCH_APPEND=1` and give each tunnel a distinct label:

```sh
result_directory="benchmarks/e2e/results/$(date -u +%Y%m%dT%H%M%SZ)"

BENCH_PATHS=direct \
benchmarks/e2e/run.sh \
  http://127.0.0.1:18080 \
  http://127.0.0.1:18080 \
  "$result_directory"

# Start only tnlc before this phase.
BENCH_PATHS=tunnel TUNNEL_LABEL=tnld BENCH_APPEND=1 \
benchmarks/e2e/run.sh \
  http://127.0.0.1:18080 \
  https://<tnld-host> \
  "$result_directory"

# Stop tnlc and start only cloudflared before this phase.
BENCH_PATHS=tunnel TUNNEL_LABEL=cloudflare BENCH_APPEND=1 \
benchmarks/e2e/run.sh \
  http://127.0.0.1:18080 \
  https://<cloudflare-host> \
  "$result_directory"
```

`TUNNEL_CONNECT_TO` is optional. It makes visitor connections enter `tnld` over loopback while preserving the tunnel hostname for SNI and certificate verification. This removes an accidental public-IP hairpin from the visitor side. It does not alter the persistent `tnld` to `tnlc` control connection.

## Emulated tunnel links

`link_proxy.js` adds latency, jitter, connection-setup delay, and an optional aggregate bandwidth
limit. To isolate the link affected by a tunnel implementation change, point `tnlc`'s
`connect_addr` at the proxy while its API URL and visitor traffic continue to use `tnld` directly:

```sh
# tnld listens on 127.0.0.1:8443. The proxy listens on 9443.
node benchmarks/e2e/link_proxy.js wan30

# tnlc configuration
{
  "api_url": "https://tnl.example:8443",
  "connect_addr": "127.0.0.1:9443"
}
```

For absolute end-to-end latency, run a second proxy for the visitor-to-`tnld` hop and point
`TUNNEL_CONNECT_TO` at it:

```sh
LINK_LISTEN_PORT=10443 node benchmarks/e2e/link_proxy.js wan30

TUNNEL_CONNECT_TO='<tunnel-host>:8443:127.0.0.1:10443' \
benchmarks/e2e/run.sh \
  http://127.0.0.1:18080 \
  https://<tunnel-host>:8443
```

Named profiles are `loopback`, `lan`, `wan30`, and `wan100`. `wan30` models a 30 ms RTT,
2 ms one-way jitter, a 30 ms TCP setup cost, and a 200 Mbit/s link. `wan100` uses a 100 ms RTT
and 50 Mbit/s. Override individual settings with `LINK_DELAY_MS`, `LINK_JITTER_MS`,
`LINK_CONNECT_DELAY_MS`, and `LINK_BANDWIDTH_MBPS`.

This is a TCP byte-stream proxy. It models propagation, setup, and serialization delay without
root access. It cannot model packet loss or retransmission correctly. Use `tc netem` or two hosts
for loss tests. Dropping bytes in this proxy would corrupt TCP rather than trigger TCP recovery.

The default matrix covers 1-byte, 1 KiB, 64 KiB, 1 MiB, and 8 MiB responses. It exercises downloads,
uploads, and simultaneous upload/download work at concurrency levels from 1 through 192. The fresh
connection case sends a fixed 2,000 requests so it does not exhaust the benchmark host's ephemeral
ports. Override the defaults with `DURATION`, `REPETITIONS`, and `NEW_CONNECTION_REQUESTS`.

Cloudflare Quick Tunnels allow no more than 200 in-flight requests. The default matrix stops at 192 so
the same cases can run through a Quick Tunnel and TNLD.

To run a smaller experiment matrix, set `BENCH_CASES` to semicolon-separated
`name response_bytes request_bytes concurrency mode` entries. For example:

```sh
BENCH_CASES='download 1048576 0 64 keepalive;upload 0 1048576 64 keepalive'
```

A nonzero request size sends a `POST` body generated as a sparse benchmark fixture. This permits
download, upload, and full request/response measurements through the same endpoint.

Set `BENCH_PATHS=tunnel` when iterating on tunnel-only changes without rerunning the direct baseline.
Set `TUNNEL_LABEL` to identify the implementation in filenames and CSV rows. `DIRECT_LABEL` does the
same for the direct path. Set `BENCH_APPEND=1` to add a separately run phase to an existing result
directory. The script refuses to overwrite raw results.

Requests already in flight at the end of a timed case are allowed to finish. This avoids treating load-generator cancellation and its temporary socket backlog as steady-state tunnel memory.

Raw `oha` JSON, `summary.csv`, `aggregate.csv`, and `processes.csv` are written below
`benchmarks/e2e/results/`. `summary.csv` has one row per repetition. `aggregate.csv` reports medians,
the minimum success rate, and the total error count for each path and case.
Every summary row contains completed responses, errors, elapsed time, request rate, response latency,
time to first byte, success rate, and request/response payload throughput. Payload throughput excludes
HTTP framing and TLS overhead. Response throughput uses the bytes actually received, so timeouts do
not inflate it. Request throughput is an estimate based on attempted requests and configured body
size. CPU is reported as a percentage of one core, and memory is the peak resident set sampled during
each case. Process sampling currently requires Linux `/proc`; request and network measurements work
on Linux and macOS.

Recorded optimization studies:

- [Optimization guide](OPTIMIZATIONS.md): conceptual data paths, rationale, code map, and why
  concurrency-one results differ from aggregate throughput.
- [First pass](OPTIMIZATION_RESULTS.md): mux buffers, flow-control window, session pooling, and LTO.
- [Second pass](ROUND2_RESULTS.md): dedicated data transports, adaptive mux fallback, uploads, and the full checkpoint matrix.
- [Raw TCP comparison](RAW_TCP_RESULTS.md): outer TLS versus authenticated raw data sockets.
- [Reusable transport comparison](REUSABLE_TRANSPORT_RESULTS.md): protocol v4 single-use versus
  protocol v5 framed and protocol v6 sideband-recycled dedicated transports, including emulated WAN
  results.
