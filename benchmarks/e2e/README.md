# End-to-end benchmarks

This benchmark compares the same HTTP backend through two paths:

- Direct: load generator to the backend.
- Tunnel: load generator to `tnld`, the multiplexed control connection, `tnlc`, and the backend.

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

`TUNNEL_CONNECT_TO` is optional. It makes visitor connections enter `tnld` over loopback while preserving the tunnel hostname for SNI and certificate verification. This removes an accidental public-IP hairpin from the visitor side. It does not alter the persistent `tnld` to `tnlc` control connection.

The default matrix covers concurrency-one latency, small responses, large downloads and uploads at several concurrency levels, a large request followed by a large response, and fresh connections. Override the defaults with `DURATION` and `REPETITIONS`.

To run a smaller experiment matrix, set `BENCH_CASES` to semicolon-separated
`name response_bytes request_bytes concurrency mode` entries. For example:

```sh
BENCH_CASES='download 1048576 0 64 keepalive;upload 0 1048576 64 keepalive'
```

A nonzero request size sends a `POST` body generated as a sparse benchmark fixture. This permits
download, upload, and full request/response measurements through the same endpoint.

Set `BENCH_PATHS=tunnel` when iterating on tunnel-only changes without rerunning the direct baseline.

Requests already in flight at the end of a timed case are allowed to finish. This avoids treating load-generator cancellation and its temporary socket backlog as steady-state tunnel memory.

Raw `oha` JSON, `summary.csv`, and `processes.csv` are written below `benchmarks/e2e/results/`. CPU is reported as a percentage of one core, and memory is the peak resident set sampled during each case.

Recorded optimization studies:

- [First pass](OPTIMIZATION_RESULTS.md): mux buffers, flow-control window, session pooling, and LTO.
- [Second pass](ROUND2_RESULTS.md): dedicated data transports, adaptive mux fallback, uploads, and the full checkpoint matrix.
