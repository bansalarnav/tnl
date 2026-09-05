# QUIC implementation comparison

This is a server-side throughput test for raw `quiche`, `tokio-quiche`, and
`s2n-quic`. A single raw-quiche client drives every server using the same small
application protocol over bidirectional QUIC streams. It reports application
bytes, not UDP or QUIC wire bytes.

The benchmark covers:

- download, upload, and simultaneous bidirectional transfer;
- one or many streams on one warm connection;
- fixed aggregate bytes when stream count changes;
- one discarded warmup followed by repeated JSONL samples;
- matching CUBIC congestion control, 1350-byte UDP payloads, 512 MiB flow-control
  windows, ALPN, certificate, and release/LTO settings.

Run it on Linux with a release Rust toolchain, a C/C++ toolchain, Clang headers,
and CMake:

```sh
cd benchmarks/quic-comparison
./run.sh
```

Useful controls:

```sh
TOTAL_BYTES=$((1024 * 1024 * 1024)) \
STREAM_COUNTS="1 8 32 128" \
SAMPLES=10 WARMUPS=2 \
SERVER_CPUS=2-5 CLIENT_CPUS=6-9 \
./run.sh
```

Each line in `results.jsonl` is one sample. `gbps` counts both directions for
the bidirectional workload. Use separate physical cores for client and server,
disable frequency scaling/turbo if you need low-noise results, and run client
and server on separate machines before treating the numbers as a network
capacity claim.

To print the median for every backend/workload/stream-count group:

```sh
jq -rs '
  group_by([.backend, .workload, .streams])
  | map({
      backend: .[0].backend,
      workload: .[0].workload,
      streams: .[0].streams,
      median_gbps: (map(.gbps) | sort | .[length / 2 | floor])
    })
' results.jsonl
```

## What this does and does not compare

The common client makes the server implementation the independent variable.
That is useful for choosing a server stack for `tnl`, and it also checks real
interoperability. It does not compare client APIs, handshake rate, latency under
loss, connection migration, or memory consumption.

The raw-quiche adapter is a deliberately small `mio`/`sendto` integration. It
does not implement GSO/GRO or `sendmmsg`/`recvmmsg`; tokio-quiche and s2n-quic
can use Linux UDP offloads. Therefore:

- the raw-quiche row is an integration baseline, not quiche's engine ceiling;
- tokio-quiche versus raw quiche shows the value of Cloudflare's production I/O
  wrapper as much as Tokio overhead;
- tokio-quiche versus s2n-quic is the most directly actionable comparison here;
- throughput is not CPU efficiency. Pinning four server cores gives a different
  question from pinning one.

For the optimized transport-engine ceiling, pair these results with
[quicperf](https://github.com/victorstewart/quicperf). Its shared C++ packet I/O
backend supplies GSO/GRO and both syscall and io_uring modes to quiche and
s2n-quic. It currently has no tokio-quiche adapter, so it cannot answer the
wrapper question by itself.

## Pinned versions

- `tokio-quiche 0.19.1`, which re-exports `quiche 0.29.3`
- `s2n-quic 1.88.0` with its s2n-tls provider

The test certificate and key are public fixtures copied from s2n-quic and are
only suitable for local benchmarking.
