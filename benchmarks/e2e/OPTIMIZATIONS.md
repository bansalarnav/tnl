# Throughput optimization guide

This document describes the historical protocol v2 path and the changes that led to protocol v6's
reusable authenticated TCP data sockets.

This guide explains why each selected optimization exists, how the data paths work, and where
the implementation lives. Measurements and rejected experiments remain in
[OPTIMIZATION_RESULTS.md](OPTIMIZATION_RESULTS.md) and [ROUND2_RESULTS.md](ROUND2_RESULTS.md).

## Data paths

The original and fallback path carries each visitor connection as a logical muxado stream:

```text
visitor -> tnld visitor socket -> mux stream -> outer TLS/TCP control session
        -> tnlc -> visitor TLS termination -> local backend TCP socket
```

The second pass added a pool of authenticated TLS/TCP connections. Protocol v4 removed TLS from
these data sockets. Protocol v6 pairs each raw data socket with a persistent boundary stream and
makes the pair reusable. For a normal connection, one idle transport is removed from the pool and
becomes that visitor connection's data plane:

```text
visitor -> tnld visitor socket -> dedicated authenticated TCP transport
        -> tnlc -> visitor TLS termination -> local backend TCP socket
```

`tnld` writes `TRANSPORT_ACTIVATION_MARKER` before application bytes so the waiting `tnlc`
worker knows that its idle transport has been claimed. Payload then travels unframed. At EOF, each
peer sends the exact number of bytes it wrote over the transport's persistent mux sideband. The
receiver consumes exactly that many raw bytes and acknowledges the boundary. Once both directions
finish, `tnld` returns the data socket and sideband to the pool. The acknowledgement matters because
the count and payload use different TCP connections; it prevents the next activation marker from
overtaking the previous count.

One visitor TCP connection always retains one ordered data path. Traffic is never striped across
dedicated transports.

The mux path remains as a bounded fallback when every dedicated transport is active.

## Selected optimizations

### Larger forwarding buffers

Tokio's default bidirectional copy buffer was 8 KiB. Both forwarding directions now use 64 KiB,
which reduces reads, writes, mux frames, wakeups, and bookkeeping per transferred byte without
the memory cost observed with 256 KiB buffers.

- Server forwarding: [`tnld/src/server/tunnel.rs`](../../tnld/src/server/tunnel.rs)
- Client-to-backend forwarding: [`tnlc/src/tunnel.rs`](../../tnlc/src/tunnel.rs)

This change is especially important in combination with `TCP_NODELAY`: immediate transmission
of many 8 KiB writes hurt aggregate throughput, while 64 KiB buffers retain low latency with far
fewer writes.

### `TCP_NODELAY` on latency-sensitive sockets

Nagle's algorithm and delayed acknowledgements caused roughly 40 ms stalls in the original local
path. `TCP_NODELAY` is enabled on accepted visitor/control sockets, client control/data sockets,
and the client socket connected to the local backend.

- Accepted server sockets: [`tnld/src/server/mod.rs`](../../tnld/src/server/mod.rs)
- Client control/data and backend sockets: [`tnlc/src/tunnel.rs`](../../tnlc/src/tunnel.rs)

This removes latency cliffs. It does not itself increase every aggregate case: with small copy
buffers, sending every write immediately was an aggregate-throughput regression.

### Larger mux flow-control window

Each mux stream has a 4 MiB receive window instead of muxado's 256 KiB default. A sender can keep
more data in flight before waiting for credit, reducing flow-control stalls on high-bandwidth
streams. The setting is applied by `SessionParts::start` in
[`core/src/session.rs`](../../core/src/session.rs).

A larger window improves an individual mux session but cannot remove that session's serialized
framing, TLS, reader, and writer work. That required session pooling.

### Eight authenticated control sessions

`tnlc` opens up to eight control sessions for one logical tunnel. `TunnelServer::open` assigns
new visitor streams round-robin, spreading mux and TLS work across connections and CPU cores.
All additional sessions must have the same authenticated owner, so another client cannot attach
itself to an existing tunnel name.

- Pool limits, ownership, and round-robin selection:
  [`core/src/server/mod.rs`](../../core/src/server/mod.rs)
- Server negotiation through `X-Tnl-Control-Sessions`:
  [`tnld/src/server/api.rs`](../../tnld/src/server/api.rs)
- Client session creation and serving: [`tnlc/src/tunnel.rs`](../../tnlc/src/tunnel.rs)

This raises aggregate concurrency substantially, but one visitor connection is still one stream
on one session. It therefore cannot make a single flow use eight connections.

### Dedicated data transports

For bulk traffic, mux framing, allocation, copying, flow control, and per-session scheduling are
avoidable overhead. Application bytes move over a claimed authenticated raw TCP connection. The
mux connections carry tunnel control, boundary counts, acknowledgements, and fallback traffic.

- Core transport type: [`core/src/transport.rs`](../../core/src/transport.rs)
- Registration, authenticated ownership, FIFO assignment, and availability notification:
  [`core/src/server/mod.rs`](../../core/src/server/mod.rs)
- CONNECT endpoint and protocol negotiation through `X-Tnl-Transport-Pool`:
  [`tnld/src/server/api.rs`](../../tnld/src/server/api.rs)
- Server claim, activation, forwarding, and mux fallback:
  [`tnld/src/server/tunnel.rs`](../../tnld/src/server/tunnel.rs)
- Client pool workers and activation-marker handling:
  [`tnlc/src/tunnel.rs`](../../tnlc/src/tunnel.rs)

The API is in `core` because pool ownership and path selection are protocol behavior shared by
server integrations, rather than an HTTP-server-only detail.

### Reusable pool and burst wait

The server recommends 64 persistent transports, matching the previous maximum of 64 idle plus
active transports. Each client worker owns one physical connection and serves visitor streams on
it sequentially. If a burst temporarily empties the pool, `tnld` waits up to 250 ms for a returned
transport, then falls back to mux rather than blocking indefinitely.

- Recommended and maximum sizes plus async availability:
  [`core/src/server/mod.rs`](../../core/src/server/mod.rs)
- Client workers and sideband boundaries: [`tnlc/src/tunnel.rs`](../../tnlc/src/tunnel.rs)
- Bounded wait and fallback: [`tnld/src/server/tunnel.rs`](../../tnld/src/server/tunnel.rs)

Using only 32 persistent workers reduced c64 throughput because it also capped active dedicated
connections at 32. Keeping 64 preserves the old design's peak concurrency without its replacement
connection and HMAC-authentication churn.

### Release code generation

Release builds use thin LTO and one codegen unit in [`Cargo.toml`](../../Cargo.toml). This gives
LLVM visibility across crate boundaries and produced a modest improvement after the larger data
path bottlenecks were removed.

## Why concurrency one remains far below direct

Concurrency one is easier on total CPU and memory, but it is not easier to score highly in this
throughput benchmark. With one in-flight request, throughput is approximately the reciprocal of
that request's end-to-end service time. No other request can overlap socket wakeups, copying,
encryption, proxy scheduling, or request/response latency.

The direct baseline is an unusually short path: plain HTTP over loopback straight into the
backend. The tunneled measurement intentionally includes visitor TLS, `tnld`, a dedicated data
connection, `tnlc`, visitor TLS termination, a second backend TCP socket, and multiple userspace
copy/scheduling boundaries. For the final empty-response run:

- direct: 5,621 req/s, or about 0.178 ms per sequential request;
- tunnel: 1,262 req/s, or about 0.792 ms per sequential request.

The tunnel's roughly 0.61 ms additional serial latency dominates the ratio even though the
absolute latency is still below one millisecond on this host. The same effect appears for a 1 MiB
download: direct takes about 0.69 ms per response while the tunnel takes about 3.84 ms.

At c16 or c64, independent connections overlap those waits and use several cores and transports,
so aggregate throughput approaches direct. Control-session pooling and the dedicated pool mainly
improve that aggregate parallelism; they do not stripe or parallelize one ordered visitor TCP
stream. Making c1 approach the loopback direct baseline would instead require reducing the serial
per-byte and per-request work (copies, TLS passes, wakeups, and proxy boundaries), or comparing
against a direct baseline with equivalent endpoint TLS. Opening more transports alone cannot
improve a workload that has only one active connection.

## Protocol versioning

The client and server require `X-Tnl-Protocol-Version: 6` on tunnel and transport CONNECT
requests and responses. Missing or unsupported versions fail registration instead of silently
selecting a legacy data path. The server still advertises the required pool sizes; these headers
are mandatory in protocol v6. Dedicated transports are authenticated and bound to the registered
tunnel owner before entering the pool.

Mux remains part of protocol v6 for sidebands and as a bounded fallback when all dedicated
transports are active. It is not retained to support old peers.
