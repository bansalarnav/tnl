# Throughput optimization guide

This guide explains why each selected optimization exists, how the data paths work, and where
the implementation lives. Measurements and rejected experiments remain in
[OPTIMIZATION_RESULTS.md](OPTIMIZATION_RESULTS.md) and [ROUND2_RESULTS.md](ROUND2_RESULTS.md).

## Data paths

The original and fallback path carries each visitor connection as a logical muxado stream:

```text
visitor -> tnld visitor socket -> mux stream -> outer TLS/TCP control session
        -> tnlc -> visitor TLS termination -> local backend TCP socket
```

The second pass adds a pool of authenticated outer TLS/TCP connections. For a normal bulk
connection, one idle transport is removed from the pool and becomes that visitor connection's
data plane:

```text
visitor -> tnld visitor socket -> dedicated outer TLS/TCP transport
        -> tnlc -> visitor TLS termination -> local backend TCP socket
```

`tnld` writes `TRANSPORT_ACTIVATION_MARKER` before application bytes so the waiting `tnlc`
worker knows that its idle transport has been claimed. The visitor byte stream is not striped
across transports: one visitor TCP connection always retains one ordered data path.

The mux path remains necessary for compatibility and is faster for bursts of tiny, short-lived
connections because it reuses an established control connection instead of consuming and then
replacing a dedicated TLS connection.

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
avoidable overhead. The second pass moves application bytes onto a claimed raw outer TLS stream;
the mux connections continue to carry fallback traffic and tunnel control.

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

### Warm pool, bounded replenishment, and burst wait

The server recommends 32 idle transports. Client workers replenish claimed transports, but a
64-permit semaphore bounds idle plus active dedicated transports. This avoids a burst of TLS
handshakes competing with the payload workload. If a burst temporarily empties the pool, `tnld`
waits up to 250 ms for replenishment, then falls back to mux rather than blocking indefinitely.

- Recommended and maximum sizes plus async availability:
  [`core/src/server/mod.rs`](../../core/src/server/mod.rs)
- Client workers and semaphore: [`tnlc/src/tunnel.rs`](../../tnlc/src/tunnel.rs)
- Bounded wait and fallback: [`tnld/src/server/tunnel.rs`](../../tnld/src/server/tunnel.rs)

The interaction matters: an uncapped replenisher produced handshake storms, while too small a
warm pool forced c64 bursts back through mux before replacement connections were ready.

### Short-connection circuit breaker

Dedicated transports are single-use at the visitor-connection level. If four consecutive claimed
transports each finish within 100 ms and transfer at most 64 KiB, core prefers mux for two seconds.
A longer or larger connection immediately resets this signal. The logic is in
`TunnelServer::transport_pool_preferred` and `TunnelServer::report_transport_outcome` in
[`core/src/server/mod.rs`](../../core/src/server/mod.rs), with outcomes reported by
[`tnld/src/server/tunnel.rs`](../../tnld/src/server/tunnel.rs).

This lifted the fresh-connection case from about 533 req/s with unconditional dedicated
transports to 1,518 req/s, close to the first pass's mux-based result.

### Outer TLS cipher preference

The outer tunnel TLS configuration prefers TLS 1.3 AES-128-GCM on this host. It was the best
measured choice for this workload and avoids spending cycles on a wider key than required. This
does not change visitor endpoint TLS configuration.

- Client provider ordering: [`tnlc/src/tunnel.rs`](../../tnlc/src/tunnel.rs)
- Server transport provider ordering: [`tnld/src/server/tls.rs`](../../tnld/src/server/tls.rs)

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
backend. The tunneled measurement intentionally includes visitor TLS, `tnld`, an outer TLS data
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

## Compatibility

Both features are server-advertised. A new client defaults to one control session and no
dedicated pool if the new headers are absent; an old client ignores the headers and continues to
use one mux connection. Dedicated transports are authenticated and bound to the registered
tunnel owner before entering the pool.
