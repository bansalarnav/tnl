# Candidate optimizations, third pass

Hypotheses, not measured results. Rounds one and two are recorded in
[OPTIMIZATION_RESULTS.md](OPTIMIZATION_RESULTS.md) and [ROUND2_RESULTS.md](ROUND2_RESULTS.md);
the shipped design is described in [OPTIMIZATIONS.md](OPTIMIZATIONS.md).

Several of these target costs that the current loopback harness cannot observe. The harness
gaps in [NEXT_HARNESS.md](NEXT_HARNESS.md) should be closed first, or at least far enough to
tell whether the cost being targeted is real under realistic conditions.

Ordered by expected value per unit of effort.

## 1. Reusable dedicated transports

**Change.** A dedicated transport currently serves exactly one visitor connection and is then
consumed: `take_transport` pops it from the pool (`core/src/server/mod.rs:229`) and it is never
returned. Add a connection boundary to the transport protocol so an idle transport can rejoin
the pool after the visitor disconnects.

**Why it should work.** Single-use is the root cause of almost all the pool machinery. The warm
pool of 32, the 64-permit client semaphore, the 250 ms replenishment wait
(`tnld/src/server/tunnel.rs:11`), and the entire short-connection circuit breaker
(`core/src/server/mod.rs:23-26`) exist to hide the cost of replacing a consumed TLS connection.
If transports are reused, that cost mostly disappears along with the machinery that hides it.

**Sketch.** The activation marker (`core/src/lib.rs:33`) already establishes a framing point at
the start of a claimed transport. A symmetric completion marker, or a length-prefixed
connection record, lets `tnlc` recognise that the visitor connection ended and re-register the
transport rather than closing it. Framing cost is per visitor connection, not per 64 KiB block,
so it does not reintroduce mux's per-frame overhead.

**Risks.** Any desynchronisation between the two ends now corrupts a *subsequent* visitor
connection rather than just failing the current one. Needs a strict rule that a transport is
retired rather than reused on any error, timeout, or partial write. This is the main reason to
treat it as a careful change rather than an obvious one.

**Validation.** Fresh-connection c8 should approach the dedicated-transport bulk path instead of
requiring mux fallback. If it does, try removing the circuit breaker entirely and confirm the
number holds.

## 2. TLS session resumption on transport replenishment

**Change.** Enable and use TLS session tickets for the outer transport connections that `tnlc`
opens to replenish the pool.

**Why it should work.** Every replacement transport currently performs a full handshake,
including certificate verification and a fresh key exchange. Resumption cuts that to one round
trip and a fraction of the CPU. This attacks the same cost as item 1 from the opposite
direction, and the two are partially redundant: if transports become reusable, replenishment
happens far less often and resumption matters correspondingly less.

**Sketch.** Client-side resumption storage on the `tnlc` connector, ticket issuance on the
`tnld` transport acceptor. No protocol change; both ends already negotiate TLS 1.3.

**Risks.** Low. Ticket lifetime and storage bounds need attention. Note that resumption reduces
forward secrecy relative to a full handshake, which is a normal and accepted tradeoff but should
be a deliberate one.

**Validation.** Measure handshake CPU and replenishment latency directly rather than inferring
from throughput. The uncapped-replenishment experiments in `ROUND2_RESULTS.md` that were
discarded for causing handshake storms are worth re-running afterwards; resumption may make a
more aggressive pool viable.

## 3. Backend connection pooling in `tnlc`

**Change.** `tnlc` opens a fresh TCP connection to the local backend for each visitor connection
(`tnlc/src/tunnel.rs:404`). Keep a small pool of established backend connections instead.

**Why it should work.** It removes a `connect()` and a scheduling boundary from the serial
per-request path, which is the ~0.61 ms concurrency-one penalty the optimization guide
identifies as the remaining gap. Cheap to implement and independent of everything else here.

**Risks.** Backends may not tolerate connection reuse identically, and a pooled connection
carrying a half-consumed HTTP response is a correctness hazard. Safest as a plain TCP-level pool
only where the tunnel is not parsing HTTP, with connections retired on any ambiguity.

**Validation.** Concurrency-one empty response, and the fresh-connection case.

## 4. Zero-copy relay on `tnld`, which requires rethinking outer TLS

**Observation.** Visitor traffic is already TLS-encrypted end to end between the visitor and
`tnlc`; `tnld` forwards the ClientHello and never terminates visitor TLS. That already-encrypted
byte stream is then wrapped in outer transport TLS for the `tnld` to `tnlc` hop. Every payload
byte is therefore encrypted twice and decrypted twice.

**Change.** Allow the outer transport to negotiate away record-layer encryption after the
transport has been authenticated, when the inner stream is known to be TLS. `tnld`'s forwarding
loop then becomes a plain TCP-to-TCP relay and can use `splice(2)` to move bytes kernel to
kernel, eliminating the userspace copies in `copy_bidirectional_with_sizes`
(`tnld/src/server/tunnel.rs:75`) as well as a full AEAD pass on each side.

**Why it should work.** This is the largest remaining per-byte cost on the server. It removes
both a crypto pass and two copies per byte in each direction.

**Risks.** Substantial, and the reason this is not ranked first despite the size of the prize.
Payload confidentiality and integrity survive via inner TLS, but the hop loses metadata
protection: an on-path observer between `tnld` and `tnlc` would see the visitor's SNI in the
forwarded ClientHello, plus connection timing and size patterns. This must be opt-in, never the
default, and it does not apply to plaintext inner traffic at all.

**Validation.** Bulk download and upload at all concurrency levels, plus server CPU per
transferred byte, which matters more than throughput here.

## 5. `io_uring` forwarding loops

**Change.** Move the forwarding loops off epoll-based Tokio onto `io_uring`, via `tokio-uring`
or `monoio`.

**Why it might work.** The concurrency-one path is dominated by wakeups and syscalls rather than
per-byte work, which is the workload `io_uring` targets. Batched submission and completion would
reduce the fixed cost per transfer.

**Risks.** Highest effort by a wide margin, and it fragments the codebase across platforms since
`io_uring` is Linux-only and needs a reasonably recent kernel. A partial migration means two
runtimes in one process.

**Validation.** Only worth attempting if instrumentation shows syscall and wakeup overhead is
genuinely the dominant remaining cost. Measure that first; do not start from the assumption.

## A framing caution before chasing concurrency-one

The direct baseline is plain HTTP over loopback. The tunnel path terminates TLS twice. Some of
the concurrency-one gap is that asymmetry rather than removable overhead, and the optimization
guide already says so.

Add a TLS-terminating direct baseline to the harness before spending effort here. It converts
"the tunnel is at 22.5% of direct" into a number that says how much of the gap is actually
addressable, and it may show that items 3 through 5 are chasing less headroom than the current
comparison implies.
