Currently AI slop, read with caution

--------

# tnl

Expose a local HTTP service through your own public `tnld` server. The public endpoint uses HTTPS.

The workspace contains three packages:

- `tnl-core` in `core/` (imported as `tnl`): transport-neutral tunnel sessions and connection pairing
- `tnlc`: the command-line client, including HTTPS transport, ACME, and local forwarding
- `tnld`: the command-line server, including authentication, HTTP upgrades, TLS, SNI routing, and public forwarding

## Requirements

- Rust and Cargo
- A public server with TCP port 443 open
- A local HTTP service to expose

## 1. Set up the public server

Clone this repository on the server, then run:

```sh
cargo run --release -p tnld -- setup
cargo run --release -p tnld -- start --background
```

Setup detects the server's public IP and defaults to a free `nip.io` domain. If you use your own domain, add the DNS records printed by the command. Certificate issuance requires public TCP port 443 to reach the configured listen port.

Create credentials for a client:

```sh
cargo run --release -p tnld -- invite-client my-laptop
```

Copy the `tnl-login-v1...` value it prints.

## 2. Log in from your local machine

Clone this repository locally and run:

```sh
cargo run --release -p tnlc -- login 'tnl-login-v1...'
```

## 3. Expose a local service

If your app is running on port 3000:

```sh
cargo run --release -p tnlc -- expose 3000
```

The client prints the public HTTPS URL once it is ready. To request a memorable subdomain, add a name:

```sh
cargo run --release -p tnlc -- expose 3000 --name my-app
```

Keep this command running while the tunnel is in use. To stop a background server later, run:

```sh
cargo run --release -p tnld -- stop
```

## Core API

`tnl-core` multiplexes independent bidirectional streams over one application-provided connection using muxado. It does not open sockets or depend on HTTP, TLS, Axum, Hyper, ACME, hostnames, or authentication policy.

Enable either or both sides:

```toml
tnl = { package = "tnl-core", path = "core", features = ["client", "server"] }
```

### Node side

Pass an already-established Tokio `AsyncRead + AsyncWrite` stream to `TunnelClient`. How that connection is authenticated and transported is application-owned.

```rust,no_run
use std::time::Duration;
use tnl::{client::TunnelClient, SessionConfig};

# async fn node<S>(control_stream: S) -> anyhow::Result<()>
# where S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static {
let config = SessionConfig::new()
    .heartbeat(Duration::from_secs(20), Duration::from_secs(60))
    .max_concurrent_streams(256);
let client = TunnelClient::new(control_stream, config).await?;

// Either endpoint can initiate a new independent stream.
let outgoing = client.open("grpc").await?;
tokio::spawn(async move {
    drop(outgoing);
});

loop {
    let stream = client.accept().await?;
    println!("incoming protocol: {:?}", stream.tag());

    tokio::spawn(async move {
        // `stream` implements Tokio AsyncRead + AsyncWrite. Serve gRPC,
        // a framed protocol, or arbitrary bidirectional traffic over it.
        drop(stream);
    });
}
# }
```

Keep accepting while individual streams run concurrently. A background task drives the multiplexed connection, so cloned client handles may call `open` and `accept` concurrently.

### Central side

The transport adapter registers each authenticated node with `TunnelServer`, then runs its `RegisteredTunnel` over the established connection. Application code calls `open` to create a new logical stream to that node; no additional socket or authentication round trip is needed.

```rust,no_run
use std::time::Duration;
use tnl::{SessionConfig, TunnelId, server::TunnelServer};

# async fn central<Control>(control: Control) -> anyhow::Result<()>
# where
#     Control: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
# {
let config = SessionConfig::new()
    .heartbeat(Duration::from_secs(20), Duration::from_secs(60))
    .max_concurrent_streams(256);
let server = TunnelServer::new(config);
let node_id = TunnelId::new("node-42")?;

// After authenticating the node's transport:
let tunnel = server
    .register(node_id.clone())
    .expect("node is already registered");
tokio::spawn(tunnel.clone().serve(control));

// Node-initiated streams are accepted from the same registered tunnel handle.
let incoming_tunnel = tunnel.clone();
tokio::spawn(async move {
    let incoming = incoming_tunnel.accept().await?;
    match incoming.tag() {
        "grpc" => { /* serve gRPC over `incoming` */ }
        "files" => { /* receive a file */ }
        _ => { /* reject an unsupported protocol */ }
    }
    drop(incoming);
    Ok::<_, tnl::ConnectionError>(())
});

// From central application code:
if let Some(stream) = server.open(&node_id, "grpc").await? {
    // `stream` implements Tokio AsyncRead + AsyncWrite and is independent
    // of every other logical stream in the same session.
    drop(stream);
}
# Ok(())
# }
```

The node is unregistered when its session driver ends or its last `RegisteredTunnel` handle is dropped. Call `TunnelServer::shutdown` during graceful shutdown to close all sessions and reject new registrations and streams.

Heartbeat settings must match on both endpoints. Heartbeats are disabled by default; when enabled, a missed deadline closes the multiplexed session. The default maximum is 512 concurrent streams per physical connection.

The HTTP `CONNECT` implementation used by the CLI is deliberately outside core in `tnlc` and `tnld`.
