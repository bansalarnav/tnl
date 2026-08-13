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

`tnl-core` handles the control protocol, heartbeats, registration ownership, and connection pairing. It does not open sockets or depend on HTTP, TLS, Axum, Hyper, ACME, hostnames, or authentication policy.

Enable either or both sides:

```toml
tnl = { package = "tnl-core", path = "core", features = ["client", "server"] }
```

### Node side

Pass an already-established Tokio `AsyncRead + AsyncWrite` control stream to `ClientSession`. How that stream is authenticated and transported is application-owned.

```rust,no_run
use tnl::client::ClientSession;

# async fn node<S>(control_stream: S) -> anyhow::Result<()>
# where S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {
let mut session = ClientSession::new(control_stream);
let ready_value = session.wait_until_ready().await?;
println!("registered as {ready_value}");

loop {
    let request = session.accept().await?;
    let connection_id = request.into_id();

    tokio::spawn(async move {
        // Ask the application transport to open its data stream for
        // `connection_id`, then serve gRPC or another protocol over it.
        let _ = connection_id;
    });
}
# }
```

Keep accepting while individual connections run concurrently so the session continues answering heartbeats.

### Central side

`Broker<D>` is generic over the application’s data-stream type. The transport adapter registers authenticated control streams and attaches authenticated data streams; application code calls `connect` to initiate a connection to a node.

```rust,no_run
use tnl::{TunnelId, server::Broker};

# async fn central<Control, Data>(control: Control, data: Data) -> anyhow::Result<()>
# where
#     Control: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
#     Data: Send + 'static,
# {
let broker = Broker::<Data>::new();
let node_id = TunnelId::new("node-42")?;

// After authenticating the node's control transport:
let registration = broker
    .register(node_id.clone(), "authenticated-principal")
    .expect("node is already registered");
tokio::spawn(registration.serve(control));

// After authenticating a data transport and reading its connection ID:
# let connection_id = "example";
if let Some(attachment) = broker.claim(connection_id, "authenticated-principal") {
    let _ = attachment.attach(data);
}

// From central application code:
if let Some(stream) = broker.connect(&node_id).await? {
    // `stream` is the application's original `Data` type. It can carry
    // bidirectional gRPC, a framed protocol, or arbitrary traffic.
    drop(stream);
}
# Ok(())
# }
```

Use `Broker::with_ready_value` when the node needs an application-defined value after registration. Call `Broker::shutdown` during graceful shutdown; registrations and pending connections are then closed and rejected.

The HTTP `CONNECT` implementation used by the CLI is deliberately outside core in `tnlc` and `tnld`.
