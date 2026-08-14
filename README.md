# tnl

Expose a local HTTP service through your own public `tnld` server. The public endpoint uses HTTPS.

## Installation

Install the latest release on Linux or macOS:

```sh
curl -fsSL https://raw.githubusercontent.com/bansalarnav/tnl/main/install.sh | sh
```

The installer detects the operating system and processor, verifies the release checksum, and installs `tnlc` and `tnld`. Linux users install into `~/.local/bin`. On macOS, the installer uses `/usr/local/bin` when it is already writable and otherwise uses `~/.local/bin`. When run as root, it installs into `/usr/local/bin`. If the selected directory is not in `PATH`, the installer adds it to the appropriate shell startup file.

Restart the shell after the first installation if the installer updated `PATH`. Run the same command again later to upgrade. Options can be passed through `sh` to install a specific release or choose a different directory:

```sh
curl -fsSL https://raw.githubusercontent.com/bansalarnav/tnl/main/install.sh | sh -s -- --version 0.0.1
curl -fsSL https://raw.githubusercontent.com/bansalarnav/tnl/main/install.sh | sh -s -- --install-dir /custom/bin --no-modify-path
```

### Build from source

Rust users can install either package directly from GitHub:

```sh
cargo install --git https://github.com/bansalarnav/tnl tnlc
cargo install --git https://github.com/bansalarnav/tnl tnld
```

## Setup

### 1. Set up the public server

The server needs a public IP address and TCP port 443 open. Install tnl on the server, then run:

```sh
tnld setup
tnld start --background
```

Setup detects the server's public IP and defaults to a free `nip.io` domain. If you use your own domain, add the DNS records printed by the command. Certificate issuance requires public TCP port 443 to reach the configured listen port.

Create credentials for a client:

```sh
tnld invite-client my-laptop
```

Copy the `tnl-login-v1...` value it prints.

### 2. Set up the local client

Install tnl on the local machine, then log in with the invitation from the server:

```sh
tnlc login 'tnl-login-v1...'
```

## Usage

If the local app is running on port 3000, expose it with:

```sh
tnlc expose 3000
```

The client prints the public HTTPS URL once it is ready. To request a memorable subdomain, add a name:

```sh
tnlc expose 3000 --name my-app
```

Keep this command running while the tunnel is in use. To stop a background server later, run:

```sh
tnld stop
```

## Project structure

The workspace contains three packages:

- `tnl-core` in `core/` (imported as `tnl`): transport-neutral tunnel sessions and connection pairing
- `tnlc`: the command-line client, including HTTPS transport, ACME, and local forwarding
- `tnld`: the command-line server, including authentication, HTTP upgrades, TLS, SNI routing, and public forwarding

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

The transport adapter registers each authenticated node connection with `TunnelServer`. Application code calls `open` to create a new logical stream to that node; no additional socket or authentication round trip is needed.

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

// After authenticating the node's transport, registration starts the
// multiplexed connection driver internally.
server.register(node_id.clone(), control).await?;

// Node-initiated streams can be handled elsewhere through the server.
let incoming_server = server.clone();
tokio::spawn(async move {
    let (node_id, incoming) = incoming_server.accept().await?;
    println!("incoming stream from {node_id}");
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

The node is unregistered when its connection driver ends. Call `TunnelServer::shutdown` during graceful shutdown to close all sessions and reject new registrations and streams.

Heartbeat settings must match on both endpoints. Heartbeats are disabled by default; when enabled, a missed deadline closes the multiplexed session. The default maximum is 512 concurrent streams per physical connection.

The HTTP `CONNECT` implementation used by the CLI is deliberately outside core in `tnlc` and `tnld`.
