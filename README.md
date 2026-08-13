Currently AI slop, read with caution

--------

# tnl

Expose a local HTTP service through your own public `tnld` server. The public endpoint uses HTTPS.

The workspace contains three packages:

- `tnl-core` in `core/` (imported as `tnl`): the embeddable tunneling library, with additive `client`, `forwarding`, and `server` features
- `tnlc`: the command-line tunnel client
- `tnld`: the command-line tunnel server

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

## Embed the client

Depend on the core package with its forwarding feature:

```toml
tnl = { package = "tnl-core", path = "core", features = ["forwarding"] }
```

Then construct the client from application-owned values:

```rust,no_run
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use tnl::{TunnelId, client::{Client, Forwarder}};
use url::Url;

# async fn example() -> anyhow::Result<()> {
let client = Client::new(Url::parse("https://tunnel.example.com")?)?
    .with_authorization("Bearer secret-token")?;
let forwarder = Forwarder::new(client, PathBuf::from("./acme-cache"));
let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3000);
forwarder.expose(target, TunnelId::new("my-app")?).await?;
# Ok(())
# }
```

The `server` feature exposes the corresponding `Server`, `TunnelRegistry`, API router, authenticated identity, hostname-routing, and event types. Applications provide their own listener, authentication middleware, hostname policy, certificates, and event handling.

## Use raw bidirectional connections

The lower-level `client` feature does not depend on forwarding or certificate management. A node registers a session and accepts connection requests from the central server:

```rust,no_run
use tnl::{TunnelId, client::Client};
use url::Url;

# async fn node() -> anyhow::Result<()> {
let client = Client::new(Url::parse("https://tunnel.example.com")?)?
    .with_authorization("Bearer secret-token")?;
let mut session = client.register(&TunnelId::new("my-node")?).await?;
session.wait_until_ready().await?;

loop {
    let request = session.accept().await?;
    tokio::spawn(async move {
        let connection = request.connect().await?;
        // `connection` implements Tokio AsyncRead + AsyncWrite. Serve gRPC,
        // another framed protocol, or application-specific traffic over it.
        drop(connection);
        Ok::<_, anyhow::Error>(())
    });
}
# }
```

Keep the accept loop running and handle each connection concurrently so the session can continue answering heartbeats.

On the central side, `TunnelRegistry::connect` requests a matching raw stream from that node. This can be used independently of `Server`; create a plain `TunnelRegistry::new()`, mount `api_router` in an existing authenticated Axum service, and insert a `ClientIdentity` in middleware. Call `TunnelRegistry::shutdown` during graceful shutdown to close registered control sessions and pending connection requests. The returned client and server connection types both implement Tokio `AsyncRead` and `AsyncWrite`, so forwarding is only one possible use. Public hostname forwarding can instead use `TunnelRegistry::with_public_url` to customize the ready value sent to clients. Use `Client::with_tls_config` when nodes need a private CA or mutual TLS.
