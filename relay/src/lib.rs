//! An embeddable public relay for tnl tunnels.
//!
//! Nodes register tunnels over the relay's HTTPS API. Visitors then connect to
//! `https://<tunnel>.<domain>`, and the relay routes each connection by its TLS
//! server name to the node, without decrypting it. Only connections for the
//! relay's own `domain` are terminated, using the configured certificate.
//!
//! One port serves the API, visitors, and nodes' dedicated data transports:
//!
//! ```no_run
//! # async fn run(api_tls: std::sync::Arc<rustls::ServerConfig>) -> std::io::Result<()> {
//! use tnl_relay::{ClientId, Relay, RelayConfig};
//!
//! let relay = Relay::new(
//!     RelayConfig::new("relay.example.com", api_tls),
//!     |token: &str| (token == "secret").then(|| ClientId::new("me")),
//! );
//! relay.serve(tokio::net::TcpListener::bind("0.0.0.0:443").await?).await
//! # }
//! ```

mod api;
mod data;
mod forward;
mod registry;
mod tls;

use std::{
    fmt, io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use rustls::ServerConfig;
use tnl_core::{SessionConfig, TunnelId, data_transport, net};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    task::JoinSet,
    time::timeout,
};
use tracing::{Instrument, debug, info_span, warn};

use crate::registry::Registry;

pub use tnl_core;

const DATA_TRANSPORT_TIMEOUT: Duration = Duration::from_secs(10);

/// The authenticated client behind an API request.
///
/// A tunnel belongs to the client that registered it first, until all of its
/// control sessions close.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ClientId(Arc<str>);

impl ClientId {
    pub fn new(id: impl Into<Arc<str>>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ClientId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Decides which bearer tokens may register tunnels.
pub trait Authenticator: Send + Sync + 'static {
    /// Returns the client `token` belongs to, or `None` to reject it.
    fn authenticate(&self, token: &str) -> Option<ClientId>;
}

impl<F> Authenticator for F
where
    F: Fn(&str) -> Option<ClientId> + Send + Sync + 'static,
{
    fn authenticate(&self, token: &str) -> Option<ClientId> {
        self(token)
    }
}

pub struct RelayConfig {
    domain: String,
    api_tls: Arc<ServerConfig>,
    acme_challenge_tls: Option<Arc<ServerConfig>>,
    session: SessionConfig,
}

impl RelayConfig {
    /// Serves the API on `domain` and tunnels on its subdomains. `api_tls`
    /// terminates API connections.
    pub fn new(domain: impl Into<String>, api_tls: Arc<ServerConfig>) -> Self {
        Self {
            domain: domain.into().trim_end_matches('.').to_ascii_lowercase(),
            api_tls,
            acme_challenge_tls: None,
            session: SessionConfig::default(),
        }
    }

    /// Answers TLS-ALPN-01 challenges for `domain` with this configuration.
    pub fn acme_challenge_tls(mut self, config: Arc<ServerConfig>) -> Self {
        self.acme_challenge_tls = Some(config);
        self
    }

    /// Sets how control sessions with nodes behave.
    pub fn session(mut self, config: SessionConfig) -> Self {
        self.session = config;
        self
    }
}

/// A relay. Clones share the same tunnels.
#[derive(Clone)]
pub struct Relay {
    inner: Arc<Inner>,
}

struct Inner {
    domain: String,
    tunnel_suffix: String,
    api_tls: Arc<ServerConfig>,
    acme_challenge_tls: Option<Arc<ServerConfig>>,
    authenticator: Box<dyn Authenticator>,
    registry: Registry,
    next_transport_id: AtomicU64,
}

impl Relay {
    pub fn new(config: RelayConfig, authenticator: impl Authenticator) -> Self {
        Self {
            inner: Arc::new(Inner {
                tunnel_suffix: format!(".{}", config.domain),
                domain: config.domain,
                api_tls: config.api_tls,
                acme_challenge_tls: config.acme_challenge_tls,
                authenticator: Box::new(authenticator),
                registry: Registry::new(config.session),
                next_transport_id: AtomicU64::new(1),
            }),
        }
    }

    /// Accepts and handles connections until accepting fails. Dropping the
    /// returned future drops every connection it accepted.
    pub async fn serve(&self, listener: TcpListener) -> io::Result<()> {
        let mut connections = JoinSet::new();
        loop {
            let (stream, peer) = listener.accept().await?;
            if let Err(error) = net::configure_tcp_stream(&stream) {
                warn!(%peer, "could not configure TCP socket: {error}");
            }
            while connections.try_join_next().is_some() {}
            connections.spawn(
                self.clone()
                    .handle_connection(stream)
                    .instrument(info_span!("connection", %peer)),
            );
        }
    }

    /// Handles one accepted connection: an API request, a visitor, or a data
    /// transport. Errors are logged.
    pub async fn handle_connection<S>(self, stream: S)
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        if let Err(error) = self.route(stream).await
            && !net::is_routine_disconnect(error.as_ref())
        {
            warn!("connection failed: {error:#}");
        }
    }

    /// Closes every control session and rejects new ones.
    pub fn shutdown(&self) {
        self.inner.registry.shutdown();
    }

    async fn route<S>(&self, mut stream: S) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let first_byte = stream
            .read_u8()
            .await
            .context("could not read the connection preface")?;
        if first_byte == data_transport::PREFACE_BYTE {
            return timeout(
                DATA_TRANSPORT_TIMEOUT,
                data::accept(self, stream, first_byte),
            )
            .await
            .context("timed out adding a data transport")?;
        }
        if first_byte != tls::HANDSHAKE_RECORD {
            stream.shutdown().await?;
            return Ok(());
        }

        let hello = tls::read_client_hello(stream, first_byte).await?;
        let Some(server_name) = hello.server_name() else {
            return Ok(());
        };

        if server_name == self.inner.domain {
            if hello.is_acme_challenge()
                && let Some(config) = &self.inner.acme_challenge_tls
            {
                let mut stream = hello.terminate(Arc::clone(config)).await?;
                stream.shutdown().await?;
                return Ok(());
            }
            let stream = hello.terminate(Arc::clone(&self.inner.api_tls)).await?;
            return api::serve(self.clone(), stream).await;
        }

        let Some(tunnel_id) = server_name
            .strip_suffix(&self.inner.tunnel_suffix)
            .and_then(|name| TunnelId::new(name).ok())
        else {
            debug!(server_name, "no tunnel for server name");
            return Ok(());
        };
        let (client_hello, mut visitor) = hello.into_parts();
        forward::forward(
            &self.inner.registry,
            &tunnel_id,
            &client_hello,
            &mut visitor,
        )
        .await
    }

    fn tunnel_url(&self, tunnel_id: &TunnelId) -> String {
        format!("https://{tunnel_id}.{}", self.inner.domain)
    }

    fn next_transport_id(&self) -> u64 {
        self.inner.next_transport_id.fetch_add(1, Ordering::Relaxed)
    }
}
