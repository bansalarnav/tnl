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
use tnl_core::{SessionConfig, TunnelId, net, quic};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
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
        let (inherited_nodelay, inherited_keepalive) = net::configure_tcp_listener(&listener);
        let mut connections = JoinSet::new();

        // Bind a QUIC/UDP endpoint on the same address/port as the TCP listener
        // so nodes can establish multiplexed QUIC data connections when UDP is
        // available. Failures are non-fatal: nodes seamlessly use TCP transports.
        if let Ok(local_addr) = listener.local_addr() {
            match self.bind_quic_endpoint(local_addr) {
                Ok(endpoint) => {
                    let relay = self.clone();
                    connections.spawn(async move {
                        relay.accept_quic_loop(endpoint).await;
                    });
                }
                Err(error) => {
                    warn!(%local_addr, "could not start QUIC UDP listener: {error:#}");
                }
            }
        }

        loop {
            let (stream, peer) = listener.accept().await?;
            if !inherited_nodelay
                && let Err(error) = stream.set_nodelay(true)
            {
                warn!(%peer, "could not disable Nagle's algorithm: {error}");
            }
            if !inherited_keepalive
                && let Err(error) = net::configure_tcp_keepalive(&stream)
            {
                warn!(%peer, "could not configure TCP keepalive: {error}");
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

    async fn route<S>(&self, stream: S) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        match tls::inspect_connection(stream, &self.inner.domain).await? {
            None => Ok(()),
            Some(tls::InspectedConnection::DataTransport { prefix, stream }) => timeout(
                DATA_TRANSPORT_TIMEOUT,
                data::accept(self, stream, prefix.as_slice()),
            )
            .await
            .context("timed out adding a data transport")?,
            Some(tls::InspectedConnection::Api(hello)) => {
                if hello.is_acme_challenge()
                    && let Some(config) = &self.inner.acme_challenge_tls
                {
                    let mut stream = hello.terminate(Arc::clone(config)).await?;
                    stream.shutdown().await?;
                    return Ok(());
                }
                let stream = hello.terminate(Arc::clone(&self.inner.api_tls)).await?;
                api::serve(self.clone(), stream).await
            }
            Some(tls::InspectedConnection::Tunnel(hello)) => {
                let Some(tunnel_id) = hello
                    .server_name()
                    .strip_suffix(&self.inner.tunnel_suffix)
                    .and_then(|name| TunnelId::new(name).ok())
                else {
                    debug!(
                        server_name = hello.server_name(),
                        "no tunnel for server name"
                    );
                    return Ok(());
                };
                let (prefixed_hello, mut visitor) = hello.into_parts();
                forward::forward(
                    &self.inner.registry,
                    &tunnel_id,
                    &prefixed_hello,
                    &mut visitor,
                )
                .await
            }
        }
    }

    fn tunnel_url(&self, tunnel_id: &TunnelId) -> String {
        format!("https://{tunnel_id}.{}", self.inner.domain)
    }

    fn next_transport_id(&self) -> u64 {
        self.inner.next_transport_id.fetch_add(1, Ordering::Relaxed)
    }

    fn bind_quic_endpoint(&self, addr: std::net::SocketAddr) -> Result<quinn::Endpoint> {
        let mut tls = (*self.inner.api_tls).clone();
        tls.alpn_protocols = vec![quic::QUIC_ALPN.to_vec()];
        tls.ignore_client_order = true;
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(Arc::new(tls))
            .context("could not build QUIC server TLS config")?;
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        server_config.transport_config(quic::transport_config());
        let udp_socket = quic::bind_udp_socket(addr).context("could not bind QUIC UDP socket")?;
        let endpoint = quinn::Endpoint::new(
            quinn::EndpointConfig::default(),
            Some(server_config),
            udp_socket,
            Arc::new(quinn::TokioRuntime),
        )
        .context("could not create QUIC endpoint")?;
        Ok(endpoint)
    }

    async fn accept_quic_loop(self, endpoint: quinn::Endpoint) {
        let mut tasks = JoinSet::new();
        while let Some(incoming) = endpoint.accept().await {
            while tasks.try_join_next().is_some() {}
            let relay = self.clone();
            tasks.spawn(async move {
                if let Err(error) = relay.handle_quic_connection(incoming).await
                    && !net::is_routine_disconnect(error.as_ref())
                {
                    debug!("QUIC data connection ended: {error:#}");
                }
            });
        }
    }

    async fn handle_quic_connection(&self, incoming: quinn::Incoming) -> Result<()> {
        let conn = timeout(DATA_TRANSPORT_TIMEOUT, incoming)
            .await
            .context("timed out completing QUIC handshake")?
            .context("QUIC handshake failed")?;

        let registry = &self.inner.registry;
        let mut grant_generation = None;
        let tunnel_id = timeout(
            DATA_TRANSPORT_TIMEOUT,
            quic::authenticate_server(&conn, |tunnel_id| {
                let grant = registry.transport_grant(tunnel_id)?;
                grant_generation = Some(grant.generation);
                Some(grant.key)
            }),
        )
        .await
        .context("timed out authenticating QUIC data connection")?
        .context("QUIC data connection authentication failed")?;

        let generation = grant_generation.context("tunnel grant missing")?;
        let conn_id = self.next_transport_id();
        registry.add_quic_connection(&tunnel_id, generation, conn_id, conn.clone())?;
        debug!(%tunnel_id, conn_id, "QUIC data connection registered");
        let _ = conn.closed().await;
        registry.remove_quic_connection(&tunnel_id, generation, conn_id);
        debug!(%tunnel_id, conn_id, "QUIC data connection closed");
        Ok(())
    }
}
