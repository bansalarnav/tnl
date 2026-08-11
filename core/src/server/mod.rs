mod api;
mod http;
mod tls;
mod tunnel;

use std::{io, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Result;
use axum::Router;
use rustls::ServerConfig;
use socket2::{SockRef, TcpKeepalive};
use tokio::{io::AsyncWriteExt, net::TcpListener};

use crate::TunnelId;

pub use api::router as api_router;
pub use tunnel::TunnelRegistry;

const TCP_KEEPALIVE_IDLE: Duration = Duration::from_secs(30);
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const TCP_KEEPALIVE_RETRIES: u32 = 3;

#[derive(Clone, Debug)]
pub struct ClientIdentity(Arc<str>);

impl ClientIdentity {
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub enum ServerEvent {
    TunnelDisconnected {
        tunnel_id: TunnelId,
        error: String,
    },
    DataConnectionUpgradeFailed {
        error: String,
    },
    ConnectionFailed {
        peer_address: SocketAddr,
        error: String,
    },
}

pub type EventHandler = Arc<dyn Fn(ServerEvent) + Send + Sync>;

pub fn event_handler(handler: impl Fn(ServerEvent) + Send + Sync + 'static) -> EventHandler {
    Arc::new(handler)
}

#[derive(Clone, Debug)]
pub enum HostRoute {
    Api,
    Tunnel(TunnelId),
    Reject,
}

type HostRouter = Arc<dyn Fn(&str) -> HostRoute + Send + Sync>;

pub struct Server {
    api_tls_config: Arc<ServerConfig>,
    acme_tls_config: Arc<ServerConfig>,
    api_router: Router,
    tunnels: TunnelRegistry,
    route_host: HostRouter,
    events: EventHandler,
}

impl Server {
    pub fn new(
        api_tls_config: Arc<ServerConfig>,
        acme_tls_config: Arc<ServerConfig>,
        api_router: Router,
        tunnels: TunnelRegistry,
        route_host: impl Fn(&str) -> HostRoute + Send + Sync + 'static,
        events: EventHandler,
    ) -> Self {
        Self {
            api_tls_config,
            acme_tls_config,
            api_router,
            tunnels,
            route_host: Arc::new(route_host),
            events,
        }
    }

    pub async fn serve(self, listener: TcpListener) -> Result<()> {
        let server = Arc::new(self);
        loop {
            let (stream, peer_address) = listener.accept().await?;
            if let Err(error) = configure_tcp_keepalive(&stream) {
                (server.events)(ServerEvent::ConnectionFailed {
                    peer_address,
                    error: format!("could not configure TCP keepalive: {error}"),
                });
            }
            let server = Arc::clone(&server);
            tokio::spawn(async move {
                if let Err(error) = server.handle_connection(stream).await
                    && !is_routine_connection_error(&error)
                {
                    (server.events)(ServerEvent::ConnectionFailed {
                        peer_address,
                        error: format!("{error:#}"),
                    });
                }
            });
        }
    }

    async fn handle_connection(&self, stream: tokio::net::TcpStream) -> Result<()> {
        let connection = match tls::inspect(stream).await? {
            Some(connection) => connection,
            None => return Ok(()),
        };

        let Some(server_name) = connection.server_name() else {
            return Ok(());
        };

        match (self.route_host)(server_name) {
            HostRoute::Api if connection.is_acme_challenge() => {
                let mut stream = connection.terminate(self.acme_tls_config.clone()).await?;
                stream.shutdown().await?;
                Ok(())
            }
            HostRoute::Api => {
                let stream = connection.terminate(self.api_tls_config.clone()).await?;
                http::serve(stream, self.api_router.clone()).await
            }
            HostRoute::Tunnel(tunnel_id) => self.tunnels.forward(&tunnel_id, connection).await,
            HostRoute::Reject => Ok(()),
        }
    }
}

fn configure_tcp_keepalive(stream: &tokio::net::TcpStream) -> io::Result<()> {
    SockRef::from(stream).set_tcp_keepalive(
        &TcpKeepalive::new()
            .with_time(TCP_KEEPALIVE_IDLE)
            .with_interval(TCP_KEEPALIVE_INTERVAL)
            .with_retries(TCP_KEEPALIVE_RETRIES),
    )
}

fn is_routine_connection_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if let Some(error) = cause.downcast_ref::<io::Error>() {
            return matches!(
                error.kind(),
                io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
            );
        }

        cause.downcast_ref::<rustls::Error>().is_some_and(|error| {
            matches!(
                error,
                rustls::Error::InappropriateMessage { .. }
                    | rustls::Error::InappropriateHandshakeMessage { .. }
                    | rustls::Error::InvalidMessage(_)
                    | rustls::Error::PeerIncompatible(_)
                    | rustls::Error::PeerMisbehaved(_)
                    | rustls::Error::AlertReceived(_)
                    | rustls::Error::NoApplicationProtocol
            )
        })
    })
}
