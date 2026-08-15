use std::{
    collections::HashMap,
    error::Error,
    fmt,
    sync::{Arc, Mutex},
};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot, watch},
};

use crate::{ConnectionError, SessionConfig, Stream, TunnelId, session::SessionParts};

/// Maximum number of transport sessions pooled for one logical tunnel.
pub const MAX_SESSIONS_PER_TUNNEL: usize = 8;

type IncomingStream = Result<(TunnelId, Stream), ConnectionError>;
type IncomingStreamReceiver = Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<IncomingStream>>>;

struct OpenRequest {
    tag: String,
    response: oneshot::Sender<Result<Stream, ConnectionError>>,
}

/// Matches registered nodes with multiplexed sessions.
pub struct TunnelServer {
    config: SessionConfig,
    state: Arc<Mutex<TunnelRegistry>>,
    shutdown: watch::Sender<bool>,
    incoming_streams: IncomingStreamReceiver,
    incoming_sender: mpsc::UnboundedSender<IncomingStream>,
}

#[derive(Default)]
struct TunnelRegistry {
    shutdown: bool,
    next_session_id: u64,
    sessions: HashMap<String, RegisteredTunnel>,
}

struct RegisteredTunnel {
    owner: Option<String>,
    sessions: Vec<RegisteredSession>,
    next_session: usize,
}

struct RegisteredSession {
    session_id: u64,
    opener: mpsc::Sender<OpenRequest>,
}

impl TunnelServer {
    pub fn new(config: SessionConfig) -> Self {
        let (shutdown, _) = watch::channel(false);
        let (incoming_sender, incoming_streams) = mpsc::unbounded_channel();
        Self {
            config,
            state: Arc::new(Mutex::new(TunnelRegistry::default())),
            shutdown,
            incoming_streams: Arc::new(tokio::sync::Mutex::new(incoming_streams)),
            incoming_sender,
        }
    }

    /// Registers an authenticated node and starts serving its connection.
    pub async fn register<S>(&self, tunnel_id: TunnelId, connection: S) -> Result<(), RegisterError>
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        self.register_inner(tunnel_id, None, connection).await
    }

    /// Registers a session owned by an authenticated peer. A tunnel may pool
    /// multiple sessions only when all of them have the same owner.
    pub async fn register_with_owner<S>(
        &self,
        tunnel_id: TunnelId,
        owner: impl Into<String>,
        connection: S,
    ) -> Result<(), RegisterError>
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        self.register_inner(tunnel_id, Some(owner.into()), connection)
            .await
    }

    async fn register_inner<S>(
        &self,
        tunnel_id: TunnelId,
        owner: Option<String>,
        connection: S,
    ) -> Result<(), RegisterError>
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (session_id, open_requests) = {
            let mut state = self.state.lock().expect("server lock was poisoned");
            if state.shutdown {
                return Err(RegisterError::ServerShutdown);
            }
            let session_count = if let Some(tunnel) = state.sessions.get(tunnel_id.as_str()) {
                if owner.is_none() || tunnel.owner != owner {
                    return Err(RegisterError::AlreadyRegistered);
                }
                tunnel.sessions.len()
            } else {
                0
            };
            if session_count >= MAX_SESSIONS_PER_TUNNEL {
                return Err(RegisterError::AlreadyRegistered);
            }

            let session_id = state.next_session_id;
            state.next_session_id = state.next_session_id.wrapping_add(1);
            let (opener, open_requests) = mpsc::channel(32);
            state
                .sessions
                .entry(tunnel_id.to_string())
                .or_insert_with(|| RegisteredTunnel {
                    owner,
                    sessions: Vec::new(),
                    next_session: 0,
                })
                .sessions
                .push(RegisteredSession { session_id, opener });
            (session_id, open_requests)
        };

        let parts = match SessionParts::start(connection, false, &self.config).await {
            Ok(parts) => parts,
            Err(error) => {
                self.unregister(&tunnel_id, session_id);
                return Err(error.into());
            }
        };

        if self.is_shutdown() {
            self.unregister(&tunnel_id, session_id);
            return Err(RegisterError::ServerShutdown);
        }

        let server = self.clone();
        tokio::spawn(async move {
            let _ = server
                .serve(tunnel_id, session_id, open_requests, parts)
                .await;
        });
        Ok(())
    }

    /// Opens a tagged bidirectional stream to a registered node.
    pub async fn open(
        &self,
        tunnel_id: &TunnelId,
        tag: impl AsRef<str>,
    ) -> Result<Option<Stream>, ConnectionError> {
        let opener = {
            let mut state = self.state.lock().expect("server lock was poisoned");
            if state.shutdown {
                return Err(muxado::Error::SessionClosed.into());
            }
            let Some(tunnel) = state.sessions.get_mut(tunnel_id.as_str()) else {
                return Ok(None);
            };
            let session_index = tunnel.next_session % tunnel.sessions.len();
            tunnel.next_session = tunnel.next_session.wrapping_add(1);
            let session = &tunnel.sessions[session_index];
            session.opener.clone()
        };

        open(&opener, tag.as_ref()).await.map(Some)
    }

    /// Waits for the next stream opened by any registered node.
    pub async fn accept(&self) -> Result<(TunnelId, Stream), ConnectionError> {
        let mut incoming_streams = self.incoming_streams.lock().await;
        let mut shutdown = self.shutdown.subscribe();
        loop {
            if *shutdown.borrow() {
                return Err(muxado::Error::SessionClosed.into());
            }
            tokio::select! {
                incoming = incoming_streams.recv() => {
                    return incoming.unwrap_or_else(|| Err(muxado::Error::SessionClosed.into()));
                }
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        return Err(muxado::Error::SessionClosed.into());
                    }
                }
            }
        }
    }

    /// Closes all registered sessions.
    pub fn shutdown(&self) {
        let mut state = self.state.lock().expect("server lock was poisoned");
        state.shutdown = true;
        state.sessions.clear();
        drop(state);
        self.shutdown.send_replace(true);
    }

    pub fn is_shutdown(&self) -> bool {
        self.state
            .lock()
            .expect("server lock was poisoned")
            .shutdown
    }

    /// Completes when shutdown begins.
    pub async fn wait_for_shutdown(&self) {
        let mut shutdown = self.shutdown.subscribe();
        loop {
            if *shutdown.borrow() || shutdown.changed().await.is_err() {
                return;
            }
        }
    }

    async fn serve(
        &self,
        tunnel_id: TunnelId,
        session_id: u64,
        mut open_requests: mpsc::Receiver<OpenRequest>,
        mut parts: SessionParts,
    ) -> Result<(), ConnectionError> {
        let _unregister = UnregisterOnDrop {
            server: self.clone(),
            tunnel_id: tunnel_id.clone(),
            session_id,
        };
        let mut shutdown = self.shutdown.subscribe();

        loop {
            tokio::select! {
                request = open_requests.recv() => {
                    let Some(request) = request else {
                        break;
                    };
                    let stream = match parts.opener.lock().await.open().await {
                        Ok(stream) => Stream::outgoing(stream, request.tag).await,
                        Err(error) => Err(error.into()),
                    };
                    let session_closed = matches!(
                        stream,
                        Err(ConnectionError::Multiplexer(
                            muxado::Error::SessionClosed
                                | muxado::Error::RemoteGoneAway
                                | muxado::Error::PeerEOF
                        ))
                    );
                    let _ = request.response.send(stream);
                    if session_closed {
                        break;
                    }
                }
                inbound = parts.accepter.accept() => {
                    let stream = match inbound {
                        Ok(stream) => stream,
                        Err(muxado::Error::SessionClosed | muxado::Error::PeerEOF) => break,
                        Err(error) => return Err(error.into()),
                    };
                    let tunnel_id = tunnel_id.clone();
                    let incoming_sender = self.incoming_sender.clone();
                    tokio::spawn(async move {
                        let stream = Stream::incoming(stream)
                            .await
                            .map(|stream| (tunnel_id, stream));
                        let _ = incoming_sender.send(stream);
                    });
                }
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    fn unregister(&self, tunnel_id: &TunnelId, session_id: u64) {
        let mut state = self.state.lock().expect("server lock was poisoned");
        let remove_tunnel = if let Some(tunnel) = state.sessions.get_mut(tunnel_id.as_str()) {
            tunnel
                .sessions
                .retain(|session| session.session_id != session_id);
            tunnel.sessions.is_empty()
        } else {
            false
        };
        if remove_tunnel {
            state.sessions.remove(tunnel_id.as_str());
        }
    }
}

impl Clone for TunnelServer {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            state: Arc::clone(&self.state),
            shutdown: self.shutdown.clone(),
            incoming_streams: Arc::clone(&self.incoming_streams),
            incoming_sender: self.incoming_sender.clone(),
        }
    }
}

impl Default for TunnelServer {
    fn default() -> Self {
        Self::new(SessionConfig::default())
    }
}

struct UnregisterOnDrop {
    server: TunnelServer,
    tunnel_id: TunnelId,
    session_id: u64,
}

impl Drop for UnregisterOnDrop {
    fn drop(&mut self) {
        self.server.unregister(&self.tunnel_id, self.session_id);
    }
}

/// An error registering a node connection with a [`TunnelServer`].
#[derive(Debug)]
pub enum RegisterError {
    AlreadyRegistered,
    ServerShutdown,
    Connection(ConnectionError),
}

impl fmt::Display for RegisterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyRegistered => formatter
                .write_str("tunnel is already registered or has reached its control session limit"),
            Self::ServerShutdown => formatter.write_str("tunnel server is shut down"),
            Self::Connection(error) => error.fmt(formatter),
        }
    }
}

impl Error for RegisterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Connection(error) => Some(error),
            Self::AlreadyRegistered | Self::ServerShutdown => None,
        }
    }
}

impl From<ConnectionError> for RegisterError {
    fn from(error: ConnectionError) -> Self {
        Self::Connection(error)
    }
}

async fn open(opener: &mpsc::Sender<OpenRequest>, tag: &str) -> Result<Stream, ConnectionError> {
    Stream::validate_tag(tag)?;
    let (response, receiver) = oneshot::channel();
    opener
        .send(OpenRequest {
            tag: tag.to_owned(),
            response,
        })
        .await
        .map_err(|_| muxado::Error::SessionClosed)?;
    receiver
        .await
        .map_err(|_| ConnectionError::from(muxado::Error::SessionClosed))?
}

#[cfg(test)]
mod tests {
    use tokio::io::duplex;

    use super::{MAX_SESSIONS_PER_TUNNEL, RegisterError, TunnelServer};
    use crate::TunnelId;

    #[tokio::test]
    async fn pools_sessions_only_for_the_same_owner() {
        let server = TunnelServer::default();
        let tunnel_id = TunnelId::new("pooled").unwrap();
        let mut peers = Vec::new();

        for _ in 0..MAX_SESSIONS_PER_TUNNEL {
            let (server_side, peer) = duplex(1024);
            peers.push(peer);
            server
                .register_with_owner(tunnel_id.clone(), "owner-a", server_side)
                .await
                .unwrap();
        }

        let (server_side, peer) = duplex(1024);
        peers.push(peer);
        assert!(matches!(
            server
                .register_with_owner(tunnel_id.clone(), "owner-a", server_side)
                .await,
            Err(RegisterError::AlreadyRegistered)
        ));

        let other_id = TunnelId::new("owned").unwrap();
        let (server_side, peer) = duplex(1024);
        peers.push(peer);
        server
            .register_with_owner(other_id.clone(), "owner-a", server_side)
            .await
            .unwrap();
        let (server_side, peer) = duplex(1024);
        peers.push(peer);
        assert!(matches!(
            server
                .register_with_owner(other_id, "owner-b", server_side)
                .await,
            Err(RegisterError::AlreadyRegistered)
        ));
    }

    #[tokio::test]
    async fn legacy_registration_remains_single_session() {
        let server = TunnelServer::default();
        let tunnel_id = TunnelId::new("legacy").unwrap();
        let (first, _first_peer) = duplex(1024);
        server.register(tunnel_id.clone(), first).await.unwrap();

        let (second, _second_peer) = duplex(1024);
        assert!(matches!(
            server.register(tunnel_id, second).await,
            Err(RegisterError::AlreadyRegistered)
        ));
    }
}
