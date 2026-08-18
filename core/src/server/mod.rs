use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Notify, mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};

use crate::{ConnectionError, SessionConfig, Stream, Transport, TunnelId, session::SessionParts};

/// Maximum number of transport sessions pooled for one logical tunnel.
pub const MAX_SESSIONS_PER_TUNNEL: usize = 8;
/// Maximum number of idle dedicated data transports retained for one tunnel.
pub const MAX_TRANSPORTS_PER_TUNNEL: usize = 64;
/// Number of idle dedicated transports a node should normally keep warm.
pub const RECOMMENDED_IDLE_TRANSPORTS_PER_TUNNEL: usize = 32;
const SHORT_TRANSPORT_MAX_DURATION: Duration = Duration::from_millis(100);
const SHORT_TRANSPORT_MAX_BYTES: u64 = 64 * 1024;
const SHORT_TRANSPORT_STREAK_LIMIT: u8 = 4;
const SHORT_TRANSPORT_BACKOFF: Duration = Duration::from_secs(2);

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
    tunnels: HashMap<String, RegisteredTunnel>,
}

struct RegisteredTunnel {
    owner: String,
    sessions: Vec<RegisteredSession>,
    transports: VecDeque<Transport>,
    transport_available: Arc<Notify>,
    transport_pool_active: bool,
    short_transport_streak: u8,
    transport_backoff_until: Option<Instant>,
    next_session: usize,
}

impl RegisteredTunnel {
    fn new(owner: String) -> Self {
        Self {
            owner,
            sessions: Vec::new(),
            transports: VecDeque::new(),
            transport_available: Arc::new(Notify::new()),
            transport_pool_active: false,
            short_transport_streak: 0,
            transport_backoff_until: None,
            next_session: 0,
        }
    }
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

    /// Registers a session owned by an authenticated node and starts serving
    /// its connection. A tunnel may pool multiple sessions only when all of
    /// them have the same owner.
    pub async fn register<S>(
        &self,
        tunnel_id: TunnelId,
        owner: impl Into<String>,
        connection: S,
    ) -> Result<(), RegisterError>
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let owner = owner.into();
        let (session_id, open_requests) = {
            let mut state = self.state.lock().expect("server lock was poisoned");
            if state.shutdown {
                return Err(RegisterError::ServerShutdown);
            }
            if let Some(tunnel) = state.tunnels.get(tunnel_id.as_str())
                && (tunnel.owner != owner || tunnel.sessions.len() >= MAX_SESSIONS_PER_TUNNEL)
            {
                return Err(RegisterError::AlreadyRegistered);
            }

            let session_id = state.next_session_id;
            state.next_session_id = state.next_session_id.wrapping_add(1);
            let (opener, open_requests) = mpsc::channel(32);
            state
                .tunnels
                .entry(tunnel_id.to_string())
                .or_insert_with(|| RegisteredTunnel::new(owner))
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
            let Some(tunnel) = state.tunnels.get_mut(tunnel_id.as_str()) else {
                return Ok(None);
            };
            let session_index = tunnel.next_session % tunnel.sessions.len();
            tunnel.next_session = tunnel.next_session.wrapping_add(1);
            let session = &tunnel.sessions[session_index];
            session.opener.clone()
        };

        open(&opener, tag.as_ref()).await.map(Some)
    }

    /// Adds an authenticated, dedicated data transport to a registered tunnel.
    ///
    /// Dedicated transports are consumed once and avoid sending application
    /// bytes through the multiplexed control session.
    pub fn register_transport<S>(
        &self,
        tunnel_id: &TunnelId,
        owner: impl AsRef<str>,
        connection: S,
    ) -> Result<(), RegisterTransportError>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let mut state = self.state.lock().expect("server lock was poisoned");
        if state.shutdown {
            return Err(RegisterTransportError::ServerShutdown);
        }
        let Some(tunnel) = state.tunnels.get_mut(tunnel_id.as_str()) else {
            return Err(RegisterTransportError::TunnelNotRegistered);
        };
        if tunnel.owner != owner.as_ref() {
            return Err(RegisterTransportError::OwnerMismatch);
        }
        if tunnel.transports.len() >= MAX_TRANSPORTS_PER_TUNNEL {
            return Err(RegisterTransportError::PoolFull);
        }
        tunnel.transport_pool_active = true;
        tunnel.transports.push_back(Transport::new(connection));
        tunnel.transport_available.notify_waiters();
        Ok(())
    }

    /// Takes the oldest idle dedicated data transport for a tunnel.
    pub fn take_transport(&self, tunnel_id: &TunnelId) -> Option<Transport> {
        self.state
            .lock()
            .expect("server lock was poisoned")
            .tunnels
            .get_mut(tunnel_id.as_str())
            .and_then(|tunnel| tunnel.transports.pop_front())
    }

    /// Returns whether dedicated transport should currently be preferred.
    pub fn transport_pool_preferred(&self, tunnel_id: &TunnelId) -> bool {
        let mut state = self.state.lock().expect("server lock was poisoned");
        let Some(tunnel) = state.tunnels.get_mut(tunnel_id.as_str()) else {
            return false;
        };
        if tunnel
            .transport_backoff_until
            .is_some_and(|deadline| deadline > Instant::now())
        {
            return false;
        }
        tunnel.transport_backoff_until = None;
        tunnel.transport_pool_active
    }

    /// Reports how a consumed dedicated transport behaved so short-connection
    /// workloads can fall back to the reusable multiplexed data plane.
    pub fn report_transport_outcome(&self, tunnel_id: &TunnelId, duration: Duration, bytes: u64) {
        let mut state = self.state.lock().expect("server lock was poisoned");
        let Some(tunnel) = state.tunnels.get_mut(tunnel_id.as_str()) else {
            return;
        };
        if duration <= SHORT_TRANSPORT_MAX_DURATION && bytes <= SHORT_TRANSPORT_MAX_BYTES {
            tunnel.short_transport_streak = tunnel.short_transport_streak.saturating_add(1);
            if tunnel.short_transport_streak >= SHORT_TRANSPORT_STREAK_LIMIT {
                tunnel.transport_backoff_until = Some(Instant::now() + SHORT_TRANSPORT_BACKOFF);
                tunnel.short_transport_streak = 0;
            }
        } else {
            tunnel.short_transport_streak = 0;
            tunnel.transport_backoff_until = None;
        }
    }

    /// Waits briefly for a dedicated transport to become available.
    pub async fn take_transport_wait(
        &self,
        tunnel_id: &TunnelId,
        wait: Duration,
    ) -> Option<Transport> {
        let available = self
            .state
            .lock()
            .expect("server lock was poisoned")
            .tunnels
            .get(tunnel_id.as_str())
            .map(|tunnel| Arc::clone(&tunnel.transport_available))?;
        let deadline = Instant::now() + wait;

        loop {
            let notified = available.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(transport) = self.take_transport(tunnel_id) {
                return Some(transport);
            }
            if timeout_at(deadline, notified).await.is_err() {
                return None;
            }
        }
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
        state.tunnels.clear();
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
        let remove_tunnel = if let Some(tunnel) = state.tunnels.get_mut(tunnel_id.as_str()) {
            tunnel
                .sessions
                .retain(|session| session.session_id != session_id);
            tunnel.sessions.is_empty()
        } else {
            false
        };
        if remove_tunnel {
            state.tunnels.remove(tunnel_id.as_str());
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

/// An error registering a dedicated data transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegisterTransportError {
    TunnelNotRegistered,
    OwnerMismatch,
    PoolFull,
    ServerShutdown,
}

impl fmt::Display for RegisterTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TunnelNotRegistered => "tunnel is not registered",
            Self::OwnerMismatch => "transport owner does not own the tunnel",
            Self::PoolFull => "tunnel transport pool is full",
            Self::ServerShutdown => "tunnel server is shut down",
        })
    }
}

impl Error for RegisterTransportError {}

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
    use std::time::Duration;

    use tokio::io::duplex;

    use super::{
        MAX_SESSIONS_PER_TUNNEL, MAX_TRANSPORTS_PER_TUNNEL, RegisterError, RegisterTransportError,
        SHORT_TRANSPORT_STREAK_LIMIT, TunnelServer,
    };
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
                .register(tunnel_id.clone(), "owner-a", server_side)
                .await
                .unwrap();
        }

        let (server_side, peer) = duplex(1024);
        peers.push(peer);
        assert!(matches!(
            server
                .register(tunnel_id.clone(), "owner-a", server_side)
                .await,
            Err(RegisterError::AlreadyRegistered)
        ));

        let other_id = TunnelId::new("owned").unwrap();
        let (server_side, peer) = duplex(1024);
        peers.push(peer);
        server
            .register(other_id.clone(), "owner-a", server_side)
            .await
            .unwrap();
        let (server_side, peer) = duplex(1024);
        peers.push(peer);
        assert!(matches!(
            server.register(other_id, "owner-b", server_side).await,
            Err(RegisterError::AlreadyRegistered)
        ));
    }

    #[tokio::test]
    async fn dedicated_transports_require_the_tunnel_owner_and_are_bounded() {
        let server = TunnelServer::default();
        let tunnel_id = TunnelId::new("transport-pool").unwrap();
        let (control, _control_peer) = duplex(1024);
        server
            .register(tunnel_id.clone(), "owner-a", control)
            .await
            .unwrap();
        assert!(!server.transport_pool_preferred(&tunnel_id));

        let (wrong_owner, _wrong_owner_peer) = duplex(1024);
        assert_eq!(
            server.register_transport(&tunnel_id, "owner-b", wrong_owner),
            Err(RegisterTransportError::OwnerMismatch)
        );

        let mut transport_peers = Vec::new();
        for _ in 0..MAX_TRANSPORTS_PER_TUNNEL {
            let (transport, peer) = duplex(1024);
            transport_peers.push(peer);
            server
                .register_transport(&tunnel_id, "owner-a", transport)
                .unwrap();
        }

        let (overflow, _overflow_peer) = duplex(1024);
        assert_eq!(
            server.register_transport(&tunnel_id, "owner-a", overflow),
            Err(RegisterTransportError::PoolFull)
        );

        assert!(server.take_transport(&tunnel_id).is_some());

        assert!(server.transport_pool_preferred(&tunnel_id));
        for _ in 0..SHORT_TRANSPORT_STREAK_LIMIT {
            server.report_transport_outcome(&tunnel_id, Duration::from_millis(5), 1024);
        }
        assert!(!server.transport_pool_preferred(&tunnel_id));
        server.report_transport_outcome(&tunnel_id, Duration::from_secs(1), 1024 * 1024);
        assert!(server.transport_pool_preferred(&tunnel_id));
    }

    #[tokio::test]
    async fn waits_for_a_replenished_dedicated_transport() {
        let server = TunnelServer::default();
        let tunnel_id = TunnelId::new("transport-wait").unwrap();
        let (control, _control_peer) = duplex(1024);
        server
            .register(tunnel_id.clone(), "owner-a", control)
            .await
            .unwrap();

        let waiting_server = server.clone();
        let waiting_id = tunnel_id.clone();
        let waiter = tokio::spawn(async move {
            waiting_server
                .take_transport_wait(&waiting_id, Duration::from_secs(1))
                .await
        });
        tokio::task::yield_now().await;

        let (transport, _transport_peer) = duplex(1024);
        server
            .register_transport(&tunnel_id, "owner-a", transport)
            .unwrap();
        assert!(waiter.await.unwrap().is_some());
    }
}
