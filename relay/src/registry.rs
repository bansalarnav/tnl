//! Registered tunnels, their control sessions, and their idle data transports.

use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use tnl_core::{
    Opener, Session, SessionConfig, SessionError, Stream, TunnelId,
    data_transport::TransportKey,
    protocol::{MAX_CONTROL_SESSIONS, MAX_DATA_TRANSPORTS},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Notify, watch},
    time::{Instant, timeout_at},
};
use tracing::debug;

use crate::ClientId;

pub(crate) trait Io: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T> Io for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

/// A raw connection to a node that carries one visitor at a time, paired with
/// the sideband stream that marks where each visitor's bytes end.
pub(crate) struct DataTransport {
    pub data: Box<dyn Io>,
    pub sideband: Stream,
    generation: u64,
}

/// Credentials a node must prove to attach data transports to a tunnel.
pub(crate) struct TransportGrant {
    pub key: TransportKey,
    pub generation: u64,
}

#[derive(Clone)]
pub(crate) struct Registry {
    config: SessionConfig,
    state: Arc<Mutex<State>>,
    shutdown: watch::Sender<bool>,
}

#[derive(Default)]
struct State {
    shutdown: bool,
    next_session_id: u64,
    tunnels: HashMap<TunnelId, Tunnel>,
}

struct Tunnel {
    owner: ClientId,
    transport_key: TransportKey,
    /// Identifies this registration of the tunnel, so data transports from an
    /// earlier registration are never handed to a later one.
    generation: u64,
    sessions: Vec<(u64, Opener)>,
    next_session: usize,
    transports: VecDeque<DataTransport>,
    transport_available: Arc<Notify>,
    /// Set once the node has supplied a data transport. From then on, visitors
    /// briefly wait for one instead of falling back to a session stream.
    prefers_transports: bool,
}

impl Registry {
    pub fn new(config: SessionConfig) -> Self {
        Self {
            config,
            state: Arc::default(),
            shutdown: watch::channel(false).0,
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("registry lock was poisoned")
    }

    /// Starts a control session for `tunnel_id`. A tunnel may hold up to
    /// [`MAX_CONTROL_SESSIONS`] sessions, all from the same owner.
    pub async fn register<S>(
        &self,
        tunnel_id: TunnelId,
        owner: ClientId,
        transport_key: TransportKey,
        connection: S,
    ) -> Result<Registration, RegisterError>
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let session = Session::server(connection, self.config).await?;
        let mut state = self.state();
        if state.shutdown {
            return Err(RegisterError::Shutdown);
        }
        let session_id = state.next_session_id;
        state.next_session_id += 1;
        let tunnel = state
            .tunnels
            .entry(tunnel_id.clone())
            .or_insert_with(|| Tunnel {
                owner: owner.clone(),
                transport_key,
                generation: session_id,
                sessions: Vec::new(),
                next_session: 0,
                transports: VecDeque::new(),
                transport_available: Arc::new(Notify::new()),
                prefers_transports: false,
            });
        if tunnel.owner != owner || tunnel.sessions.len() >= MAX_CONTROL_SESSIONS {
            return Err(RegisterError::AlreadyRegistered);
        }
        tunnel.sessions.push((session_id, session.opener()));
        Ok(Registration {
            registry: self.clone(),
            tunnel_id,
            session_id,
            session,
        })
    }

    /// Opens a stream to the node serving `tunnel_id`, rotating between its
    /// control sessions. Returns `None` if the tunnel is not registered.
    pub async fn open(
        &self,
        tunnel_id: &TunnelId,
        tag: &str,
    ) -> Option<Result<Stream, SessionError>> {
        let opener = {
            let mut state = self.state();
            let tunnel = state.tunnels.get_mut(tunnel_id)?;
            let (_, opener) = &tunnel.sessions[tunnel.next_session % tunnel.sessions.len()];
            let opener = opener.clone();
            tunnel.next_session = tunnel.next_session.wrapping_add(1);
            opener
        };
        Some(opener.open(tag).await)
    }

    pub fn transport_grant(&self, tunnel_id: &TunnelId) -> Option<TransportGrant> {
        self.state()
            .tunnels
            .get(tunnel_id)
            .map(|tunnel| TransportGrant {
                key: tunnel.transport_key.clone(),
                generation: tunnel.generation,
            })
    }

    /// Adds an authenticated data transport to the tunnel registration that
    /// issued `generation`.
    pub fn add_transport<S>(
        &self,
        tunnel_id: &TunnelId,
        generation: u64,
        data: S,
        sideband: Stream,
    ) -> Result<(), AddTransportError>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let mut state = self.state();
        if state.shutdown {
            return Err(AddTransportError::Shutdown);
        }
        let tunnel = state
            .tunnels
            .get_mut(tunnel_id)
            .filter(|tunnel| tunnel.generation == generation)
            .ok_or(AddTransportError::NotRegistered)?;
        if tunnel.transports.len() >= MAX_DATA_TRANSPORTS {
            return Err(AddTransportError::PoolFull);
        }
        tunnel.prefers_transports = true;
        tunnel.transports.push_back(DataTransport {
            data: Box::new(data),
            sideband,
            generation,
        });
        tunnel.transport_available.notify_waiters();
        Ok(())
    }

    /// Returns whether visitors should wait for a data transport.
    pub fn prefers_transports(&self, tunnel_id: &TunnelId) -> bool {
        self.state()
            .tunnels
            .get(tunnel_id)
            .is_some_and(|tunnel| tunnel.prefers_transports)
    }

    fn take_transport(&self, tunnel_id: &TunnelId) -> Option<DataTransport> {
        self.state()
            .tunnels
            .get_mut(tunnel_id)
            .and_then(|tunnel| tunnel.transports.pop_front())
    }

    /// Takes the oldest idle data transport, waiting up to `wait` for one.
    pub async fn take_transport_within(
        &self,
        tunnel_id: &TunnelId,
        wait: Duration,
    ) -> Option<DataTransport> {
        let available = self
            .state()
            .tunnels
            .get(tunnel_id)
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

    /// Returns a data transport whose visitor finished cleanly to its pool.
    pub fn recycle_transport(&self, tunnel_id: &TunnelId, transport: DataTransport) {
        let mut state = self.state();
        let Some(tunnel) = state.tunnels.get_mut(tunnel_id) else {
            return;
        };
        if transport.generation != tunnel.generation
            || tunnel.transports.len() >= MAX_DATA_TRANSPORTS
        {
            return;
        }
        tunnel.transports.push_back(transport);
        tunnel.transport_available.notify_waiters();
    }

    /// Closes every session and refuses new ones.
    pub fn shutdown(&self) {
        let mut state = self.state();
        state.shutdown = true;
        state.tunnels.clear();
        drop(state);
        self.shutdown.send_replace(true);
    }

    pub fn is_shutdown(&self) -> bool {
        self.state().shutdown
    }

    /// Completes when shutdown begins.
    pub async fn wait_for_shutdown(&self) {
        let _ = self
            .shutdown
            .subscribe()
            .wait_for(|shutdown| *shutdown)
            .await;
    }

    fn unregister(&self, tunnel_id: &TunnelId, session_id: u64) {
        let mut state = self.state();
        let Some(tunnel) = state.tunnels.get_mut(tunnel_id) else {
            return;
        };
        tunnel.sessions.retain(|(id, _)| *id != session_id);
        if tunnel.sessions.is_empty() {
            state.tunnels.remove(tunnel_id);
        }
    }
}

/// A registered control session. Dropping it unregisters the session.
pub(crate) struct Registration {
    registry: Registry,
    tunnel_id: TunnelId,
    session_id: u64,
    session: Session,
}

impl Registration {
    /// Serves the session until it closes or the registry shuts down.
    pub async fn run(mut self) {
        let shutdown = self.registry.clone();
        tokio::select! {
            () = async {
                loop {
                    match self.session.accept().await {
                        // Nodes have nothing to send the relay unprompted.
                        Ok(stream) => debug!(tag = stream.tag(), "ignoring stream opened by node"),
                        Err(SessionError::Multiplexer(_)) => break,
                        Err(error) => debug!("node opened an invalid stream: {error}"),
                    }
                }
            } => {}
            () = shutdown.wait_for_shutdown() => {}
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.registry.unregister(&self.tunnel_id, self.session_id);
    }
}

#[derive(Debug)]
pub(crate) enum RegisterError {
    AlreadyRegistered,
    Shutdown,
    Session(SessionError),
}

impl fmt::Display for RegisterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyRegistered => formatter
                .write_str("tunnel is owned by another client or has reached its session limit"),
            Self::Shutdown => formatter.write_str("relay is shutting down"),
            Self::Session(error) => error.fmt(formatter),
        }
    }
}

impl Error for RegisterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Session(error) => Some(error),
            Self::AlreadyRegistered | Self::Shutdown => None,
        }
    }
}

impl From<SessionError> for RegisterError {
    fn from(error: SessionError) -> Self {
        Self::Session(error)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AddTransportError {
    NotRegistered,
    PoolFull,
    Shutdown,
}

impl fmt::Display for AddTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotRegistered => "tunnel registration ended",
            Self::PoolFull => "tunnel data transport pool is full",
            Self::Shutdown => "relay is shutting down",
        })
    }
}

impl Error for AddTransportError {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tnl_core::{
        Session, SessionConfig, Stream, TunnelId,
        data_transport::TransportKey,
        protocol::{MAX_CONTROL_SESSIONS, MAX_DATA_TRANSPORTS},
    };
    use tokio::io::{DuplexStream, duplex};

    use super::{AddTransportError, RegisterError, Registration, Registry};
    use crate::ClientId;

    fn registry() -> Registry {
        Registry::new(SessionConfig::default().without_heartbeat())
    }

    async fn register(
        registry: &Registry,
        tunnel_id: &TunnelId,
        owner: &str,
    ) -> Result<(Registration, DuplexStream), RegisterError> {
        let (relay_side, node_side) = duplex(64 * 1024);
        let registration = registry
            .register(
                tunnel_id.clone(),
                ClientId::new(owner),
                TransportKey::from_token(owner),
                relay_side,
            )
            .await?;
        Ok((registration, node_side))
    }

    /// Returns a sideband stream from a throwaway session pair.
    async fn sideband() -> (Stream, Session, Session) {
        let (left, right) = duplex(64 * 1024);
        let config = SessionConfig::default().without_heartbeat();
        let (client, server) = tokio::join!(
            Session::client(left, config),
            Session::server(right, config)
        );
        let (client, server) = (client.unwrap(), server.unwrap());
        (client.open("sideband").await.unwrap(), client, server)
    }

    fn add(
        registry: &Registry,
        tunnel_id: &TunnelId,
        generation: u64,
        sideband: Stream,
    ) -> Result<(), AddTransportError> {
        let (data, _) = duplex(1024);
        registry.add_transport(tunnel_id, generation, data, sideband)
    }

    #[tokio::test]
    async fn pools_sessions_only_for_the_same_owner() {
        let registry = registry();
        let tunnel_id = TunnelId::new("pooled").unwrap();
        let mut registrations = Vec::new();
        for _ in 0..MAX_CONTROL_SESSIONS {
            registrations.push(register(&registry, &tunnel_id, "owner-a").await.unwrap());
        }
        assert!(matches!(
            register(&registry, &tunnel_id, "owner-a").await,
            Err(RegisterError::AlreadyRegistered)
        ));

        let other_id = TunnelId::new("owned").unwrap();
        let _owned = register(&registry, &other_id, "owner-a").await.unwrap();
        assert!(matches!(
            register(&registry, &other_id, "owner-b").await,
            Err(RegisterError::AlreadyRegistered)
        ));
    }

    #[tokio::test]
    async fn bounds_the_data_transport_pool() {
        let registry = registry();
        let tunnel_id = TunnelId::new("transport-pool").unwrap();
        let _registration = register(&registry, &tunnel_id, "owner-a").await.unwrap();
        assert!(!registry.prefers_transports(&tunnel_id));
        let generation = registry.transport_grant(&tunnel_id).unwrap().generation;

        let mut sessions = Vec::new();
        for _ in 0..MAX_DATA_TRANSPORTS {
            let (stream, client, server) = sideband().await;
            sessions.push((client, server));
            add(&registry, &tunnel_id, generation, stream).unwrap();
        }
        let (stream, _client, _server) = sideband().await;
        assert_eq!(
            add(&registry, &tunnel_id, generation, stream),
            Err(AddTransportError::PoolFull)
        );

        let transport = registry.take_transport(&tunnel_id).unwrap();
        registry.recycle_transport(&tunnel_id, transport);
        assert!(registry.take_transport(&tunnel_id).is_some());
        assert!(registry.prefers_transports(&tunnel_id));
    }

    #[tokio::test]
    async fn waits_for_a_replenished_data_transport() {
        let registry = registry();
        let tunnel_id = TunnelId::new("transport-wait").unwrap();
        let _registration = register(&registry, &tunnel_id, "owner-a").await.unwrap();
        let generation = registry.transport_grant(&tunnel_id).unwrap().generation;

        let waiter = tokio::spawn({
            let registry = registry.clone();
            let tunnel_id = tunnel_id.clone();
            async move {
                registry
                    .take_transport_within(&tunnel_id, Duration::from_secs(1))
                    .await
                    .is_some()
            }
        });
        tokio::task::yield_now().await;
        let (stream, _client, _server) = sideband().await;
        add(&registry, &tunnel_id, generation, stream).unwrap();
        assert!(waiter.await.unwrap());
    }

    #[tokio::test]
    async fn keeps_data_transports_within_one_registration() {
        let registry = registry();
        let tunnel_id = TunnelId::new("transport-generation").unwrap();
        let registration = register(&registry, &tunnel_id, "owner-a").await.unwrap();
        let generation = registry.transport_grant(&tunnel_id).unwrap().generation;
        let (stream, _client, _server) = sideband().await;
        add(&registry, &tunnel_id, generation, stream).unwrap();
        let transport = registry.take_transport(&tunnel_id).unwrap();

        drop(registration);
        let _replacement = register(&registry, &tunnel_id, "owner-b").await.unwrap();
        registry.recycle_transport(&tunnel_id, transport);
        assert!(registry.take_transport(&tunnel_id).is_none());

        let (stream, _client, _server) = sideband().await;
        assert_eq!(
            add(&registry, &tunnel_id, generation, stream),
            Err(AddTransportError::NotRegistered)
        );
    }
}
