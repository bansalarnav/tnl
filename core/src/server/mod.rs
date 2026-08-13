use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot, watch},
};

use crate::{ConnectionError, SessionConfig, Stream, TunnelId, session::SessionParts};

struct OpenRequest {
    tag: String,
    response: oneshot::Sender<Result<Stream, ConnectionError>>,
}

/// Matches registered nodes with multiplexed sessions.
pub struct Broker {
    config: SessionConfig,
    state: Arc<Mutex<State>>,
    shutdown: watch::Sender<bool>,
}

#[derive(Default)]
struct State {
    shutdown: bool,
    next_session_id: u64,
    sessions: HashMap<String, RegisteredSession>,
}

struct RegisteredSession {
    session_id: u64,
    opener: mpsc::Sender<OpenRequest>,
}

impl Broker {
    pub fn new() -> Self {
        Self::with_config(SessionConfig::default())
    }

    pub fn with_config(config: SessionConfig) -> Self {
        let (shutdown, _) = watch::channel(false);
        Self {
            config,
            state: Arc::new(Mutex::new(State::default())),
            shutdown,
        }
    }

    /// Registers a node after the application has authenticated it.
    pub fn register(&self, tunnel_id: TunnelId) -> Option<ServerSession> {
        let mut state = self.state.lock().expect("broker lock was poisoned");
        if state.shutdown || state.sessions.contains_key(tunnel_id.as_str()) {
            return None;
        }

        let session_id = state.next_session_id;
        state.next_session_id = state.next_session_id.wrapping_add(1);
        let (opener, open_requests) = mpsc::channel(32);
        let (inbound_streams, inbound_receiver) = mpsc::unbounded_channel();
        state.sessions.insert(
            tunnel_id.to_string(),
            RegisteredSession {
                session_id,
                opener: opener.clone(),
            },
        );

        Some(ServerSession {
            inner: Arc::new(ServerSessionInner {
                broker: self.clone(),
                tunnel_id,
                session_id,
                opener,
                inbound_streams: Arc::new(tokio::sync::Mutex::new(inbound_receiver)),
                open_requests: Mutex::new(Some(open_requests)),
                inbound_sender: Mutex::new(Some(inbound_streams)),
            }),
        })
    }

    /// Opens a tagged bidirectional stream to a registered node.
    pub async fn connect(
        &self,
        tunnel_id: &TunnelId,
        tag: impl AsRef<str>,
    ) -> Result<Option<Stream>, ConnectionError> {
        let opener = {
            let state = self.state.lock().expect("broker lock was poisoned");
            if state.shutdown {
                return Err(muxado::Error::SessionClosed.into());
            }
            let Some(session) = state.sessions.get(tunnel_id.as_str()) else {
                return Ok(None);
            };
            session.opener.clone()
        };

        open(&opener, tag.as_ref()).await.map(Some)
    }

    /// Closes all registered sessions.
    pub fn shutdown(&self) {
        let mut state = self.state.lock().expect("broker lock was poisoned");
        state.shutdown = true;
        state.sessions.clear();
        drop(state);
        self.shutdown.send_replace(true);
    }

    pub fn is_shutdown(&self) -> bool {
        self.state
            .lock()
            .expect("broker lock was poisoned")
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
}

impl Clone for Broker {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            state: Arc::clone(&self.state),
            shutdown: self.shutdown.clone(),
        }
    }
}

impl Default for Broker {
    fn default() -> Self {
        Self::new()
    }
}

/// The central-server side of a registered multiplexed tunnel session.
#[derive(Clone)]
pub struct ServerSession {
    inner: Arc<ServerSessionInner>,
}

struct ServerSessionInner {
    broker: Broker,
    tunnel_id: TunnelId,
    session_id: u64,
    opener: mpsc::Sender<OpenRequest>,
    inbound_streams:
        Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<Result<Stream, ConnectionError>>>>,
    open_requests: Mutex<Option<mpsc::Receiver<OpenRequest>>>,
    inbound_sender: Mutex<Option<mpsc::UnboundedSender<Result<Stream, ConnectionError>>>>,
}

struct UnregisterOnDrop(Arc<ServerSessionInner>);

impl ServerSession {
    /// Runs the session over an already-established transport stream.
    ///
    /// This may be called only once for a registered session.
    pub async fn serve<S>(self, stream: S) -> Result<(), ConnectionError>
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let mut open_requests = self
            .inner
            .open_requests
            .lock()
            .expect("session lock was poisoned")
            .take()
            .ok_or(muxado::Error::SessionClosed)?;
        let inbound_streams = self
            .inner
            .inbound_sender
            .lock()
            .expect("session lock was poisoned")
            .take()
            .ok_or(muxado::Error::SessionClosed)?;
        let _unregister = UnregisterOnDrop(Arc::clone(&self.inner));
        let mut parts = SessionParts::start(stream, false, &self.inner.broker.config).await?;
        let mut shutdown = self.inner.broker.shutdown.subscribe();

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
                    let inbound_streams = inbound_streams.clone();
                    tokio::spawn(async move {
                        let stream = Stream::incoming(stream).await;
                        let _ = inbound_streams.send(stream);
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

    /// Opens a new tagged bidirectional stream to the node.
    pub async fn open(&self, tag: impl AsRef<str>) -> Result<Stream, ConnectionError> {
        open(&self.inner.opener, tag.as_ref()).await
    }

    /// Waits for the node to open the next tagged bidirectional stream.
    pub async fn accept(&self) -> Result<Stream, ConnectionError> {
        self.inner
            .inbound_streams
            .lock()
            .await
            .recv()
            .await
            .unwrap_or_else(|| Err(muxado::Error::SessionClosed.into()))
    }
}

impl Drop for ServerSessionInner {
    fn drop(&mut self) {
        self.unregister();
    }
}

impl ServerSessionInner {
    fn unregister(&self) {
        let mut state = self.broker.state.lock().expect("broker lock was poisoned");
        if state
            .sessions
            .get(self.tunnel_id.as_str())
            .is_some_and(|session| session.session_id == self.session_id)
        {
            state.sessions.remove(self.tunnel_id.as_str());
        }
    }
}

impl Drop for UnregisterOnDrop {
    fn drop(&mut self) {
        self.0.unregister();
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
