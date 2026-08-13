use std::sync::Arc;

use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Mutex,
};

use crate::{
    ConnectionError, SessionConfig, Stream,
    session::{AcceptStreams, OpenStreams, SessionParts},
};

/// The node side of a multiplexed tunnel session.
#[derive(Clone)]
pub struct ClientSession {
    inner: Arc<ClientSessionInner>,
}

struct ClientSessionInner {
    opener: Arc<Mutex<Box<dyn OpenStreams>>>,
    accepter: Mutex<Box<dyn AcceptStreams>>,
    _heartbeat: Option<muxado::heartbeat::HeartbeatCtl>,
}

impl ClientSession {
    /// Starts a session with heartbeat checks disabled.
    pub async fn new<S>(stream: S) -> Result<Self, ConnectionError>
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        Self::with_config(stream, SessionConfig::default()).await
    }

    /// Starts a session with the supplied limits and liveness settings.
    pub async fn with_config<S>(stream: S, config: SessionConfig) -> Result<Self, ConnectionError>
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let parts = SessionParts::start(stream, true, &config).await?;
        Ok(Self {
            inner: Arc::new(ClientSessionInner {
                opener: parts.opener,
                accepter: Mutex::new(parts.accepter),
                _heartbeat: parts._heartbeat,
            }),
        })
    }

    /// Opens a new tagged bidirectional stream to the central server.
    pub async fn open(&self, tag: impl AsRef<str>) -> Result<Stream, ConnectionError> {
        let tag = tag.as_ref().to_owned();
        Stream::validate_tag(&tag)?;
        let stream = self.inner.opener.lock().await.open().await?;
        Stream::outgoing(stream, tag).await
    }

    /// Waits for the central server to open the next bidirectional stream.
    pub async fn accept(&self) -> Result<Stream, ConnectionError> {
        let stream = self.inner.accepter.lock().await.accept().await?;
        Stream::incoming(stream).await
    }
}
