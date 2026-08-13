use std::{
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use muxado::{
    SessionBuilder,
    heartbeat::{Heartbeat, HeartbeatConfig as MuxadoHeartbeatConfig, HeartbeatCtl},
    typed::{StreamType, Typed, TypedAccept, TypedOpenClose, TypedSession, TypedStream},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Mutex as AsyncMutex, oneshot},
};

use crate::SessionError;

const APPLICATION_STREAM_TYPE: StreamType = StreamType::clamp(0);
const DEFAULT_MAX_CONCURRENT_STREAMS: usize = 512;

/// Periodic liveness checks for a multiplexed session.
#[derive(Clone, Copy, Debug)]
pub struct HeartbeatConfig {
    pub interval: Duration,
    pub timeout: Duration,
}

impl HeartbeatConfig {
    pub fn new(interval: Duration, timeout: Duration) -> Self {
        Self { interval, timeout }
    }
}

/// Limits and liveness settings applied to a multiplexed session.
#[derive(Clone, Debug)]
pub struct SessionConfig {
    heartbeat: Option<HeartbeatConfig>,
    max_concurrent_streams: usize,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            heartbeat: None,
            max_concurrent_streams: DEFAULT_MAX_CONCURRENT_STREAMS,
        }
    }
}

impl SessionConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables heartbeat checks. A missed heartbeat closes the session.
    pub fn heartbeat(mut self, interval: Duration, timeout: Duration) -> Self {
        self.heartbeat = Some(HeartbeatConfig::new(interval, timeout));
        self
    }

    pub fn heartbeat_config(&self) -> Option<HeartbeatConfig> {
        self.heartbeat
    }

    /// Sets the maximum number of simultaneously open streams on one connection.
    pub fn max_concurrent_streams(mut self, maximum: usize) -> Self {
        self.max_concurrent_streams = maximum;
        self
    }

    pub fn max_concurrent_streams_value(&self) -> usize {
        self.max_concurrent_streams
    }
}

#[async_trait]
pub(crate) trait OpenStreams: Send {
    async fn open(&mut self) -> Result<TypedStream, muxado::Error>;
    async fn close(&mut self, error: muxado::Error, message: String);
}

#[async_trait]
impl<T> OpenStreams for T
where
    T: TypedOpenClose + Send,
{
    async fn open(&mut self) -> Result<TypedStream, muxado::Error> {
        self.open_typed(APPLICATION_STREAM_TYPE).await
    }

    async fn close(&mut self, error: muxado::Error, message: String) {
        let _ = TypedOpenClose::close(self, error, message).await;
    }
}

#[async_trait]
pub(crate) trait AcceptStreams: Send {
    async fn accept(&mut self) -> Result<TypedStream, muxado::Error>;
}

#[async_trait]
impl<T> AcceptStreams for T
where
    T: TypedAccept + Send,
{
    async fn accept(&mut self) -> Result<TypedStream, muxado::Error> {
        self.accept_typed().await
    }
}

pub(crate) struct SessionParts {
    pub opener: Arc<AsyncMutex<Box<dyn OpenStreams>>>,
    pub accepter: Box<dyn AcceptStreams>,
    pub _heartbeat: Option<HeartbeatCtl>,
}

impl SessionParts {
    pub(crate) async fn start<S>(
        stream: S,
        client: bool,
        config: &SessionConfig,
    ) -> Result<Self, SessionError>
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        if let Some(heartbeat) = config.heartbeat
            && (heartbeat.interval.is_zero() || heartbeat.timeout <= heartbeat.interval)
        {
            return Err(SessionError::InvalidHeartbeatConfig);
        }
        if config.max_concurrent_streams == 0 {
            return Err(SessionError::InvalidStreamLimit);
        }
        let builder = SessionBuilder::new(stream).stream_limit(config.max_concurrent_streams);
        let session = if client {
            builder.client().start()
        } else {
            builder.server().start()
        };
        let typed = Typed::new(session);

        if let Some(heartbeat) = config.heartbeat {
            let (timeout_sender, timeout_receiver) = oneshot::channel();
            let timeout_sender = Arc::new(Mutex::new(Some(timeout_sender)));
            let handler = move |latency: Option<Duration>| {
                let timeout_sender = Arc::clone(&timeout_sender);
                async move {
                    if latency.is_none()
                        && let Some(sender) = timeout_sender
                            .lock()
                            .expect("heartbeat lock was poisoned")
                            .take()
                    {
                        let _ = sender.send(());
                    }
                    Ok::<_, Box<dyn Error>>(())
                }
            };
            let muxado_config = MuxadoHeartbeatConfig {
                interval: heartbeat.interval,
                tolerance: heartbeat.timeout.saturating_sub(heartbeat.interval),
                handler: Some(Arc::new(handler)),
            };
            let (heartbeat, controller) = Heartbeat::start(typed, muxado_config).await?;
            let (opener, accepter) = heartbeat.split_typed();
            let opener: Arc<AsyncMutex<Box<dyn OpenStreams>>> =
                Arc::new(AsyncMutex::new(Box::new(opener)));
            let closer = Arc::clone(&opener);
            tokio::spawn(async move {
                if timeout_receiver.await.is_ok() {
                    closer
                        .lock()
                        .await
                        .close(muxado::Error::PeerEOF, "heartbeat timed out".to_owned())
                        .await;
                }
            });
            Ok(Self {
                opener,
                accepter: Box::new(accepter),
                _heartbeat: Some(controller),
            })
        } else {
            let (opener, accepter) = typed.split_typed();
            Ok(Self {
                opener: Arc::new(AsyncMutex::new(Box::new(opener))),
                accepter: Box::new(accepter),
                _heartbeat: None,
            })
        }
    }
}
