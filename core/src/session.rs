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
const MAX_CONCURRENT_STREAMS: usize = 512;
const STREAM_WINDOW_SIZE: usize = 4 * 1024 * 1024;

/// Periodic liveness checks for a multiplexed session.
#[derive(Clone, Copy, Debug)]
struct HeartbeatConfig {
    interval: Duration,
    timeout: Duration,
}

/// Liveness settings applied to a multiplexed session.
#[derive(Clone, Debug, Default)]
pub struct SessionConfig {
    heartbeat: Option<HeartbeatConfig>,
}

impl SessionConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables heartbeat checks. A missed heartbeat closes the session.
    pub fn heartbeat(mut self, interval: Duration, timeout: Duration) -> Self {
        self.heartbeat = Some(HeartbeatConfig { interval, timeout });
        self
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
        let builder = SessionBuilder::new(stream)
            .window_size(STREAM_WINDOW_SIZE)
            .stream_limit(MAX_CONCURRENT_STREAMS);
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
