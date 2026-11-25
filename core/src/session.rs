use std::{
    error::Error,
    fmt, io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use muxado::{
    MuxadoAccept, MuxadoOpen, SessionBuilder,
    heartbeat::{Heartbeat, HeartbeatConfig, HeartbeatCtl},
    typed::{StreamType, Typed, TypedAccept, TypedOpenClose, TypedSession, TypedStream},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

const APPLICATION_STREAM_TYPE: StreamType = StreamType::clamp(0);
const MAX_CONCURRENT_STREAMS: usize = 512;
const STREAM_WINDOW_SIZE: usize = 4 * 1024 * 1024;
const STREAM_HEADER_MAGIC: &[u8; 4] = b"TNL\x01";

/// Maximum number of UTF-8 bytes allowed in a stream tag.
pub const MAX_TAG_LENGTH: usize = u16::MAX as usize;

/// Settings for a multiplexed [`Session`].
#[derive(Clone, Copy, Debug)]
pub struct SessionConfig {
    heartbeat: Option<(Duration, Duration)>,
}

impl Default for SessionConfig {
    /// Sends a heartbeat every 20 seconds and closes the session after 60
    /// seconds without a response.
    fn default() -> Self {
        Self {
            heartbeat: Some((Duration::from_secs(20), Duration::from_secs(60))),
        }
    }
}

impl SessionConfig {
    /// Sends a heartbeat every `interval` and closes the session when none has
    /// been answered for `timeout`.
    pub fn heartbeat(mut self, interval: Duration, timeout: Duration) -> Self {
        self.heartbeat = Some((interval, timeout));
        self
    }

    pub fn without_heartbeat(mut self) -> Self {
        self.heartbeat = None;
        self
    }
}

/// A multiplexed connection carrying tagged bidirectional [`Stream`]s.
///
/// Either peer may open streams. Dropping the session closes it once every
/// [`Opener`] and stream is gone.
pub struct Session {
    opener: Opener,
    accepter: Accepter,
    _heartbeat: Option<HeartbeatCtl>,
}

impl Session {
    /// Starts the side of a session that dialed the connection.
    pub async fn client<S>(connection: S, config: SessionConfig) -> Result<Self, SessionError>
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        Self::start(SessionBuilder::new(connection).client(), config).await
    }

    /// Starts the side of a session that accepted the connection.
    pub async fn server<S>(connection: S, config: SessionConfig) -> Result<Self, SessionError>
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        Self::start(SessionBuilder::new(connection).server(), config).await
    }

    async fn start<S>(
        builder: SessionBuilder<S>,
        config: SessionConfig,
    ) -> Result<Self, SessionError>
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (open, accept) = Typed::new(
            builder
                .window_size(STREAM_WINDOW_SIZE)
                .stream_limit(MAX_CONCURRENT_STREAMS)
                .start(),
        )
        .split_typed();
        let opener = Opener(open.clone());
        let Some((interval, timeout)) = config.heartbeat else {
            return Ok(Self {
                opener,
                accepter: Accepter::Plain(accept),
                _heartbeat: None,
            });
        };
        if interval.is_zero() || timeout <= interval {
            return Err(SessionError::InvalidHeartbeatConfig);
        }

        let closer = opener.clone();
        let handler = move |latency: Option<Duration>| {
            let closer = closer.clone();
            async move {
                if latency.is_none() {
                    closer.close("heartbeat timed out").await;
                }
                Ok::<_, Box<dyn Error>>(())
            }
        };
        let (heartbeat, controller) = Heartbeat::start(
            Halves { open, accept },
            HeartbeatConfig {
                interval,
                tolerance: timeout - interval,
                handler: Some(Arc::new(handler)),
            },
        )
        .await?;
        // Heartbeats only intercept streams of their own type, so opening
        // through the plain opener is equivalent and lets it be cloned.
        let (_, accept) = heartbeat.split_typed();
        Ok(Self {
            opener,
            accepter: Accepter::Heartbeat(accept),
            _heartbeat: Some(controller),
        })
    }

    /// Returns a handle that opens streams on this session.
    pub fn opener(&self) -> Opener {
        self.opener.clone()
    }

    /// Opens a stream to the peer.
    pub async fn open(&self, tag: &str) -> Result<Stream, SessionError> {
        self.opener.open(tag).await
    }

    /// Waits for the peer to open a stream.
    ///
    /// A [`SessionError::Multiplexer`] means the session has closed. Other
    /// errors concern only the stream that failed to arrive.
    pub async fn accept(&mut self) -> Result<Stream, SessionError> {
        let stream = match &mut self.accepter {
            Accepter::Plain(accepter) => accepter.accept_typed().await,
            Accepter::Heartbeat(accepter) => accepter.accept_typed().await,
        }?;
        Stream::incoming(stream).await
    }
}

/// A cloneable handle that opens streams on a [`Session`].
#[derive(Clone)]
pub struct Opener(Typed<MuxadoOpen>);

impl Opener {
    /// Opens a stream to the peer, identified by `tag`.
    pub async fn open(&self, tag: &str) -> Result<Stream, SessionError> {
        Stream::validate_tag(tag)?;
        let stream = self.0.clone().open_typed(APPLICATION_STREAM_TYPE).await?;
        Stream::outgoing(stream, tag.to_owned()).await
    }

    /// Closes the session.
    pub async fn close(&self, reason: &str) {
        let _ = TypedOpenClose::close(
            &mut self.0.clone(),
            muxado::Error::PeerEOF,
            reason.to_owned(),
        )
        .await;
    }
}

enum Accepter {
    Plain(Typed<MuxadoAccept>),
    Heartbeat(Heartbeat<Typed<MuxadoAccept>>),
}

/// Reassembles split session halves so the heartbeat wrapper can take them.
struct Halves {
    open: Typed<MuxadoOpen>,
    accept: Typed<MuxadoAccept>,
}

#[async_trait]
impl TypedAccept for Halves {
    async fn accept_typed(&mut self) -> Result<TypedStream, muxado::Error> {
        self.accept.accept_typed().await
    }
}

#[async_trait]
impl TypedOpenClose for Halves {
    async fn open_typed(&mut self, typ: StreamType) -> Result<TypedStream, muxado::Error> {
        self.open.open_typed(typ).await
    }

    async fn close(&mut self, error: muxado::Error, message: String) -> Result<(), muxado::Error> {
        TypedOpenClose::close(&mut self.open, error, message).await
    }
}

impl TypedSession for Halves {
    type TypedAccept = Typed<MuxadoAccept>;
    type TypedOpen = Typed<MuxadoOpen>;

    fn split_typed(self) -> (Self::TypedOpen, Self::TypedAccept) {
        (self.open, self.accept)
    }
}

/// A tagged bidirectional stream within a [`Session`].
pub struct Stream {
    tag: String,
    inner: TypedStream,
}

impl Stream {
    /// Returns the string the opener identified this stream with.
    pub fn tag(&self) -> &str {
        &self.tag
    }

    fn validate_tag(tag: &str) -> Result<(), SessionError> {
        if tag.len() > MAX_TAG_LENGTH {
            return Err(SessionError::TagTooLong(tag.len()));
        }
        Ok(())
    }

    async fn outgoing(mut inner: TypedStream, tag: String) -> Result<Self, SessionError> {
        let mut header = Vec::with_capacity(STREAM_HEADER_MAGIC.len() + 2 + tag.len());
        header.extend_from_slice(STREAM_HEADER_MAGIC);
        header.extend_from_slice(&(tag.len() as u16).to_be_bytes());
        header.extend_from_slice(tag.as_bytes());
        inner.write_all(&header).await?;
        Ok(Self { tag, inner })
    }

    async fn incoming(mut inner: TypedStream) -> Result<Self, SessionError> {
        let mut magic = [0; STREAM_HEADER_MAGIC.len()];
        inner.read_exact(&mut magic).await?;
        if &magic != STREAM_HEADER_MAGIC {
            return Err(SessionError::InvalidStreamHeader);
        }
        let length = inner.read_u16().await?;
        let mut tag = vec![0; length as usize];
        inner.read_exact(&mut tag).await?;
        let tag = String::from_utf8(tag).map_err(SessionError::InvalidTagEncoding)?;
        Ok(Self { tag, inner })
    }
}

impl fmt::Debug for Stream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Stream")
            .field("tag", &self.tag)
            .finish_non_exhaustive()
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.inner).poll_write(context, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_shutdown(context)
    }
}

/// An error starting a session or opening or accepting one of its streams.
#[derive(Debug)]
pub enum SessionError {
    /// The session failed or closed.
    Multiplexer(muxado::Error),
    Io(io::Error),
    InvalidStreamHeader,
    InvalidTagEncoding(std::string::FromUtf8Error),
    InvalidHeartbeatConfig,
    TagTooLong(usize),
}

impl fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Multiplexer(error) => error.fmt(formatter),
            Self::Io(error) => error.fmt(formatter),
            Self::InvalidStreamHeader => formatter.write_str("invalid stream header"),
            Self::InvalidTagEncoding(_) => formatter.write_str("stream tag is not valid UTF-8"),
            Self::InvalidHeartbeatConfig => formatter
                .write_str("heartbeat interval must be nonzero and shorter than its timeout"),
            Self::TagTooLong(length) => write!(
                formatter,
                "stream tag is {length} bytes; maximum is {MAX_TAG_LENGTH}"
            ),
        }
    }
}

impl Error for SessionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Multiplexer(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::InvalidTagEncoding(error) => Some(error),
            Self::InvalidStreamHeader | Self::InvalidHeartbeatConfig | Self::TagTooLong(_) => None,
        }
    }
}

impl From<muxado::Error> for SessionError {
    fn from(error: muxado::Error) -> Self {
        Self::Multiplexer(error)
    }
}

impl From<io::Error> for SessionError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    use super::{Session, SessionConfig, SessionError};

    #[tokio::test]
    async fn opens_tagged_streams_in_both_directions() {
        let (left, right) = duplex(64 * 1024);
        let config =
            SessionConfig::default().heartbeat(Duration::from_secs(1), Duration::from_secs(3));
        let (client, server) = tokio::join!(
            Session::client(left, config),
            Session::server(right, config)
        );
        let (mut client, mut server) = (client.unwrap(), server.unwrap());

        let mut outgoing = client.open("hello").await.unwrap();
        outgoing.write_all(b"ping").await.unwrap();
        let mut incoming = server.accept().await.unwrap();
        assert_eq!(incoming.tag(), "hello");
        let mut buffer = [0; 4];
        incoming.read_exact(&mut buffer).await.unwrap();
        assert_eq!(&buffer, b"ping");

        let reverse = server.opener().open("reverse").await.unwrap();
        drop(reverse);
        assert_eq!(client.accept().await.unwrap().tag(), "reverse");
    }

    #[tokio::test]
    async fn rejects_invalid_heartbeats() {
        let (left, _right) = duplex(1024);
        let config =
            SessionConfig::default().heartbeat(Duration::from_secs(2), Duration::from_secs(1));
        assert!(matches!(
            Session::client(left, config).await,
            Err(SessionError::InvalidHeartbeatConfig)
        ));
    }
}
