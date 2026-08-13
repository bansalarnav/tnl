use std::{
    error::Error,
    fmt, io,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

const HEADER_MAGIC: &[u8; 4] = b"TNL\x01";
/// Maximum number of UTF-8 bytes allowed in a stream tag.
pub const MAX_TAG_LENGTH: usize = u16::MAX as usize;

/// A tagged bidirectional stream within a session.
pub struct Stream {
    tag: String,
    inner: muxado::Stream,
}

impl Stream {
    /// Returns the application-defined string identifying this stream.
    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub(crate) fn validate_tag(tag: &str) -> Result<(), SessionError> {
        if tag.len() > MAX_TAG_LENGTH {
            return Err(SessionError::TagTooLong(tag.len()));
        }
        Ok(())
    }

    pub(crate) async fn outgoing(
        mut inner: muxado::Stream,
        tag: String,
    ) -> Result<Self, SessionError> {
        Self::validate_tag(&tag)?;
        inner.write_all(HEADER_MAGIC).await?;
        inner.write_all(&(tag.len() as u16).to_be_bytes()).await?;
        inner.write_all(tag.as_bytes()).await?;
        Ok(Self { tag, inner })
    }

    pub(crate) async fn incoming(mut inner: muxado::Stream) -> Result<Self, SessionError> {
        let mut magic = [0; HEADER_MAGIC.len()];
        inner.read_exact(&mut magic).await?;
        if &magic != HEADER_MAGIC {
            return Err(SessionError::InvalidStreamHeader);
        }

        let mut length = [0; 2];
        inner.read_exact(&mut length).await?;
        let mut bytes = vec![0; u16::from_be_bytes(length) as usize];
        inner.read_exact(&mut bytes).await?;
        let tag = String::from_utf8(bytes).map_err(SessionError::InvalidTagEncoding)?;
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
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(context, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

/// An error opening or accepting a session stream.
#[derive(Debug)]
pub enum SessionError {
    Multiplexer(muxado::Error),
    Io(io::Error),
    InvalidStreamHeader,
    InvalidTagEncoding(std::string::FromUtf8Error),
    TagTooLong(usize),
}

impl fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Multiplexer(error) => error.fmt(formatter),
            Self::Io(error) => error.fmt(formatter),
            Self::InvalidStreamHeader => formatter.write_str("invalid stream header"),
            Self::InvalidTagEncoding(_) => formatter.write_str("stream tag is not valid UTF-8"),
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
            Self::InvalidStreamHeader | Self::TagTooLong(_) => None,
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
