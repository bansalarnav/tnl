use std::{
    fmt, io,
    io::IoSlice,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

trait TransportIo: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T> TransportIo for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

/// A dedicated raw data connection and boundary sideband supplied by a tunnel node.
///
/// Unlike [`crate::Stream`], this connection does not pass application data
/// through the multiplexed control session.
pub struct Transport {
    inner: Box<dyn TransportIo>,
    sideband: Box<dyn TransportIo>,
    generation: u64,
}

impl Transport {
    pub(crate) fn new<T, C>(inner: T, sideband: C, generation: u64) -> Self
    where
        T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
        C: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        Self {
            inner: Box::new(inner),
            sideband: Box::new(sideband),
            generation,
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// Borrows the raw data path and its persistent boundary channel.
    pub fn split(&mut self) -> (TransportHalf<'_>, TransportHalf<'_>) {
        (
            TransportHalf {
                inner: &mut *self.inner,
            },
            TransportHalf {
                inner: &mut *self.sideband,
            },
        )
    }
}

/// One borrowed I/O channel from a dedicated transport.
pub struct TransportHalf<'a> {
    inner: &'a mut dyn TransportIo,
}

impl AsyncRead for TransportHalf<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for TransportHalf<'_> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.inner).poll_write(context, buffer)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.inner).poll_write_vectored(context, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_shutdown(context)
    }
}

impl fmt::Debug for Transport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Transport").finish_non_exhaustive()
    }
}

impl AsyncRead for Transport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for Transport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.inner).poll_write(context, buffer)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.inner).poll_write_vectored(context, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.inner).poll_shutdown(context)
    }
}
