//! An embeddable node that serves tnl tunnels through a relay.
//!
//! A node registers a tunnel with its relay and receives the tunnel's visitor
//! connections, still TLS-encrypted, through a [`Handler`]:
//!
//! ```no_run
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use tnl_node::{ForwardTls, NodeConfig, Tunnel, TunnelCertificate, tnl_core::TunnelId};
//!
//! let config = NodeConfig::new("https://relay.example.com".parse()?, "token")?;
//! let tunnel = Tunnel::register(config, TunnelId::new("my-app")?).await?;
//! let certificate = TunnelCertificate::start(tunnel.hostname(), "/var/cache/tnl".into())?;
//! let error = tunnel
//!     .serve(ForwardTls::new(certificate, "127.0.0.1:3000".parse()?))
//!     .await;
//! # Err(error.into())
//! # }
//! ```

mod certificate;
mod client;
mod forward;
mod tunnel;

use std::{
    error::Error as StdError,
    fmt, io,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub use certificate::TunnelCertificate;
pub use client::NodeConfig;
pub use forward::ForwardTls;
pub use tnl_core;
pub use tunnel::Tunnel;

/// An error from a node.
pub struct Error(anyhow::Error);

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An error returned by a [`Handler`].
pub type BoxError = Box<dyn StdError + Send + Sync>;

impl Error {
    /// Returns the HTTP status if the relay rejected a request.
    pub fn rejection_status(&self) -> Option<u16> {
        self.0
            .downcast_ref::<client::Rejection>()
            .map(|rejection| rejection.status)
    }
}

impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        Self(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, formatter)
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0.source()
    }
}

/// Serves visitor connections arriving through a [`Tunnel`].
pub trait Handler: Send + Sync + 'static {
    /// Serves one visitor. The visitor's bytes start with its TLS ClientHello,
    /// which the relay did not decrypt.
    ///
    /// Returning `Ok` lets the tunnel reuse the underlying data transport, so
    /// only do so once the visitor is done.
    fn handle(&self, visitor: Visitor<'_>) -> impl Future<Output = Result<(), BoxError>> + Send;
}

trait VisitorIo: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T> VisitorIo for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

/// A visitor's byte stream, from its TLS ClientHello onwards.
pub struct Visitor<'a> {
    io: &'a mut dyn VisitorIo,
}

impl<'a> Visitor<'a> {
    fn new(io: &'a mut (impl AsyncRead + AsyncWrite + Send + Unpin)) -> Self {
        Self { io }
    }
}

impl AsyncRead for Visitor<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.io).poll_read(context, buffer)
    }
}

impl AsyncWrite for Visitor<'_> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.io).poll_write(context, buffer)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.io).poll_write_vectored(context, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.io).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.io).poll_shutdown(context)
    }
}
