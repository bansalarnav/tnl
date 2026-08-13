//! Programmatic client and server APIs for tnl tunnels.

mod protocol;

#[cfg(any(feature = "client", feature = "server"))]
mod stream;

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "server")]
pub mod server;

pub use protocol::TunnelId;

#[cfg(any(feature = "client", feature = "server"))]
pub use stream::{MAX_TAG_LENGTH, SessionError, Stream};

#[cfg(any(feature = "client", feature = "server"))]
pub type ConnectionError = SessionError;
