//! Programmatic client and server APIs for tnl tunnels.

mod protocol;

#[cfg(any(feature = "client", feature = "server"))]
mod session;

#[cfg(any(feature = "client", feature = "server"))]
mod stream;

#[cfg(feature = "server")]
mod transport;

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "server")]
pub mod server;

pub use protocol::TunnelId;

/// HTTP transport protocol version required by this release.
pub const PROTOCOL_VERSION: &str = "2";

/// HTTP header carrying the tunnel protocol version in both directions.
pub const PROTOCOL_VERSION_HEADER: &str = "X-Tnl-Protocol-Version";

#[cfg(any(feature = "client", feature = "server"))]
pub use stream::{MAX_TAG_LENGTH, SessionError, Stream};

#[cfg(feature = "server")]
pub use transport::Transport;

#[cfg(any(feature = "client", feature = "server"))]
/// Marker sent when an idle dedicated transport is assigned application data.
pub const TRANSPORT_ACTIVATION_MARKER: &[u8; 4] = b"TNL\x02";

#[cfg(any(feature = "client", feature = "server"))]
pub use session::SessionConfig;

#[cfg(any(feature = "client", feature = "server"))]
pub type ConnectionError = SessionError;
