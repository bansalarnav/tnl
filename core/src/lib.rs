//! Programmatic client and server APIs for tnl tunnels.

mod protocol;

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "server")]
pub mod server;

pub use protocol::TunnelId;
