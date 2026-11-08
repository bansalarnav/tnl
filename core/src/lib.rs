//! Wire protocol and multiplexed sessions shared by tnl relays and nodes.
//!
//! A node keeps one or more control [`Session`]s open to a relay. The relay
//! sends each visitor connection to the node in one of two ways:
//!
//! - as a tagged [`Stream`] inside a control session, or
//! - over a dedicated [`data_transport`]: a raw TCP connection paired with a
//!   sideband stream that carries the end-of-stream boundaries, so the
//!   connection can be reused for the next visitor (see [`SidebandStream`]).

pub mod data_transport;
mod id;
pub mod net;
pub mod protocol;
mod session;
mod sideband;

pub use id::{InvalidTunnelId, TunnelId};
pub use session::{MAX_TAG_LENGTH, Opener, Session, SessionConfig, SessionError, Stream};
pub use sideband::SidebandStream;
