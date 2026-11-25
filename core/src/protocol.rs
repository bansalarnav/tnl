//! Constants and helpers for the relay HTTP API and control-session streams.

use std::num::ParseIntError;

/// Protocol version both peers must send in [`PROTOCOL_VERSION_HEADER`].
pub const PROTOCOL_VERSION: &str = "6";

/// Header carrying the protocol version in requests and responses.
pub const PROTOCOL_VERSION_HEADER: &str = "X-Tnl-Protocol-Version";

/// Response header with the public URL of a registered tunnel.
pub const TUNNEL_URL_HEADER: &str = "X-Tnl-Url";

/// Response header with the number of control sessions a node should open.
pub const CONTROL_SESSIONS_HEADER: &str = "X-Tnl-Control-Sessions";

/// Response header with the number of idle data transports a node should keep.
pub const TRANSPORT_POOL_HEADER: &str = "X-Tnl-Transport-Pool";

/// Most control sessions one tunnel may hold.
pub const MAX_CONTROL_SESSIONS: usize = 8;

/// Most idle data transports a relay keeps for one tunnel.
pub const MAX_DATA_TRANSPORTS: usize = 64;

/// Tag of a stream carrying one visitor connection.
pub const FORWARD_TAG: &str = "tnl/tcp";

const SIDEBAND_TAG_PREFIX: &str = "tnl/sideband/";

/// Bytes a relay writes to an idle data transport before the first visitor
/// byte, telling the node a visitor has been assigned.
pub const ACTIVATION_MARKER: &[u8; 4] = b"TNL\x06";

/// Path of the `CONNECT` request that registers a control session.
pub fn tunnel_path(tunnel_id: &crate::TunnelId) -> String {
    format!("/v1/tunnels/{tunnel_id}")
}

/// Parses the tunnel name from a [`tunnel_path`].
pub fn parse_tunnel_path(path: &str) -> Option<&str> {
    path.strip_prefix("/v1/tunnels/")
        .filter(|name| !name.is_empty() && !name.contains('/'))
}

/// Tag of the stream that carries boundaries for data transport `transport_id`.
pub fn sideband_tag(transport_id: u64) -> String {
    format!("{SIDEBAND_TAG_PREFIX}{transport_id:016x}")
}

/// Returns the data transport identifier if `tag` is a [`sideband_tag`].
pub fn parse_sideband_tag(tag: &str) -> Option<Result<u64, ParseIntError>> {
    tag.strip_prefix(SIDEBAND_TAG_PREFIX)
        .map(|id| u64::from_str_radix(id, 16))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_tags_and_paths() {
        assert_eq!(parse_sideband_tag(&sideband_tag(42)), Some(Ok(42)));
        assert_eq!(parse_sideband_tag(FORWARD_TAG), None);
        assert!(parse_sideband_tag("tnl/sideband/zz").unwrap().is_err());

        let id = crate::TunnelId::new("app").unwrap();
        assert_eq!(parse_tunnel_path(&tunnel_path(&id)), Some("app"));
        assert_eq!(parse_tunnel_path("/v1/tunnels/"), None);
        assert_eq!(parse_tunnel_path("/v1/tunnels/a/b"), None);
    }
}
