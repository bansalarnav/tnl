//! QUIC/UDP multiplexed data-plane transport configuration, authentication, and
//! stream adapter shared by relays and nodes.

use std::{
    io,
    net::{SocketAddr, UdpSocket},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use hmac::Mac;
use quinn::{
    MtuDiscoveryConfig, RecvStream, SendStream, TransportConfig, VarInt,
    congestion::{BbrConfig, CubicConfig},
};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{TunnelId, data_transport::TransportKey};

/// ALPN protocol identifier for tnl QUIC data connections.
pub const QUIC_ALPN: &[u8] = b"tnl-quic/6";

/// Default number of parallel QUIC data connections opened per tunnel.
/// Overridable via `TNL_QUIC_CONNS` (`0` disables QUIC and uses raw TCP).
pub const DEFAULT_QUIC_CONNECTIONS: usize = 4;

/// Maximum number of QUIC data connections one tunnel may hold.
pub const MAX_QUIC_CONNECTIONS: usize = 16;

const EXPORTER_LABEL: &[u8] = b"tnl-quic-auth-v6";
const UDP_SOCKET_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const ACCEPTED: u8 = 0;
const REJECTED: u8 = 1;

/// Returns the configured QUIC connection count (`TNL_QUIC_CONNS` or default 4).
pub fn configured_connections() -> usize {
    std::env::var("TNL_QUIC_CONNS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|n| n.min(MAX_QUIC_CONNECTIONS))
        .unwrap_or(DEFAULT_QUIC_CONNECTIONS)
}

/// Builds a high-throughput QUIC transport configuration with large flow-control
/// windows and BBR (or Cubic via `TNL_QUIC_CC=cubic`) congestion control.
pub fn transport_config() -> Arc<TransportConfig> {
    let mut cfg = TransportConfig::default();
    cfg.max_concurrent_bidi_streams(VarInt::from_u32(512));
    cfg.max_concurrent_uni_streams(VarInt::from_u32(0));
    // Bounded windows (4 MiB/stream, 16 MiB/conn, 8 MiB send) keep peak RSS
    // comparable to the raw-TCP pool while still filling high-BDP WAN links.
    cfg.stream_receive_window(VarInt::from_u32(4 * 1024 * 1024));
    cfg.receive_window(VarInt::from_u32(16 * 1024 * 1024));
    cfg.send_window(8 * 1024 * 1024);
    cfg.keep_alive_interval(Some(Duration::from_secs(15)));
    cfg.max_idle_timeout(Some(
        Duration::from_secs(60)
            .try_into()
            .expect("60s fits in IdleTimeout"),
    ));
    // Keep RFC 9000 initial MTU (1200B) so 1280-MTU tunnels (e.g. Tailscale/IPv6)
    // work immediately while PMTUD discovers larger MTUs automatically.
    cfg.mtu_discovery_config(Some(MtuDiscoveryConfig::default()));

    let use_cubic = std::env::var("TNL_QUIC_CC")
        .ok()
        .is_some_and(|v| v.eq_ignore_ascii_case("cubic"));
    if use_cubic {
        cfg.congestion_controller_factory(Arc::new(CubicConfig::default()));
    } else {
        cfg.congestion_controller_factory(Arc::new(BbrConfig::default()));
    }
    Arc::new(cfg)
}

/// Binds a non-blocking UDP socket with large kernel send/receive buffers to
/// prevent burst packet drops at high concurrency.
pub fn bind_udp_socket(addr: SocketAddr) -> io::Result<UdpSocket> {
    let domain = if addr.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    if addr.is_ipv6() {
        let _ = socket.set_only_v6(false);
    }
    let _ = socket.set_recv_buffer_size(UDP_SOCKET_BUFFER_BYTES);
    let _ = socket.set_send_buffer_size(UDP_SOCKET_BUFFER_BYTES);
    socket.bind(&addr.into())?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

/// Node side: authenticates a newly established QUIC connection for `tunnel_id`
/// using TLS 1.3 exported keying material (1-message, replay-proof).
pub async fn authenticate_client(
    conn: &quinn::Connection,
    tunnel_id: &TunnelId,
    key: &TransportKey,
) -> io::Result<()> {
    let mut exporter = [0_u8; 32];
    conn.export_keying_material(&mut exporter, EXPORTER_LABEL, b"")
        .map_err(|_| io::Error::other("could not export QUIC TLS keying material"))?;
    let name = tunnel_id.as_str().as_bytes();
    let name_len = u8::try_from(name.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "tunnel name is too long"))?;
    let proof = key.quic_proof(tunnel_id, &exporter).finalize().into_bytes();

    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| io::Error::other(format!("could not open QUIC auth stream: {e}")))?;

    let mut msg = [0_u8; 1 + 255 + 32];
    msg[0] = name_len;
    msg[1..1 + name.len()].copy_from_slice(name);
    let end = 1 + name.len() + 32;
    msg[1 + name.len()..end].copy_from_slice(&proof);
    send.write_all(&msg[..end])
        .await
        .map_err(|e| io::Error::other(format!("could not write QUIC auth proof: {e}")))?;
    send.finish()
        .map_err(|e| io::Error::other(format!("could not finish QUIC auth stream: {e}")))?;

    let mut status = [0_u8; 1];
    recv.read_exact(&mut status)
        .await
        .map_err(|e| io::Error::other(format!("could not read QUIC auth response: {e}")))?;
    if status[0] != ACCEPTED {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "relay rejected the QUIC data connection",
        ));
    }
    Ok(())
}

/// Relay side: authenticates an incoming QUIC connection and returns the
/// `TunnelId` it proved ownership of.
pub async fn authenticate_server<F>(
    conn: &quinn::Connection,
    lookup_key: F,
) -> io::Result<TunnelId>
where
    F: FnOnce(&TunnelId) -> Option<TransportKey>,
{
    let mut exporter = [0_u8; 32];
    conn.export_keying_material(&mut exporter, EXPORTER_LABEL, b"")
        .map_err(|_| io::Error::other("could not export QUIC TLS keying material"))?;

    let (mut send, mut recv) = conn
        .accept_bi()
        .await
        .map_err(|e| io::Error::other(format!("could not accept QUIC auth stream: {e}")))?;

    let mut len_buf = [0_u8; 1];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "missing QUIC tunnel name length"))?;
    let name_len = len_buf[0] as usize;
    if name_len == 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "empty tunnel name"));
    }
    let mut payload = vec![0_u8; name_len + 32];
    recv.read_exact(&mut payload)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "truncated QUIC auth payload"))?;

    let name = std::str::from_utf8(&payload[..name_len])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "tunnel name is not UTF-8"))?;
    let tunnel_id = TunnelId::new(name)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid tunnel name"))?;
    let proof = &payload[name_len..];

    let authorized = lookup_key(&tunnel_id)
        .is_some_and(|key| key.quic_proof(&tunnel_id, &exporter).verify_slice(proof).is_ok());

    if !authorized {
        let _ = send.write_all(&[REJECTED]).await;
        let _ = send.finish();
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "invalid QUIC data connection proof",
        ));
    }

    send.write_all(&[ACCEPTED])
        .await
        .map_err(|e| io::Error::other(format!("could not write QUIC auth acceptance: {e}")))?;
    send.finish()
        .map_err(|e| io::Error::other(format!("could not finish QUIC auth acceptance: {e}")))?;
    Ok(tunnel_id)
}

/// Combines a `quinn::SendStream` and `quinn::RecvStream` into a single
/// `AsyncRead + AsyncWrite` bidirectional stream.
pub struct QuicStream {
    send: SendStream,
    recv: RecvStream,
}

impl QuicStream {
    pub fn new(send: SendStream, recv: RecvStream) -> Self {
        Self { send, recv }
    }
}

impl AsyncRead for QuicStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        <RecvStream as AsyncRead>::poll_read(Pin::new(&mut self.recv), cx, buf)
    }
}

impl AsyncWrite for QuicStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        <SendStream as AsyncWrite>::poll_write(Pin::new(&mut self.send), cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        <SendStream as AsyncWrite>::poll_write_vectored(Pin::new(&mut self.send), cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        <SendStream as AsyncWrite>::is_write_vectored(&self.send)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        <SendStream as AsyncWrite>::poll_flush(Pin::new(&mut self.send), cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        <SendStream as AsyncWrite>::poll_shutdown(Pin::new(&mut self.send), cx)
    }
}
