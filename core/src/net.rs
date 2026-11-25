//! Socket settings and error classification shared by relays and nodes.

use std::{error::Error, io, time::Duration};

use socket2::{SockRef, TcpKeepalive};
use tokio::net::TcpStream;

const TCP_KEEPALIVE_IDLE: Duration = Duration::from_secs(30);
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const TCP_KEEPALIVE_RETRIES: u32 = 3;

/// Disables Nagle's algorithm and enables TCP keepalive.
pub fn configure_tcp_stream(stream: &TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    SockRef::from(stream).set_tcp_keepalive(
        &TcpKeepalive::new()
            .with_time(TCP_KEEPALIVE_IDLE)
            .with_interval(TCP_KEEPALIVE_INTERVAL)
            .with_retries(TCP_KEEPALIVE_RETRIES),
    )
}

/// Returns whether `error` or any of its sources is an ordinary disconnect or
/// a peer speaking garbage TLS, which is not worth logging.
pub fn is_routine_disconnect(error: &(dyn Error + 'static)) -> bool {
    let mut cause = Some(error);
    while let Some(error) = cause {
        if is_routine_cause(error) {
            return true;
        }
        cause = error.source();
    }
    false
}

fn is_routine_cause(cause: &(dyn Error + 'static)) -> bool {
    if let Some(error) = cause.downcast_ref::<io::Error>() {
        return matches!(
            error.kind(),
            io::ErrorKind::UnexpectedEof
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::BrokenPipe
        );
    }
    cause.downcast_ref::<rustls::Error>().is_some_and(|error| {
        matches!(
            error,
            rustls::Error::InappropriateMessage { .. }
                | rustls::Error::InappropriateHandshakeMessage { .. }
                | rustls::Error::InvalidMessage(_)
                | rustls::Error::PeerIncompatible(_)
                | rustls::Error::PeerMisbehaved(_)
                | rustls::Error::AlertReceived(_)
                | rustls::Error::NoApplicationProtocol
        )
    })
}
