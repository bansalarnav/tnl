use std::time::Duration;

use anyhow::{Context, Result};
use tnl::{TRANSPORT_ACTIVATION_MARKER, TunnelId, server::TunnelServer};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, copy_bidirectional_with_sizes};
use tokio::time::{Instant, timeout};

use super::tls::TlsConnection;

const OPEN_STREAM_TIMEOUT: Duration = Duration::from_secs(10);
const TRANSPORT_REPLENISH_WAIT: Duration = Duration::from_millis(250);
const TCP_FORWARD_TAG: &str = "tnl/tcp";
const FORWARD_BUFFER_SIZE: usize = 64 * 1024;

pub async fn forward(
    tunnel_server: &TunnelServer,
    tunnel_id: &TunnelId,
    connection: TlsConnection,
) -> Result<()> {
    let (client_hello, mut visitor_stream) = connection.into_raw_parts();

    let dedicated_transport = if tunnel_server.transport_pool_preferred(tunnel_id) {
        tunnel_server
            .take_transport_wait(tunnel_id, TRANSPORT_REPLENISH_WAIT)
            .await
    } else {
        None
    };
    if let Some(mut data_stream) = dedicated_transport {
        data_stream
            .write_all(TRANSPORT_ACTIVATION_MARKER)
            .await
            .context("could not activate dedicated tunnel transport")?;
        let started = Instant::now();
        return match forward_stream(&mut visitor_stream, &mut data_stream, &client_hello).await {
            Ok((visitor_to_node, node_to_visitor)) => {
                tunnel_server.report_transport_outcome(
                    tunnel_id,
                    started.elapsed(),
                    visitor_to_node + node_to_visitor + client_hello.len() as u64,
                );
                Ok(())
            }
            Err(error) => Err(error),
        };
    }

    let Some(mut data_stream) = timeout(
        OPEN_STREAM_TIMEOUT,
        tunnel_server.open(tunnel_id, TCP_FORWARD_TAG),
    )
    .await
    .context("timed out opening a stream to the node")??
    else {
        return Ok(());
    };

    forward_stream(&mut visitor_stream, &mut data_stream, &client_hello)
        .await
        .map(|_| ())
}

async fn forward_stream<S>(
    visitor_stream: &mut tokio::net::TcpStream,
    data_stream: &mut S,
    client_hello: &[u8],
) -> Result<(u64, u64)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    data_stream
        .write_all(client_hello)
        .await
        .context("could not forward the visitor TLS ClientHello")?;
    let transferred = copy_bidirectional_with_sizes(
        visitor_stream,
        data_stream,
        FORWARD_BUFFER_SIZE,
        FORWARD_BUFFER_SIZE,
    )
    .await
    .context("tunnel forwarding failed")?;

    Ok(transferred)
}
