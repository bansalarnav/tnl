use std::time::Duration;

use anyhow::{Context, Result, bail};
use tnl::{
    TRANSPORT_ACTIVATION_MARKER, TunnelId, protocol::ReusableTransportStream, server::TunnelServer,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, copy_bidirectional_with_sizes};
use tokio::net::TcpStream;
use tokio::time::timeout;

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
        {
            let mut visitor_transport = ReusableTransportStream::new(&mut data_stream);
            forward_stream(&mut visitor_stream, &mut visitor_transport, &client_hello).await?;
            visitor_transport.shutdown().await?;
            if !visitor_transport.is_finished() {
                bail!("reusable dedicated transport did not finish cleanly");
            }
        }
        tunnel_server.recycle_transport(tunnel_id, data_stream);
        return Ok(());
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

    forward_stream(&mut visitor_stream, &mut data_stream, &client_hello).await
}

async fn forward_stream<S>(
    visitor_stream: &mut TcpStream,
    data_stream: &mut S,
    client_hello: &[u8],
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    data_stream
        .write_all(client_hello)
        .await
        .context("could not forward the visitor TLS ClientHello")?;
    copy_bidirectional_with_sizes(
        visitor_stream,
        data_stream,
        FORWARD_BUFFER_SIZE,
        FORWARD_BUFFER_SIZE,
    )
    .await
    .context("tunnel forwarding failed")?;
    Ok(())
}
