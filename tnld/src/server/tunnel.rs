use std::time::Duration;

use anyhow::{Context, Result};
use tnl::{TunnelId, server::Broker};
use tokio::io::{AsyncWriteExt, copy_bidirectional};
use tokio::time::timeout;

use super::tls::TlsConnection;

const OPEN_STREAM_TIMEOUT: Duration = Duration::from_secs(10);
const TCP_FORWARD_TAG: &str = "tnl/tcp";

pub async fn forward(
    broker: &Broker,
    tunnel_id: &TunnelId,
    connection: TlsConnection,
) -> Result<()> {
    let Some(mut data_stream) = timeout(
        OPEN_STREAM_TIMEOUT,
        broker.connect(tunnel_id, TCP_FORWARD_TAG),
    )
    .await
    .context("timed out opening a stream to the node")??
    else {
        return Ok(());
    };

    let (client_hello, mut visitor_stream) = connection.into_raw_parts();
    data_stream
        .write_all(&client_hello)
        .await
        .context("could not forward the visitor TLS ClientHello")?;
    copy_bidirectional(&mut visitor_stream, &mut data_stream)
        .await
        .context("tunnel forwarding failed")?;
    Ok(())
}
