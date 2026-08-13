use anyhow::{Context, Result};
use tnl::{TunnelId, server::Broker};
use tokio::io::{AsyncWriteExt, copy_bidirectional};

use super::{DataStream, tls::TlsConnection};

pub async fn forward(
    broker: &Broker<DataStream>,
    tunnel_id: &TunnelId,
    connection: TlsConnection,
) -> Result<()> {
    let Some(mut data_stream) = broker.connect(tunnel_id).await? else {
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
