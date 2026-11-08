//! Forwards visitor connections, still TLS-encrypted, to the tunnel's node.

use std::time::Duration;

use anyhow::{Context, Result};
use tnl_core::{
    SidebandStream, TunnelId,
    protocol::{ACTIVATION_MARKER, FORWARD_TAG},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, copy_bidirectional_with_sizes},
    time::timeout,
};

use crate::registry::Registry;

const OPEN_STREAM_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a visitor waits for a busy tunnel to free a data transport before
/// falling back to a session stream.
const TRANSPORT_WAIT: Duration = Duration::from_millis(250);
const BUFFER_SIZE: usize = 64 * 1024;

/// Forwards `visitor` to the node serving `tunnel_id`. `client_hello` holds
/// the bytes already read from `visitor`.
pub(crate) async fn forward<S>(
    registry: &Registry,
    tunnel_id: &TunnelId,
    client_hello: &[u8],
    visitor: &mut S,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let transport = if registry.prefers_transports(tunnel_id) {
        registry
            .take_transport_within(tunnel_id, TRANSPORT_WAIT)
            .await
    } else {
        None
    };

    if let Some(mut transport) = transport {
        transport
            .data
            .write_all(ACTIVATION_MARKER)
            .await
            .context("could not activate the data transport")?;
        let mut node = SidebandStream::new(&mut transport.data, &mut transport.sideband);
        copy(visitor, &mut node, client_hello).await?;
        node.shutdown().await?;
        node.finish().await?;
        registry.recycle_transport(tunnel_id, transport);
        return Ok(());
    }

    let Some(stream) = timeout(OPEN_STREAM_TIMEOUT, registry.open(tunnel_id, FORWARD_TAG))
        .await
        .context("timed out opening a stream to the node")?
    else {
        return Ok(());
    };
    copy(visitor, &mut stream?, client_hello).await
}

async fn copy<V, N>(visitor: &mut V, node: &mut N, client_hello: &[u8]) -> Result<()>
where
    V: AsyncRead + AsyncWrite + Unpin,
    N: AsyncRead + AsyncWrite + Unpin,
{
    node.write_all(client_hello)
        .await
        .context("could not forward the visitor's TLS ClientHello")?;
    copy_bidirectional_with_sizes(visitor, node, BUFFER_SIZE, BUFFER_SIZE)
        .await
        .context("tunnel forwarding failed")?;
    Ok(())
}
