//! Accepts dedicated data transports from nodes.

use anyhow::{Context, Result};
use rand::RngCore;
use tnl_core::{
    data_transport::{self, CHALLENGE_LENGTH},
    protocol::sideband_tag,
};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::Relay;

pub(crate) async fn accept<S>(relay: &Relay, mut stream: S, first_byte: u8) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let registry = &relay.inner.registry;
    let tunnel_id = data_transport::read_request(&mut stream, first_byte)
        .await
        .context("invalid data transport request")?;
    let grant = registry
        .transport_grant(&tunnel_id)
        .with_context(|| format!("tunnel {tunnel_id} is not registered"))?;

    let mut challenge = [0; CHALLENGE_LENGTH];
    rand::thread_rng().fill_bytes(&mut challenge);
    data_transport::authenticate(&mut stream, &tunnel_id, &grant.key, challenge).await?;

    let transport_id = relay.next_transport_id();
    data_transport::assign(&mut stream, transport_id).await?;
    let sideband = registry
        .open(&tunnel_id, &sideband_tag(transport_id))
        .await
        .context("tunnel closed while adding a data transport")??;
    registry.add_transport(&tunnel_id, grant.generation, stream, sideband)?;
    Ok(())
}
