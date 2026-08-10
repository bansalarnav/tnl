use anyhow::{Result, bail};

use super::tls::TlsConnection;

pub async fn forward(tunnel_id: &str, connection: TlsConnection) -> Result<()> {
    let (_client_hello, _stream) = connection.into_raw_parts();
    bail!("no tunnel client is connected for {tunnel_id}")
}
