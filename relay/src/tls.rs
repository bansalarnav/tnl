//! Reads a TLS ClientHello without terminating TLS, so connections can be
//! routed by server name and either terminated or passed through untouched.

use std::{io::Cursor, sync::Arc};

use anyhow::{Context as _, Result, bail};
use rustls::{
    ServerConfig,
    server::{Accepted, Acceptor},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio_rustls::{StartHandshake, server::TlsStream};

/// First byte of a TLS handshake record.
pub(crate) const HANDSHAKE_RECORD: u8 = 22;
const MAX_CLIENT_HELLO_LENGTH: usize = 64 * 1024;

pub(crate) struct ClientHello<S> {
    accepted: Accepted,
    server_name: Option<String>,
    /// Every byte read from the connection so far.
    bytes: Vec<u8>,
    stream: S,
}

impl<S> ClientHello<S> {
    /// Returns the lowercase SNI hostname without a trailing dot.
    pub fn server_name(&self) -> Option<&str> {
        self.server_name.as_deref()
    }

    pub fn is_acme_challenge(&self) -> bool {
        self.accepted
            .client_hello()
            .alpn()
            .into_iter()
            .flatten()
            .eq([b"acme-tls/1".as_slice()])
    }

    pub async fn terminate(self, config: Arc<ServerConfig>) -> Result<TlsStream<S>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        StartHandshake::from_parts(self.accepted, self.stream)
            .into_stream(config)
            .await
            .context("could not complete the TLS handshake")
    }

    /// Returns the bytes read so far and the rest of the connection.
    pub fn into_parts(self) -> (Vec<u8>, S) {
        (self.bytes, self.stream)
    }
}

pub(crate) async fn read_client_hello<S>(mut stream: S, first_byte: u8) -> Result<ClientHello<S>>
where
    S: AsyncRead + Unpin,
{
    let mut acceptor = Acceptor::default();
    let mut bytes = vec![first_byte];
    feed(&mut acceptor, &bytes)?;
    loop {
        let accepted = acceptor
            .accept()
            .map_err(|(error, _)| error)
            .context("could not parse the TLS ClientHello")?;
        if let Some(accepted) = accepted {
            let server_name = accepted
                .client_hello()
                .server_name()
                .map(|name| name.trim_end_matches('.').to_ascii_lowercase());
            return Ok(ClientHello {
                accepted,
                server_name,
                bytes,
                stream,
            });
        }

        let mut buffer = [0; 4096];
        let read = stream
            .read(&mut buffer)
            .await
            .context("could not read the TLS ClientHello")?;
        if read == 0 {
            bail!("connection closed before sending a complete TLS ClientHello");
        }
        if bytes.len() + read > MAX_CLIENT_HELLO_LENGTH {
            bail!("TLS ClientHello is too large");
        }
        bytes.extend_from_slice(&buffer[..read]);
        feed(&mut acceptor, &buffer[..read])?;
    }
}

fn feed(acceptor: &mut Acceptor, bytes: &[u8]) -> Result<()> {
    let mut cursor = Cursor::new(bytes);
    while cursor.position() < bytes.len() as u64 {
        if acceptor.read_tls(&mut cursor)? == 0 {
            bail!("rustls did not consume the ClientHello bytes");
        }
    }
    Ok(())
}
