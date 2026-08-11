use std::{io::Cursor, sync::Arc};

use anyhow::{Context as _, Result, bail};
use rustls::ServerConfig;
use rustls::server::Acceptor;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::{StartHandshake, server::TlsStream};

const MAX_CLIENT_HELLO_LENGTH: usize = 64 * 1024;

pub struct TlsConnection {
    accepted: rustls::server::Accepted,
    server_name: Option<String>,
    prefix: Vec<u8>,
    stream: TcpStream,
}

impl TlsConnection {
    pub fn server_name(&self) -> Option<&str> {
        self.server_name.as_deref()
    }

    pub fn is_acme_challenge(&self) -> bool {
        self.accepted
            .client_hello()
            .alpn()
            .is_some_and(|protocols| {
                protocols
                    .into_iter()
                    .any(|protocol| protocol == b"acme-tls/1")
            })
    }

    pub async fn terminate(self, config: Arc<ServerConfig>) -> Result<TlsStream<TcpStream>> {
        StartHandshake::from_parts(self.accepted, self.stream)
            .into_stream(config)
            .await
            .context("could not complete the TLS handshake")
    }

    pub fn into_raw_parts(self) -> (Vec<u8>, TcpStream) {
        (self.prefix, self.stream)
    }
}

pub async fn inspect(mut stream: TcpStream) -> Result<Option<TlsConnection>> {
    let mut first_byte = [0];
    stream
        .read_exact(&mut first_byte)
        .await
        .context("could not read the connection preface")?;

    if first_byte[0] != 22 {
        stream.shutdown().await?;
        return Ok(None);
    }

    let mut acceptor = Acceptor::default();
    let mut prefix = first_byte.to_vec();
    feed_acceptor(&mut acceptor, &first_byte)?;

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
            return Ok(Some(TlsConnection {
                accepted,
                server_name,
                prefix,
                stream,
            }));
        }

        let mut buffer = [0; 4096];
        let bytes_read = stream
            .read(&mut buffer)
            .await
            .context("could not read the TLS ClientHello")?;
        if bytes_read == 0 {
            bail!("connection closed before sending a complete TLS ClientHello");
        }
        if prefix.len() + bytes_read > MAX_CLIENT_HELLO_LENGTH {
            bail!("TLS ClientHello is too large");
        }

        let bytes = &buffer[..bytes_read];
        prefix.extend_from_slice(bytes);
        feed_acceptor(&mut acceptor, bytes)?;
    }
}

fn feed_acceptor(acceptor: &mut Acceptor, bytes: &[u8]) -> Result<()> {
    let mut cursor = Cursor::new(bytes);
    while cursor.position() < bytes.len() as u64 {
        if acceptor.read_tls(&mut cursor)? == 0 {
            bail!("Rustls did not consume the ClientHello bytes");
        }
    }
    Ok(())
}
