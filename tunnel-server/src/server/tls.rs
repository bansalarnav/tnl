use std::{fs, io::Cursor, path::PathBuf, sync::Arc};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use anyhow::{Context as _, Result, bail};
use rustls::{ServerConfig, server::Acceptor};
use rustls_acme::{AcmeConfig, caches::DirCache, is_tls_alpn_challenge};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::{StartHandshake, server::TlsStream};
use tokio_stream::StreamExt;

const MAX_CLIENT_HELLO_LENGTH: usize = 64 * 1024;

pub struct Configs {
    pub api: Arc<ServerConfig>,
    pub acme_challenge: Arc<ServerConfig>,
}

pub fn manage_certificate(domain: &str, cache_directory: PathBuf) -> Result<Configs> {
    fs::create_dir_all(&cache_directory).with_context(|| {
        format!(
            "could not create ACME cache directory {}",
            cache_directory.display()
        )
    })?;
    #[cfg(unix)]
    fs::set_permissions(&cache_directory, fs::Permissions::from_mode(0o700)).with_context(
        || {
            format!(
                "could not secure ACME cache directory {}",
                cache_directory.display()
            )
        },
    )?;

    let mut acme = AcmeConfig::new([domain.to_owned()])
        .cache(DirCache::new(cache_directory))
        .directory_lets_encrypt(true)
        .state();
    let configs = Configs {
        api: acme.default_rustls_config(),
        acme_challenge: acme.challenge_rustls_config(),
    };

    println!("Managing TLS certificate for {domain}");
    tokio::spawn(async move {
        while let Some(event) = acme.next().await {
            match event {
                Ok(event) => println!("ACME: {event:?}"),
                Err(error) => eprintln!("ACME error: {error:?}"),
            }
        }
    });

    Ok(configs)
}

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
        is_tls_alpn_challenge(&self.accepted.client_hello())
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
