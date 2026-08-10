use std::{io, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use rand::{Rng, distributions::Alphanumeric};
use rustls::{ClientConfig, RootCertStore, ServerConfig, pki_types::ServerName};
use rustls_acme::{AcmeConfig, EventError, EventOk, caches::DirCache, is_tls_alpn_challenge};
use serde::Deserialize;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, copy_bidirectional},
    net::TcpStream,
    sync::watch,
};
use tokio_rustls::{LazyConfigAcceptor, TlsConnector, client::TlsStream};
use tokio_stream::StreamExt;
use url::{Host, Url};

use crate::config::{self, Config};

const MAX_HTTP_RESPONSE_HEADER_LENGTH: usize = 16 * 1024;
const ACME_ORDER_RETRY_DELAY: Duration = Duration::from_secs(60 * 60);

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ControlMessage {
    Ready { url: String },
    Connection { id: String },
}

#[derive(Clone)]
struct EndpointTls {
    regular: Arc<ServerConfig>,
    challenge: Arc<ServerConfig>,
    certificate_ready: watch::Receiver<bool>,
}

pub async fn expose(port: u16, name: Option<String>) -> Result<()> {
    if port == 0 {
        bail!("port must be between 1 and 65535");
    }

    let config = Arc::new(config::read()?);
    let tunnel_id = match name {
        Some(name) => {
            let name = name.to_ascii_lowercase();
            validate_tunnel_id(&name)?;
            name
        }
        None => default_tunnel_id()?,
    };

    let control_stream = open_tunnel_connection(&config, &tunnel_id).await?;
    let mut messages = BufReader::new(control_stream).lines();
    let mut endpoint_tls = None;

    while let Some(line) = messages.next_line().await? {
        match serde_json::from_str::<ControlMessage>(&line).context("invalid server message")? {
            ControlMessage::Ready { url } => {
                let hostname = Url::parse(&url)
                    .context("server returned an invalid tunnel URL")?
                    .host_str()
                    .context("server tunnel URL did not contain a hostname")?
                    .to_owned();
                println!("Obtaining a TLS certificate for {hostname}...");
                endpoint_tls = Some(start_endpoint_tls(hostname, url, port)?);
            }
            ControlMessage::Connection { id } => {
                let endpoint_tls = endpoint_tls
                    .clone()
                    .context("server sent a connection before the tunnel was ready")?;
                let config = Arc::clone(&config);
                tokio::spawn(async move {
                    if let Err(error) = forward_connection(&config, endpoint_tls, port, &id).await
                        && !is_routine_connection_error(&error)
                    {
                        eprintln!("connection {id} failed: {error:#}");
                    }
                });
            }
        }
    }

    bail!("tunnel server closed the control connection")
}

async fn forward_connection(
    config: &Config,
    mut endpoint_tls: EndpointTls,
    port: u16,
    connection_id: &str,
) -> Result<()> {
    let tunnel_stream =
        open_api_connection(config, &format!("/v1/connections/{connection_id}")).await?;
    let handshake = LazyConfigAcceptor::new(rustls::server::Acceptor::default(), tunnel_stream)
        .await
        .context("could not read the visitor TLS ClientHello")?;
    let is_challenge = is_tls_alpn_challenge(&handshake.client_hello());
    if !is_challenge {
        endpoint_tls
            .certificate_ready
            .wait_for(|ready| *ready)
            .await
            .context("TLS certificate manager stopped before obtaining a certificate")?;
    }
    let tls_config = if is_challenge {
        endpoint_tls.challenge
    } else {
        endpoint_tls.regular
    };
    let handshake_context = if is_challenge {
        "could not complete ACME TLS-ALPN-01 handshake"
    } else {
        "could not complete visitor TLS handshake"
    };
    let mut visitor_stream = handshake
        .into_stream(tls_config)
        .await
        .context(handshake_context)?;

    if is_challenge {
        visitor_stream.shutdown().await?;
        return Ok(());
    }

    let mut local_stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .with_context(|| format!("could not connect to 127.0.0.1:{port}"))?;

    copy_bidirectional(&mut visitor_stream, &mut local_stream)
        .await
        .context("could not forward tunnel connection")?;
    Ok(())
}

fn is_routine_connection_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if let Some(error) = cause.downcast_ref::<io::Error>() {
            return matches!(
                error.kind(),
                io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
            );
        }

        cause.downcast_ref::<rustls::Error>().is_some_and(|error| {
            matches!(
                error,
                rustls::Error::InappropriateMessage { .. }
                    | rustls::Error::InappropriateHandshakeMessage { .. }
                    | rustls::Error::InvalidMessage(_)
                    | rustls::Error::PeerIncompatible(_)
                    | rustls::Error::PeerMisbehaved(_)
                    | rustls::Error::AlertReceived(_)
                    | rustls::Error::NoApplicationProtocol
            )
        })
    })
}

fn start_endpoint_tls(hostname: String, url: String, port: u16) -> Result<EndpointTls> {
    let cache_directory = config::path()?
        .parent()
        .context("client config path does not have a parent directory")?
        .join("acme");
    std::fs::create_dir_all(&cache_directory).with_context(|| {
        format!(
            "could not create TLS cache directory {}",
            cache_directory.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&cache_directory, std::fs::Permissions::from_mode(0o700))?;
    }

    let mut state = AcmeConfig::new([hostname.clone()])
        .cache(DirCache::new(cache_directory))
        .directory_lets_encrypt(true)
        .state();
    let (certificate_ready, certificate_status) = watch::channel(false);
    let endpoint_tls = EndpointTls {
        regular: state.default_rustls_config(),
        challenge: state.challenge_rustls_config(),
        certificate_ready: certificate_status,
    };

    tokio::spawn(async move {
        while let Some(event) = state.next().await {
            match event {
                Ok(EventOk::DeployedCachedCert | EventOk::DeployedNewCert) => {
                    certificate_ready.send_replace(true);
                    println!("Forwarding {url} to http://127.0.0.1:{port}");
                }
                Ok(EventOk::CertCacheStore | EventOk::AccountCacheStore) => {}
                Err(error) => {
                    eprintln!("TLS certificate error for {hostname}: {error}");
                    if matches!(error, EventError::Order(_)) {
                        eprintln!("Retrying certificate order for {hostname} in 1 hour");
                        tokio::time::sleep(ACME_ORDER_RETRY_DELAY).await;
                    }
                }
            }
        }
    });

    Ok(endpoint_tls)
}

async fn open_tunnel_connection(config: &Config, tunnel_id: &str) -> Result<TlsStream<TcpStream>> {
    open_api_connection(config, &format!("/v1/tunnels/{tunnel_id}"))
        .await
        .with_context(|| format!("could not register tunnel {tunnel_id}"))
}

async fn open_api_connection(config: &Config, path: &str) -> Result<TlsStream<TcpStream>> {
    let api_url = Url::parse(&config.api_url).context("config contains an invalid API URL")?;
    if api_url.scheme() != "https" {
        bail!("config API URL must use HTTPS");
    }
    if config.token.is_empty() || config.token.chars().any(char::is_control) {
        bail!("config contains an invalid token");
    }

    let host = api_url
        .host_str()
        .context("config API URL does not contain a host")?;
    let port = api_url
        .port_or_known_default()
        .context("config API URL does not contain a port")?;
    let tcp_stream = TcpStream::connect((host, port))
        .await
        .with_context(|| format!("could not connect to {host}:{port}"))?;

    let roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let tls_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server_name = ServerName::try_from(host.to_owned()).context("invalid API hostname")?;
    let mut stream = TlsConnector::from(Arc::new(tls_config))
        .connect(server_name, tcp_stream)
        .await
        .context("could not establish TLS with the tunnel server")?;

    let host_header = match api_url.host().expect("host was checked above") {
        Host::Ipv6(address) => format!("[{address}]:{port}"),
        _ => format!("{host}:{port}"),
    };
    let request = format!(
        "CONNECT {path} HTTP/1.1\r\nHost: {host_header}\r\nAuthorization: Bearer {}\r\n\r\n",
        config.token
    );
    stream.write_all(request.as_bytes()).await?;

    let response_header = read_http_response_header(&mut stream).await?;
    let status_line = response_header
        .lines()
        .next()
        .context("tunnel server returned an empty HTTP response")?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<u16>().ok())
        .context("tunnel server returned an invalid HTTP status")?;
    if !(200..300).contains(&status) {
        bail!("tunnel server rejected the connection ({status_line})");
    }

    Ok(stream)
}

async fn read_http_response_header(stream: &mut TlsStream<TcpStream>) -> Result<String> {
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        if header.len() == MAX_HTTP_RESPONSE_HEADER_LENGTH {
            bail!("tunnel server HTTP response header is too large");
        }
        let mut byte = [0];
        stream
            .read_exact(&mut byte)
            .await
            .context("tunnel server closed the connection during the HTTP response")?;
        header.push(byte[0]);
    }
    String::from_utf8(header).context("tunnel server returned a non-UTF-8 HTTP response")
}

fn default_tunnel_id() -> Result<String> {
    let directory = std::env::current_dir().context("could not determine current directory")?;
    let name = directory
        .file_name()
        .and_then(|name| name.to_str())
        .map(sanitize_tunnel_id)
        .filter(|name| !name.is_empty());

    Ok(name.unwrap_or_else(|| {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .filter(|byte| byte.is_ascii_alphanumeric())
            .take(8)
            .map(char::from)
            .collect::<String>()
            .to_ascii_lowercase()
    }))
}

fn sanitize_tunnel_id(value: &str) -> String {
    let mut result = String::new();
    let mut previous_was_separator = false;
    for character in value.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_lowercase() || character.is_ascii_digit() {
            result.push(character);
            previous_was_separator = false;
        } else if !previous_was_separator && !result.is_empty() {
            result.push('-');
            previous_was_separator = true;
        }
    }
    result.truncate(63);
    result.trim_end_matches('-').to_owned()
}

fn validate_tunnel_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 63
        || value.starts_with('-')
        || value.ends_with('-')
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        bail!("tunnel name must contain only lowercase letters, numbers, and internal hyphens");
    }
    Ok(())
}
