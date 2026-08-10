use std::{fs, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use clap::{Parser, Subcommand};
use rand::{Rng, distributions::Alphanumeric};
use rustls::{ClientConfig, RootCertStore, ServerConfig, pki_types::ServerName};
use rustls_acme::{AcmeConfig, EventOk, caches::DirCache, is_tls_alpn_challenge};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, copy_bidirectional},
    net::TcpStream,
};
use tokio_rustls::{LazyConfigAcceptor, TlsConnector, client::TlsStream};
use tokio_stream::StreamExt;
use url::{Host, Url};

const LOGIN_BLOB_PREFIX: &str = "tunnel-login-v1.";
const MAX_HTTP_RESPONSE_HEADER_LENGTH: usize = 16 * 1024;

#[derive(Parser)]
#[command(version, about = "Tunnel client")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Login {
        blob: String,
    },
    Expose {
        port: u16,
        #[arg(short, long)]
        name: Option<String>,
    },
}

#[derive(Deserialize)]
struct LoginPayload {
    api_url: String,
    token: String,
}

#[derive(Clone, Deserialize, Serialize)]
struct Config {
    api_url: String,
    token: String,
}

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
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Login { blob } => login(&blob),
        Command::Expose { port, name } => expose(port, name).await,
    }
}

fn login(blob: &str) -> Result<()> {
    let encoded = blob
        .trim()
        .strip_prefix(LOGIN_BLOB_PREFIX)
        .with_context(|| format!("not a valid {LOGIN_BLOB_PREFIX}* login blob"))?;
    let json = URL_SAFE_NO_PAD
        .decode(encoded)
        .with_context(|| format!("not a valid {LOGIN_BLOB_PREFIX}* login blob"))?;
    let payload: LoginPayload = serde_json::from_slice(&json)
        .with_context(|| format!("not a valid {LOGIN_BLOB_PREFIX}* login blob"))?;

    let api_url = Url::parse(&payload.api_url).context("login blob contains an invalid API URL")?;
    if api_url.scheme() != "https" || api_url.host().is_none() {
        bail!("login blob API URL must be an HTTPS URL with a host");
    }
    if payload.token.is_empty() || payload.token.chars().any(char::is_control) {
        bail!("login blob contains an invalid token");
    }

    let path = config_path()?;
    let directory = path
        .parent()
        .context("client config path does not have a parent directory")?;
    fs::create_dir_all(directory)
        .with_context(|| format!("could not create {}", directory.display()))?;
    let config = Config {
        api_url: payload.api_url,
        token: payload.token,
    };
    let json =
        serde_json::to_string_pretty(&config).context("could not serialize client config")?;
    fs::write(&path, format!("{json}\n"))
        .with_context(|| format!("could not write config to {}", path.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    }

    println!("Logged in to {}", config.api_url);
    println!("Configuration saved to {}", path.display());
    Ok(())
}

async fn expose(port: u16, name: Option<String>) -> Result<()> {
    if port == 0 {
        bail!("port must be between 1 and 65535");
    }

    let config = Arc::new(read_config()?);
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
                    if let Err(error) = forward_connection(&config, endpoint_tls, port, &id).await {
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
    endpoint_tls: EndpointTls,
    port: u16,
    connection_id: &str,
) -> Result<()> {
    let tunnel_stream =
        open_api_connection(config, &format!("/v1/connections/{connection_id}")).await?;
    let handshake = LazyConfigAcceptor::new(rustls::server::Acceptor::default(), tunnel_stream)
        .await
        .context("could not read the visitor TLS ClientHello")?;
    let is_challenge = is_tls_alpn_challenge(&handshake.client_hello());
    let tls_config = if is_challenge {
        endpoint_tls.challenge
    } else {
        endpoint_tls.regular
    };
    let mut visitor_stream = handshake
        .into_stream(tls_config)
        .await
        .context("could not complete visitor TLS handshake")?;

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

fn start_endpoint_tls(hostname: String, url: String, port: u16) -> Result<EndpointTls> {
    let cache_directory = config_path()?
        .parent()
        .context("client config path does not have a parent directory")?
        .join("acme");
    fs::create_dir_all(&cache_directory).with_context(|| {
        format!(
            "could not create TLS cache directory {}",
            cache_directory.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&cache_directory, fs::Permissions::from_mode(0o700))?;
    }

    let mut state = AcmeConfig::new([hostname.clone()])
        .cache(DirCache::new(cache_directory))
        .directory_lets_encrypt(true)
        .state();
    let endpoint_tls = EndpointTls {
        regular: state.default_rustls_config(),
        challenge: state.challenge_rustls_config(),
    };

    tokio::spawn(async move {
        while let Some(event) = state.next().await {
            match event {
                Ok(EventOk::DeployedCachedCert | EventOk::DeployedNewCert) => {
                    println!("Forwarding {url} to http://127.0.0.1:{port}");
                }
                Ok(EventOk::CertCacheStore | EventOk::AccountCacheStore) => {}
                Err(error) => eprintln!("TLS certificate error for {hostname}: {error}"),
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

fn read_config() -> Result<Config> {
    let path = config_path()?;
    let json = fs::read_to_string(&path).with_context(|| {
        format!(
            "could not read config from {}; log in first",
            path.display()
        )
    })?;
    serde_json::from_str(&json)
        .with_context(|| format!("could not parse config from {}", path.display()))
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

fn config_path() -> Result<PathBuf> {
    Ok(dirs::home_dir()
        .context("could not determine the home directory")?
        .join(".tunnel/config.json"))
}
