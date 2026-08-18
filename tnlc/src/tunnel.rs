use std::{
    error::Error,
    fmt, io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use rand::{Rng, distributions::Alphanumeric};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    crypto::{CryptoProvider, ring},
    pki_types::ServerName,
};
use rustls_acme::{AcmeConfig, EventError, EventOk, caches::DirCache, is_tls_alpn_challenge};
use socket2::{SockRef, TcpKeepalive};
use tnl::{
    PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER, SessionConfig, TRANSPORT_ACTIVATION_MARKER,
    TunnelId, client::TunnelClient,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, copy_bidirectional_with_sizes},
    net::TcpStream,
    sync::{Semaphore, watch},
    task::JoinSet,
    time::Instant,
};
use tokio_rustls::{LazyConfigAcceptor, TlsConnector, client::TlsStream};
use tokio_stream::StreamExt;
use url::{Host, Url};

use crate::config;

const MAX_HTTP_RESPONSE_HEADER_LENGTH: usize = 16 * 1024;
const ACME_ORDER_RETRY_DELAY: Duration = Duration::from_secs(60 * 60);
const TCP_KEEPALIVE_IDLE: Duration = Duration::from_secs(30);
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const TCP_KEEPALIVE_RETRIES: u32 = 3;
const RECONNECT_INITIAL_DELAY: Duration = Duration::from_secs(1);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);
const RECONNECT_BACKOFF_RESET_AFTER: Duration = Duration::from_secs(60);
const RECONNECT_JITTER_MAX_MILLIS: u64 = 250;
const TCP_FORWARD_TAG: &str = "tnl/tcp";
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(60);
const FORWARD_BUFFER_SIZE: usize = 64 * 1024;
const MAX_CONTROL_SESSION_COUNT: usize = 8;
const MAX_TRANSPORT_POOL_SIZE: usize = 64;
const MAX_CONCURRENT_DEDICATED_TRANSPORTS: usize = 64;
const TRANSPORT_RETRY_DELAY: Duration = Duration::from_secs(1);

type ApiStream = TlsStream<TcpStream>;

#[derive(Clone)]
struct HttpTransport {
    api_url: Url,
    authorization: Arc<str>,
    tls_config: Arc<ClientConfig>,
}

#[derive(Clone)]
struct EndpointTls {
    hostname: Arc<str>,
    regular: Arc<ServerConfig>,
    challenge: Arc<ServerConfig>,
    certificate_ready: watch::Receiver<bool>,
}

struct TunnelRegistration {
    stream: ApiStream,
    url: String,
    control_session_count: usize,
    transport_pool_size: usize,
}

#[derive(Debug)]
struct ApiRejection {
    status: u16,
    status_line: String,
}

impl fmt::Display for ApiRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "tunnel server rejected the connection ({})",
            self.status_line
        )
    }
}

impl Error for ApiRejection {}

pub async fn expose(port: u16, name: Option<String>) -> Result<()> {
    if port == 0 {
        bail!("port must be between 1 and 65535");
    }

    let config = config::read()?;
    let tunnel_id = match name {
        Some(name) => TunnelId::new(name.to_ascii_lowercase())?,
        None => default_tunnel_id()?,
    };
    let api_url = Url::parse(&config.api_url).context("config contains an invalid API URL")?;
    if api_url.scheme() != "https" {
        bail!("config API URL must use HTTPS");
    }
    if config.token.is_empty() || config.token.chars().any(char::is_control) {
        bail!("config contains an invalid token");
    }
    let cache_directory = config::path()?
        .parent()
        .context("tnlc config path does not have a parent directory")?
        .join("acme");
    let roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let tls_config = ClientConfig::builder_with_provider(transport_crypto_provider())
        .with_safe_default_protocol_versions()
        .context("could not configure tunnel TLS protocol versions")?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let transport = Arc::new(HttpTransport {
        api_url,
        authorization: Arc::from(format!("Bearer {}", config.token)),
        tls_config: Arc::new(tls_config),
    });
    let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);

    run(transport, cache_directory, target, tunnel_id).await
}

// Prefer AES-128-GCM for the outer transport TLS: same protections against
// active attackers, measurably faster bulk encryption than the AES-256 default.
fn transport_crypto_provider() -> Arc<CryptoProvider> {
    let mut provider = ring::default_provider();
    provider.cipher_suites.sort_by_key(|suite| {
        usize::from(suite.suite() != rustls::CipherSuite::TLS13_AES_128_GCM_SHA256)
    });
    Arc::new(provider)
}

async fn run(
    transport: Arc<HttpTransport>,
    cache_directory: PathBuf,
    target: SocketAddr,
    tunnel_id: TunnelId,
) -> Result<()> {
    let mut endpoint_tls = None;
    let mut has_registered = false;
    let mut reconnect_delay = RECONNECT_INITIAL_DELAY;
    let mut background_tasks = JoinSet::new();

    loop {
        let session_started = Instant::now();
        let result = run_control_session(
            Arc::clone(&transport),
            &cache_directory,
            &tunnel_id,
            target,
            &mut endpoint_tls,
            &mut has_registered,
            &mut background_tasks,
        )
        .await;

        let error = result.expect_err("control sessions only end when their connection fails");
        if registration_error_is_fatal(&error, has_registered) {
            return Err(error);
        }
        if session_started.elapsed() >= RECONNECT_BACKOFF_RESET_AFTER {
            reconnect_delay = RECONNECT_INITIAL_DELAY;
        }
        let jitter =
            Duration::from_millis(rand::thread_rng().r#gen_range(0..=RECONNECT_JITTER_MAX_MILLIS));
        let retry_in = reconnect_delay + jitter;
        eprintln!(
            "Tunnel connection lost: {error:#}\nReconnecting in {:.1}s...",
            retry_in.as_secs_f32()
        );
        tokio::time::sleep(retry_in).await;
        reconnect_delay = (reconnect_delay * 2).min(RECONNECT_MAX_DELAY);
    }
}

async fn run_control_session(
    transport: Arc<HttpTransport>,
    cache_directory: &Path,
    tunnel_id: &TunnelId,
    target: SocketAddr,
    endpoint_tls: &mut Option<EndpointTls>,
    has_registered: &mut bool,
    background_tasks: &mut JoinSet<()>,
) -> Result<()> {
    let TunnelRegistration {
        stream: control_stream,
        url,
        control_session_count,
        transport_pool_size,
    } = transport
        .open_tunnel(tunnel_id)
        .await
        .with_context(|| format!("could not register tunnel {tunnel_id}"))?;
    *has_registered = true;
    let client = new_tunnel_client(control_stream).await?;
    let hostname = tunnel_hostname(&url)?;

    if let Some(endpoint_tls) = endpoint_tls {
        if endpoint_tls.hostname.as_ref() != hostname {
            bail!(
                "tunnel hostname changed from {} to {hostname}",
                endpoint_tls.hostname
            );
        }
        println!("Reconnected {url}");
    } else {
        println!("Obtaining a TLS certificate for {hostname}...");
        *endpoint_tls = Some(start_endpoint_tls(
            cache_directory.to_path_buf(),
            hostname.clone(),
            url,
            target,
            background_tasks,
        )?);
    }

    let mut clients = vec![client];
    for _ in 1..control_session_count {
        let additional = transport
            .open_tunnel(tunnel_id)
            .await
            .with_context(|| format!("could not open an additional session for {tunnel_id}"))?;
        let additional_hostname = tunnel_hostname(&additional.url)?;
        if additional_hostname != hostname {
            bail!(
                "additional tunnel session hostname changed from {hostname} to {additional_hostname}"
            );
        }
        clients.push(new_tunnel_client(additional.stream).await?);
    }

    let endpoint_tls = endpoint_tls
        .clone()
        .context("tunnel endpoint TLS was not initialized")?;
    let mut control_sessions = JoinSet::new();
    for client in clients {
        control_sessions.spawn(serve_control_session(client, endpoint_tls.clone(), target));
    }

    let mut transport_workers = JoinSet::new();
    let transport_permits = Arc::new(Semaphore::new(MAX_CONCURRENT_DEDICATED_TRANSPORTS));
    for _ in 0..transport_pool_size {
        transport_workers.spawn(serve_transport_worker(
            Arc::clone(&transport),
            Arc::clone(&transport_permits),
            tunnel_id.clone(),
            endpoint_tls.clone(),
            target,
        ));
    }

    // Dropping the JoinSets aborts the remaining sessions and workers.
    control_sessions
        .join_next()
        .await
        .context("tunnel did not start any control sessions")?
        .context("tunnel control session task failed")?
}

async fn new_tunnel_client(control_stream: ApiStream) -> Result<TunnelClient> {
    Ok(TunnelClient::new(
        control_stream,
        SessionConfig::new().heartbeat(HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT),
    )
    .await?)
}

fn tunnel_hostname(url: &str) -> Result<String> {
    Ok(Url::parse(url)
        .context("server returned an invalid tunnel URL")?
        .host_str()
        .context("server tunnel URL did not contain a hostname")?
        .to_owned())
}

async fn serve_transport_worker(
    transport: Arc<HttpTransport>,
    transport_permits: Arc<Semaphore>,
    tunnel_id: TunnelId,
    endpoint_tls: EndpointTls,
    target: SocketAddr,
) {
    let mut connection_tasks = JoinSet::new();
    loop {
        while connection_tasks.try_join_next().is_some() {}
        let permit = Arc::clone(&transport_permits)
            .acquire_owned()
            .await
            .expect("dedicated transport semaphore is never closed");
        match transport.open_transport(&tunnel_id).await {
            Ok(stream) => {
                let endpoint_tls = endpoint_tls.clone();
                connection_tasks.spawn(async move {
                    let _permit = permit;
                    log_forward_error(
                        forward_connection(stream, endpoint_tls, target).await,
                        "dedicated tunnel transport failed",
                    );
                });
            }
            Err(error) => {
                drop(permit);
                if !is_routine_connection_error(&error) {
                    eprintln!("could not replenish dedicated tunnel transport: {error:#}");
                }
                tokio::time::sleep(TRANSPORT_RETRY_DELAY).await;
            }
        }
    }
}

async fn serve_control_session(
    client: TunnelClient,
    endpoint_tls: EndpointTls,
    target: SocketAddr,
) -> Result<()> {
    let mut connection_tasks = JoinSet::new();
    loop {
        let tunnel_stream = client.accept().await?;
        if tunnel_stream.tag() != TCP_FORWARD_TAG {
            eprintln!("ignoring unsupported stream tag: {}", tunnel_stream.tag());
            continue;
        }
        let endpoint_tls = endpoint_tls.clone();
        while connection_tasks.try_join_next().is_some() {}
        connection_tasks.spawn(async move {
            log_forward_error(
                forward_connection(tunnel_stream, endpoint_tls, target).await,
                "tunnel connection failed",
            );
        });
    }
}

async fn forward_connection<S>(
    tunnel_stream: S,
    mut endpoint_tls: EndpointTls,
    target: SocketAddr,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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

    let mut local_stream = TcpStream::connect(target)
        .await
        .with_context(|| format!("could not connect to {target}"))?;
    local_stream
        .set_nodelay(true)
        .with_context(|| format!("could not disable Nagle's algorithm for {target}"))?;
    copy_bidirectional_with_sizes(
        &mut visitor_stream,
        &mut local_stream,
        FORWARD_BUFFER_SIZE,
        FORWARD_BUFFER_SIZE,
    )
    .await
    .context("could not forward tunnel connection")?;
    Ok(())
}

fn start_endpoint_tls(
    cache_directory: PathBuf,
    hostname: String,
    url: String,
    target: SocketAddr,
    background_tasks: &mut JoinSet<()>,
) -> Result<EndpointTls> {
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
        hostname: Arc::from(hostname.as_str()),
        regular: state.default_rustls_config(),
        challenge: state.challenge_rustls_config(),
        certificate_ready: certificate_status,
    };

    background_tasks.spawn(async move {
        while let Some(event) = state.next().await {
            match event {
                Ok(EventOk::DeployedCachedCert | EventOk::DeployedNewCert) => {
                    certificate_ready.send_replace(true);
                    println!("Forwarding {url} to http://{target}");
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

impl HttpTransport {
    async fn open_tunnel(&self, tunnel_id: &TunnelId) -> Result<TunnelRegistration> {
        let (stream, response_header) = self
            .open_connect(&format!("/v1/tunnels/{tunnel_id}"))
            .await?;

        let url = header_value(&response_header, "X-Tnl-Url")
            .filter(|value| !value.is_empty())
            .context("tunnel server response did not include X-Tnl-Url")?
            .to_owned();

        let control_session_count = required_header_count(
            &response_header,
            "x-tnl-control-sessions",
            MAX_CONTROL_SESSION_COUNT,
        )?;
        let transport_pool_size = required_header_count(
            &response_header,
            "x-tnl-transport-pool",
            MAX_TRANSPORT_POOL_SIZE,
        )?;

        Ok(TunnelRegistration {
            stream,
            url,
            control_session_count,
            transport_pool_size,
        })
    }

    async fn open_transport(&self, tunnel_id: &TunnelId) -> Result<ApiStream> {
        let (mut stream, _) = self
            .open_connect(&format!("/v1/tunnels/{tunnel_id}/transports"))
            .await?;
        let mut marker = [0; TRANSPORT_ACTIVATION_MARKER.len()];
        stream
            .read_exact(&mut marker)
            .await
            .context("tunnel server closed an idle dedicated transport")?;
        if &marker != TRANSPORT_ACTIVATION_MARKER {
            bail!("tunnel server sent an invalid transport activation marker");
        }
        Ok(stream)
    }

    async fn open_connect(&self, path: &str) -> Result<(ApiStream, String)> {
        let host = self
            .api_url
            .host_str()
            .context("API URL does not contain a host")?;
        let port = self
            .api_url
            .port_or_known_default()
            .context("API URL does not contain a port")?;
        let tcp_stream = TcpStream::connect((host, port))
            .await
            .with_context(|| format!("could not connect to {host}:{port}"))?;
        tcp_stream
            .set_nodelay(true)
            .with_context(|| format!("could not disable Nagle's algorithm for {host}:{port}"))?;
        configure_tcp_keepalive(&tcp_stream)
            .with_context(|| format!("could not configure TCP keepalive for {host}:{port}"))?;
        let server_name = ServerName::try_from(host.to_owned()).context("invalid API hostname")?;
        let mut stream = TlsConnector::from(Arc::clone(&self.tls_config))
            .connect(server_name, tcp_stream)
            .await
            .context("could not establish TLS with the tunnel server")?;

        let host_header = match self.api_url.host().expect("host was checked above") {
            Host::Ipv6(address) => format!("[{address}]:{port}"),
            _ => format!("{host}:{port}"),
        };
        let request = format!(
            "CONNECT {path} HTTP/1.1\r\nHost: {host_header}\r\nAuthorization: {}\r\n{PROTOCOL_VERSION_HEADER}: {PROTOCOL_VERSION}\r\n\r\n",
            self.authorization
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
            return Err(ApiRejection {
                status,
                status_line: status_line.to_owned(),
            }
            .into());
        }
        validate_protocol_version(&response_header)?;

        Ok((stream, response_header))
    }
}

fn header_value<'a>(header: &'a str, name: &str) -> Option<&'a str> {
    header
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.trim())
}

fn required_header_count(header: &str, name: &str, maximum: usize) -> Result<usize> {
    header_value(header, name)
        .with_context(|| format!("tunnel server response did not include {name}"))?
        .parse::<usize>()
        .ok()
        .filter(|count| (1..=maximum).contains(count))
        .with_context(|| format!("tunnel server returned an invalid {name}"))
}

fn validate_protocol_version(header: &str) -> Result<()> {
    let protocol_version = header_value(header, PROTOCOL_VERSION_HEADER)
        .context("tunnel server did not declare its protocol version")?;
    if protocol_version != PROTOCOL_VERSION {
        bail!(
            "tunnel server uses unsupported protocol version {protocol_version}; expected {PROTOCOL_VERSION}"
        );
    }
    Ok(())
}

fn registration_error_is_fatal(error: &anyhow::Error, has_registered: bool) -> bool {
    error
        .downcast_ref::<ApiRejection>()
        .is_some_and(|rejection| {
            (400..500).contains(&rejection.status) && !(rejection.status == 409 && has_registered)
        })
}

fn configure_tcp_keepalive(stream: &TcpStream) -> io::Result<()> {
    SockRef::from(stream).set_tcp_keepalive(
        &TcpKeepalive::new()
            .with_time(TCP_KEEPALIVE_IDLE)
            .with_interval(TCP_KEEPALIVE_INTERVAL)
            .with_retries(TCP_KEEPALIVE_RETRIES),
    )
}

async fn read_http_response_header(stream: &mut ApiStream) -> Result<String> {
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

fn log_forward_error(result: Result<()>, message: &str) {
    if let Err(error) = result
        && !is_routine_connection_error(&error)
    {
        eprintln!("{message}: {error:#}");
    }
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

fn default_tunnel_id() -> Result<TunnelId> {
    let directory = std::env::current_dir().context("could not determine current directory")?;
    let name = directory
        .file_name()
        .and_then(|name| name.to_str())
        .map(sanitize_tunnel_id)
        .filter(|name| !name.is_empty());
    let name = name.unwrap_or_else(|| {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .filter(|byte| byte.is_ascii_alphanumeric())
            .take(8)
            .map(char::from)
            .collect::<String>()
            .to_ascii_lowercase()
    });
    Ok(TunnelId::new(name)?)
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

#[cfg(test)]
mod tests {
    use super::{required_header_count, validate_protocol_version};

    #[test]
    fn requires_current_protocol_version() {
        assert!(validate_protocol_version("X-Tnl-Protocol-Version: 2\r\n").is_ok());
        assert!(validate_protocol_version("X-Tnl-Protocol-Version: 1\r\n").is_err());
        assert!(validate_protocol_version("").is_err());
    }

    #[test]
    fn requires_valid_capability_counts() {
        let header = "X-Tnl-Control-Sessions: 8\r\n";
        assert_eq!(
            required_header_count(header, "X-Tnl-Control-Sessions", 8).unwrap(),
            8
        );
        assert!(required_header_count(header, "X-Tnl-Control-Sessions", 4).is_err());
        assert!(required_header_count("", "X-Tnl-Control-Sessions", 8).is_err());
    }
}
