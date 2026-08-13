use std::{error::Error, fmt, io, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use rand::Rng;
use rustls::{ClientConfig, RootCertStore, ServerConfig, pki_types::ServerName};
use rustls_acme::{AcmeConfig, EventError, EventOk, caches::DirCache, is_tls_alpn_challenge};
use socket2::{SockRef, TcpKeepalive};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, copy_bidirectional},
    net::TcpStream,
    sync::watch,
    task::JoinSet,
    time::Instant,
};
use tokio_rustls::{LazyConfigAcceptor, TlsConnector, client::TlsStream};
use tokio_stream::StreamExt;
use url::{Host, Url};

use crate::{TunnelId, protocol::ServerControlMessage};

const MAX_HTTP_RESPONSE_HEADER_LENGTH: usize = 16 * 1024;
const ACME_ORDER_RETRY_DELAY: Duration = Duration::from_secs(60 * 60);
const TCP_KEEPALIVE_IDLE: Duration = Duration::from_secs(30);
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const TCP_KEEPALIVE_RETRIES: u32 = 3;
const RECONNECT_INITIAL_DELAY: Duration = Duration::from_secs(1);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);
const RECONNECT_BACKOFF_RESET_AFTER: Duration = Duration::from_secs(60);
const RECONNECT_JITTER_MAX_MILLIS: u64 = 250;
const PONG_MESSAGE: &[u8] = b"{\"type\":\"pong\"}\n";

#[derive(Clone, Debug)]
pub enum ClientEvent {
    ObtainingCertificate {
        hostname: String,
    },
    Ready {
        url: String,
        target: SocketAddr,
    },
    Reconnected {
        url: String,
    },
    Disconnected {
        error: String,
        retry_in: Duration,
    },
    ConnectionFailed {
        connection_id: String,
        error: String,
    },
    CertificateError {
        hostname: String,
        error: String,
        retry_in: Option<Duration>,
    },
}

type EventHandler = Arc<dyn Fn(ClientEvent) + Send + Sync>;

#[derive(Clone)]
pub struct Client {
    api_url: Url,
    authorization: Option<Arc<str>>,
    cache_directory: PathBuf,
    event_handler: EventHandler,
}

impl Client {
    pub fn new(api_url: Url, cache_directory: PathBuf) -> Result<Self> {
        if api_url.scheme() != "https" || api_url.host().is_none() {
            bail!("API URL must be an HTTPS URL with a host");
        }

        Ok(Self {
            api_url,
            authorization: None,
            cache_directory,
            event_handler: Arc::new(|_| {}),
        })
    }

    pub fn with_authorization(mut self, authorization: impl Into<Arc<str>>) -> Result<Self> {
        let authorization = authorization.into();
        if authorization.is_empty() || authorization.chars().any(char::is_control) {
            bail!("authorization must not be empty or contain control characters");
        }
        self.authorization = Some(authorization);
        Ok(self)
    }

    pub fn with_event_handler(
        mut self,
        handler: impl Fn(ClientEvent) + Send + Sync + 'static,
    ) -> Self {
        self.event_handler = Arc::new(handler);
        self
    }

    pub async fn expose(&self, target: SocketAddr, tunnel_id: TunnelId) -> Result<()> {
        if target.port() == 0 {
            bail!("target port must be between 1 and 65535");
        }
        expose(Arc::new(self.clone()), target, tunnel_id).await
    }

    fn emit(&self, event: ClientEvent) {
        (self.event_handler)(event);
    }
}

#[derive(Clone)]
struct EndpointTls {
    hostname: Arc<str>,
    regular: Arc<ServerConfig>,
    challenge: Arc<ServerConfig>,
    certificate_ready: watch::Receiver<bool>,
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

async fn expose(client: Arc<Client>, target: SocketAddr, tunnel_id: TunnelId) -> Result<()> {
    let mut endpoint_tls = None;
    let mut has_registered = false;
    let mut reconnect_delay = RECONNECT_INITIAL_DELAY;
    let mut background_tasks = JoinSet::new();

    loop {
        let session_started = Instant::now();
        let result = run_control_session(
            Arc::clone(&client),
            tunnel_id.as_str(),
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
        client.emit(ClientEvent::Disconnected {
            error: format!("{error:#}"),
            retry_in,
        });
        tokio::time::sleep(retry_in).await;
        reconnect_delay = (reconnect_delay * 2).min(RECONNECT_MAX_DELAY);
    }
}

async fn run_control_session(
    client: Arc<Client>,
    tunnel_id: &str,
    target: SocketAddr,
    endpoint_tls: &mut Option<EndpointTls>,
    has_registered: &mut bool,
    background_tasks: &mut JoinSet<()>,
) -> Result<()> {
    let control_stream = open_tunnel_connection(&client, tunnel_id).await?;
    *has_registered = true;
    let (reader, mut writer) = tokio::io::split(control_stream);
    let mut messages = BufReader::new(reader).lines();

    while let Some(line) = messages.next_line().await? {
        match serde_json::from_str::<ServerControlMessage>(&line)
            .context("invalid server message")?
        {
            ServerControlMessage::Ready { url } => {
                let hostname = Url::parse(&url)
                    .context("server returned an invalid tunnel URL")?
                    .host_str()
                    .context("server tunnel URL did not contain a hostname")?
                    .to_owned();

                if let Some(endpoint_tls) = endpoint_tls {
                    if endpoint_tls.hostname.as_ref() != hostname {
                        bail!(
                            "tunnel hostname changed from {} to {hostname}",
                            endpoint_tls.hostname
                        );
                    }
                    client.emit(ClientEvent::Reconnected { url });
                } else {
                    client.emit(ClientEvent::ObtainingCertificate {
                        hostname: hostname.clone(),
                    });
                    *endpoint_tls = Some(start_endpoint_tls(
                        &client,
                        hostname,
                        url,
                        target,
                        background_tasks,
                    )?);
                }
            }
            ServerControlMessage::Connection { id } => {
                let endpoint_tls = endpoint_tls
                    .clone()
                    .context("server sent a connection before the tunnel was ready")?;
                let client = Arc::clone(&client);
                while background_tasks.try_join_next().is_some() {}
                background_tasks.spawn(async move {
                    if let Err(error) = forward_connection(&client, endpoint_tls, target, &id).await
                        && !is_routine_connection_error(&error)
                    {
                        client.emit(ClientEvent::ConnectionFailed {
                            connection_id: id,
                            error: format!("{error:#}"),
                        });
                    }
                });
            }
            ServerControlMessage::Ping => writer
                .write_all(PONG_MESSAGE)
                .await
                .context("could not answer tunnel heartbeat")?,
        }
    }

    bail!("tunnel server closed the control connection")
}

async fn forward_connection(
    client: &Client,
    mut endpoint_tls: EndpointTls,
    target: SocketAddr,
    connection_id: &str,
) -> Result<()> {
    let tunnel_stream =
        open_api_connection(client, &format!("/v1/connections/{connection_id}")).await?;
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

fn start_endpoint_tls(
    client: &Arc<Client>,
    hostname: String,
    url: String,
    target: SocketAddr,
    background_tasks: &mut JoinSet<()>,
) -> Result<EndpointTls> {
    let cache_directory = client.cache_directory.clone();
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

    let client = Arc::clone(client);
    background_tasks.spawn(async move {
        while let Some(event) = state.next().await {
            match event {
                Ok(EventOk::DeployedCachedCert | EventOk::DeployedNewCert) => {
                    certificate_ready.send_replace(true);
                    client.emit(ClientEvent::Ready {
                        url: url.clone(),
                        target,
                    });
                }
                Ok(EventOk::CertCacheStore | EventOk::AccountCacheStore) => {}
                Err(error) => {
                    let retry_in =
                        matches!(error, EventError::Order(_)).then_some(ACME_ORDER_RETRY_DELAY);
                    client.emit(ClientEvent::CertificateError {
                        hostname: hostname.clone(),
                        error: error.to_string(),
                        retry_in,
                    });
                    if let Some(delay) = retry_in {
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
    });

    Ok(endpoint_tls)
}

async fn open_tunnel_connection(client: &Client, tunnel_id: &str) -> Result<TlsStream<TcpStream>> {
    open_api_connection(client, &format!("/v1/tunnels/{tunnel_id}"))
        .await
        .with_context(|| format!("could not register tunnel {tunnel_id}"))
}

async fn open_api_connection(client: &Client, path: &str) -> Result<TlsStream<TcpStream>> {
    let api_url = &client.api_url;
    let host = api_url
        .host_str()
        .context("API URL does not contain a host")?;
    let port = api_url
        .port_or_known_default()
        .context("API URL does not contain a port")?;
    let tcp_stream = TcpStream::connect((host, port))
        .await
        .with_context(|| format!("could not connect to {host}:{port}"))?;
    configure_tcp_keepalive(&tcp_stream)
        .with_context(|| format!("could not configure TCP keepalive for {host}:{port}"))?;

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
    let authorization = client
        .authorization
        .as_deref()
        .map(|value| format!("Authorization: {value}\r\n"))
        .unwrap_or_default();
    let request = format!("CONNECT {path} HTTP/1.1\r\nHost: {host_header}\r\n{authorization}\r\n");
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

    Ok(stream)
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
