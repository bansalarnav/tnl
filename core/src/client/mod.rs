#[cfg(feature = "forwarding")]
mod forward;

use std::{error::Error, fmt, io, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use socket2::{SockRef, TcpKeepalive};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Lines, ReadHalf, WriteHalf},
    net::TcpStream,
};
use tokio_rustls::{TlsConnector, client::TlsStream};
use url::{Host, Url};

use crate::{TunnelId, protocol::ServerControlMessage};

#[cfg(feature = "forwarding")]
pub use forward::{ClientEvent, Forwarder};

const MAX_HTTP_RESPONSE_HEADER_LENGTH: usize = 16 * 1024;
const TCP_KEEPALIVE_IDLE: Duration = Duration::from_secs(30);
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const TCP_KEEPALIVE_RETRIES: u32 = 3;
const PONG_MESSAGE: &[u8] = b"{\"type\":\"pong\"}\n";

type ApiStream = TlsStream<TcpStream>;

#[derive(Clone)]
pub struct Client {
    api_url: Url,
    authorization: Option<Arc<str>>,
    tls_config: Arc<ClientConfig>,
}

impl Client {
    /// Creates a client that trusts the public WebPKI root certificates.
    pub fn new(api_url: Url) -> Result<Self> {
        if api_url.scheme() != "https" || api_url.host().is_none() {
            bail!("API URL must be an HTTPS URL with a host");
        }

        let roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls_config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();

        Ok(Self {
            api_url,
            authorization: None,
            tls_config: Arc::new(tls_config),
        })
    }

    /// Adds the complete value for the HTTP `Authorization` header.
    pub fn with_authorization(mut self, authorization: impl Into<Arc<str>>) -> Result<Self> {
        let authorization = authorization.into();
        if authorization.is_empty() || authorization.chars().any(char::is_control) {
            bail!("authorization must not be empty or contain control characters");
        }
        self.authorization = Some(authorization);
        Ok(self)
    }

    /// Replaces the default TLS configuration, for example to use a private CA or mTLS.
    pub fn with_tls_config(mut self, tls_config: Arc<ClientConfig>) -> Self {
        self.tls_config = tls_config;
        self
    }

    /// Registers a named session with the central server.
    pub async fn register(&self, tunnel_id: &TunnelId) -> Result<ClientSession> {
        let control_stream = open_api_connection(self, &format!("/v1/tunnels/{tunnel_id}"))
            .await
            .with_context(|| format!("could not register tunnel {tunnel_id}"))?;
        let (reader, writer) = tokio::io::split(control_stream);

        Ok(ClientSession {
            client: self.clone(),
            public_url: None,
            messages: BufReader::new(reader).lines(),
            writer,
        })
    }
}

/// A registered control session that receives central-initiated connection requests.
pub struct ClientSession {
    client: Client,
    public_url: Option<String>,
    messages: Lines<BufReader<ReadHalf<ApiStream>>>,
    writer: WriteHalf<ApiStream>,
}

impl ClientSession {
    /// Waits for registration to complete and returns the server-provided ready value.
    pub async fn wait_until_ready(&mut self) -> Result<&str> {
        if self.public_url.is_none() {
            loop {
                match self.next_message().await? {
                    ServerControlMessage::Ready { url } => {
                        self.public_url = Some(url);
                        break;
                    }
                    ServerControlMessage::Connection { .. } => {
                        bail!("server sent a connection before the tunnel was ready");
                    }
                    ServerControlMessage::Ping => self.answer_heartbeat().await?,
                }
            }
        }

        Ok(self
            .public_url
            .as_deref()
            .expect("public URL was set above"))
    }

    /// Waits for the next connection requested by the central server.
    pub async fn accept(&mut self) -> Result<ConnectionRequest> {
        self.wait_until_ready().await?;
        loop {
            match self.next_message().await? {
                ServerControlMessage::Ready { .. } => {
                    bail!("server sent more than one ready message");
                }
                ServerControlMessage::Connection { id } => {
                    return Ok(ConnectionRequest {
                        id,
                        client: self.client.clone(),
                    });
                }
                ServerControlMessage::Ping => self.answer_heartbeat().await?,
            }
        }
    }

    async fn next_message(&mut self) -> Result<ServerControlMessage> {
        let line = self
            .messages
            .next_line()
            .await?
            .context("tunnel server closed the control connection")?;
        serde_json::from_str(&line).context("invalid server message")
    }

    async fn answer_heartbeat(&mut self) -> Result<()> {
        self.writer
            .write_all(PONG_MESSAGE)
            .await
            .context("could not answer tunnel heartbeat")
    }
}

/// A central-initiated connection that has not been opened yet.
pub struct ConnectionRequest {
    id: String,
    client: Client,
}

impl ConnectionRequest {
    /// Returns the protocol connection identifier.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Opens the raw bidirectional connection to the central server.
    pub async fn connect(self) -> Result<ClientConnection> {
        open_api_connection(&self.client, &format!("/v1/connections/{}", self.id))
            .await
            .with_context(|| format!("could not open tunnel connection {}", self.id))
    }
}

/// The node side of a raw central-initiated connection.
pub type ClientConnection = TlsStream<TcpStream>;

#[derive(Debug)]
struct ApiRejection {
    #[cfg_attr(not(feature = "forwarding"), allow(dead_code))]
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

#[cfg(feature = "forwarding")]
fn registration_error_is_fatal(error: &anyhow::Error, has_registered: bool) -> bool {
    error
        .downcast_ref::<ApiRejection>()
        .is_some_and(|rejection| {
            (400..500).contains(&rejection.status) && !(rejection.status == 409 && has_registered)
        })
}

async fn open_api_connection(client: &Client, path: &str) -> Result<ApiStream> {
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

    let server_name = ServerName::try_from(host.to_owned()).context("invalid API hostname")?;
    let mut stream = TlsConnector::from(Arc::clone(&client.tls_config))
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
