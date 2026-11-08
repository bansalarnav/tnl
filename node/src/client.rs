//! Connections from a node to its relay's API and data transport port.

use std::{error::Error, fmt, net::SocketAddr, sync::Arc};

use anyhow::{Context, Result, bail};
use rustls::{
    ClientConfig, RootCertStore,
    crypto::{CryptoProvider, ring},
    pki_types::ServerName,
};
use tnl_core::{
    TunnelId,
    data_transport::{self, TransportKey},
    net,
    protocol::{
        CONTROL_SESSIONS_HEADER, MAX_CONTROL_SESSIONS, MAX_DATA_TRANSPORTS, PROTOCOL_VERSION,
        PROTOCOL_VERSION_HEADER, TRANSPORT_POOL_HEADER, TUNNEL_URL_HEADER, tunnel_path,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::{TlsConnector, client::TlsStream};
use url::{Host, Url};

const MAX_RESPONSE_HEADER_LENGTH: usize = 16 * 1024;

/// How a node reaches and authenticates to its relay.
#[derive(Clone)]
pub struct NodeConfig {
    api_url: Url,
    token: String,
    connect_addr: Option<SocketAddr>,
    tls: Arc<ClientConfig>,
}

impl NodeConfig {
    /// Uses the relay API at the `https` URL `api_url`, authenticating with
    /// `token`. The relay's certificate is checked against the Mozilla roots.
    pub fn new(api_url: Url, token: impl Into<String>) -> crate::Result<Self> {
        let token = token.into();
        if api_url.scheme() != "https" || api_url.host().is_none() {
            return Err(anyhow::anyhow!("relay API URL must be an HTTPS URL with a host").into());
        }
        if token.is_empty() || token.chars().any(char::is_control) {
            return Err(anyhow::anyhow!("invalid relay API token").into());
        }
        let roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls = ClientConfig::builder_with_provider(crypto_provider())
            .with_safe_default_protocol_versions()
            .map_err(anyhow::Error::from)?
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Self {
            api_url,
            token,
            connect_addr: None,
            tls: Arc::new(tls),
        })
    }

    /// Connects to `address` instead of resolving the API URL's host.
    pub fn connect_addr(mut self, address: SocketAddr) -> Self {
        self.connect_addr = Some(address);
        self
    }

    /// Verifies the relay with `config` instead of the Mozilla roots.
    pub fn tls_config(mut self, config: Arc<ClientConfig>) -> Self {
        self.tls = config;
        self
    }
}

/// Prefers AES-128-GCM, which is cheapest on hardware with AES instructions.
fn crypto_provider() -> Arc<CryptoProvider> {
    let mut provider = ring::default_provider();
    provider.cipher_suites.sort_by_key(|suite| {
        usize::from(suite.suite() != rustls::CipherSuite::TLS13_AES_128_GCM_SHA256)
    });
    Arc::new(provider)
}

pub(crate) type ControlStream = TlsStream<TcpStream>;

/// A control session connection the relay accepted for a tunnel.
pub(crate) struct Registration {
    pub stream: ControlStream,
    pub url: String,
    pub control_sessions: usize,
    pub transport_pool: usize,
}

/// The relay refused an API request.
#[derive(Debug)]
pub(crate) struct Rejection {
    pub status: u16,
    status_line: String,
}

impl fmt::Display for Rejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "relay rejected the request ({})",
            self.status_line
        )
    }
}

impl Error for Rejection {}

pub(crate) struct Client {
    config: NodeConfig,
    transport_key: TransportKey,
}

impl Client {
    pub fn new(config: NodeConfig) -> Self {
        Self {
            transport_key: TransportKey::from_token(&config.token),
            config,
        }
    }

    pub async fn register(&self, tunnel_id: &TunnelId) -> Result<Registration> {
        let (stream, header) = self.connect(&tunnel_path(tunnel_id)).await?;
        let url = header_value(&header, TUNNEL_URL_HEADER)
            .filter(|value| !value.is_empty())
            .with_context(|| format!("relay response did not include {TUNNEL_URL_HEADER}"))?
            .to_owned();
        Ok(Registration {
            stream,
            url,
            control_sessions: header_count(&header, CONTROL_SESSIONS_HEADER, MAX_CONTROL_SESSIONS)?,
            transport_pool: header_count(&header, TRANSPORT_POOL_HEADER, MAX_DATA_TRANSPORTS)?,
        })
    }

    /// Opens a data transport and completes the handshake up to the point
    /// where the relay has assigned it an identifier.
    pub async fn request_data_transport(&self, tunnel_id: &TunnelId) -> Result<(TcpStream, u64)> {
        let mut stream = self.connect_tcp().await?;
        let id = data_transport::request(&mut stream, tunnel_id, &self.transport_key)
            .await
            .context("data transport handshake failed")?;
        Ok((stream, id))
    }

    async fn connect_tcp(&self) -> Result<TcpStream> {
        let url = &self.config.api_url;
        let host = url.host_str().context("API URL does not contain a host")?;
        let port = url
            .port_or_known_default()
            .context("API URL does not contain a port")?;
        let stream = match self.config.connect_addr {
            Some(address) => TcpStream::connect(address)
                .await
                .with_context(|| format!("could not connect to {address}"))?,
            None => TcpStream::connect((host, port))
                .await
                .with_context(|| format!("could not connect to {host}:{port}"))?,
        };
        net::configure_tcp_stream(&stream).context("could not configure the TCP socket")?;
        Ok(stream)
    }

    /// Sends an authenticated `CONNECT` request and returns the upgraded
    /// connection with the response header.
    async fn connect(&self, path: &str) -> Result<(ControlStream, String)> {
        let url = &self.config.api_url;
        let host = url.host_str().context("API URL does not contain a host")?;
        let port = url
            .port_or_known_default()
            .context("API URL does not contain a port")?;
        let server_name = ServerName::try_from(host.to_owned()).context("invalid API hostname")?;
        let tcp = self.connect_tcp().await?;
        let mut stream = TlsConnector::from(Arc::clone(&self.config.tls))
            .connect(server_name, tcp)
            .await
            .context("could not establish TLS with the relay")?;

        let host_header = match url.host() {
            Some(Host::Ipv6(address)) => format!("[{address}]:{port}"),
            _ => format!("{host}:{port}"),
        };
        let request = format!(
            "CONNECT {path} HTTP/1.1\r\nHost: {host_header}\r\nAuthorization: Bearer {}\r\n{PROTOCOL_VERSION_HEADER}: {PROTOCOL_VERSION}\r\n\r\n",
            self.config.token
        );
        stream.write_all(request.as_bytes()).await?;

        let header = read_response_header(&mut stream).await?;
        let status_line = header
            .lines()
            .next()
            .context("relay returned an empty HTTP response")?;
        let status = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|value| value.parse::<u16>().ok())
            .context("relay returned an invalid HTTP status")?;
        if !(200..300).contains(&status) {
            return Err(Rejection {
                status,
                status_line: status_line.to_owned(),
            }
            .into());
        }
        check_protocol_version(&header)?;
        Ok((stream, header))
    }
}

async fn read_response_header(stream: &mut ControlStream) -> Result<String> {
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        if header.len() == MAX_RESPONSE_HEADER_LENGTH {
            bail!("relay HTTP response header is too large");
        }
        // Read byte by byte so nothing after the header is consumed.
        header.push(
            stream
                .read_u8()
                .await
                .context("relay closed the connection during the HTTP response")?,
        );
    }
    String::from_utf8(header).context("relay returned a non-UTF-8 HTTP response")
}

fn header_value<'a>(header: &'a str, name: &str) -> Option<&'a str> {
    header
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.trim())
}

fn header_count(header: &str, name: &str, maximum: usize) -> Result<usize> {
    header_value(header, name)
        .with_context(|| format!("relay response did not include {name}"))?
        .parse::<usize>()
        .ok()
        .filter(|count| (1..=maximum).contains(count))
        .with_context(|| format!("relay returned an invalid {name}"))
}

fn check_protocol_version(header: &str) -> Result<()> {
    let version = header_value(header, PROTOCOL_VERSION_HEADER)
        .context("relay did not declare its protocol version")?;
    if version != PROTOCOL_VERSION {
        bail!("relay uses unsupported protocol version {version}; expected {PROTOCOL_VERSION}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{check_protocol_version, header_count};

    #[test]
    fn requires_current_protocol_version() {
        assert!(check_protocol_version("X-Tnl-Protocol-Version: 6\r\n").is_ok());
        assert!(check_protocol_version("X-Tnl-Protocol-Version: 5\r\n").is_err());
        assert!(check_protocol_version("").is_err());
    }

    #[test]
    fn requires_valid_capability_counts() {
        let header = "X-Tnl-Control-Sessions: 8\r\n";
        assert_eq!(
            header_count(header, "X-Tnl-Control-Sessions", 8).unwrap(),
            8
        );
        assert!(header_count(header, "X-Tnl-Control-Sessions", 4).is_err());
        assert!(header_count("", "X-Tnl-Control-Sessions", 8).is_err());
    }
}
