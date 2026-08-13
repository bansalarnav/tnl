use std::{
    io,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use rand::Rng;
use rustls::ServerConfig;
use rustls_acme::{AcmeConfig, EventError, EventOk, caches::DirCache, is_tls_alpn_challenge};
use tokio::{
    io::{AsyncWriteExt, copy_bidirectional},
    net::TcpStream,
    sync::watch,
    task::JoinSet,
};
use tokio_rustls::LazyConfigAcceptor;
use tokio_stream::StreamExt;
use url::Url;

use crate::TunnelId;

use super::{Client, ConnectionRequest, registration_error_is_fatal};

const ACME_ORDER_RETRY_DELAY: Duration = Duration::from_secs(60 * 60);
const RECONNECT_INITIAL_DELAY: Duration = Duration::from_secs(1);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);
const RECONNECT_BACKOFF_RESET_AFTER: Duration = Duration::from_secs(60);
const RECONNECT_JITTER_MAX_MILLIS: u64 = 250;

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

/// HTTPS tunnel forwarding built on top of the raw client session API.
#[derive(Clone)]
pub struct Forwarder {
    client: Client,
    cache_directory: PathBuf,
    event_handler: EventHandler,
}

impl Forwarder {
    /// Creates a forwarder with application-owned ACME cache storage.
    pub fn new(client: Client, cache_directory: PathBuf) -> Self {
        Self {
            client,
            cache_directory,
            event_handler: Arc::new(|_| {}),
        }
    }

    /// Sets a synchronous handler for forwarding lifecycle events.
    pub fn with_event_handler(
        mut self,
        handler: impl Fn(ClientEvent) + Send + Sync + 'static,
    ) -> Self {
        self.event_handler = Arc::new(handler);
        self
    }

    /// Publishes a local TCP target through the named HTTPS tunnel.
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

async fn expose(forwarder: Arc<Forwarder>, target: SocketAddr, tunnel_id: TunnelId) -> Result<()> {
    let mut endpoint_tls = None;
    let mut has_registered = false;
    let mut reconnect_delay = RECONNECT_INITIAL_DELAY;
    let mut background_tasks = JoinSet::new();

    loop {
        let session_started = Instant::now();
        let result = run_control_session(
            Arc::clone(&forwarder),
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
        forwarder.emit(ClientEvent::Disconnected {
            error: format!("{error:#}"),
            retry_in,
        });
        tokio::time::sleep(retry_in).await;
        reconnect_delay = (reconnect_delay * 2).min(RECONNECT_MAX_DELAY);
    }
}

async fn run_control_session(
    forwarder: Arc<Forwarder>,
    tunnel_id: &TunnelId,
    target: SocketAddr,
    endpoint_tls: &mut Option<EndpointTls>,
    has_registered: &mut bool,
    background_tasks: &mut JoinSet<()>,
) -> Result<()> {
    let mut session = forwarder.client.register(tunnel_id).await?;
    *has_registered = true;
    let url = session.wait_until_ready().await?.to_owned();
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
        forwarder.emit(ClientEvent::Reconnected { url });
    } else {
        forwarder.emit(ClientEvent::ObtainingCertificate {
            hostname: hostname.clone(),
        });
        *endpoint_tls = Some(start_endpoint_tls(
            &forwarder,
            hostname,
            url,
            target,
            background_tasks,
        )?);
    }

    loop {
        let request = session.accept().await?;
        let connection_id = request.id().to_owned();
        let endpoint_tls = endpoint_tls
            .clone()
            .context("server sent a connection before the tunnel was ready")?;
        let forwarder = Arc::clone(&forwarder);
        while background_tasks.try_join_next().is_some() {}
        background_tasks.spawn(async move {
            if let Err(error) = forward_connection(request, endpoint_tls, target).await
                && !is_routine_connection_error(&error)
            {
                forwarder.emit(ClientEvent::ConnectionFailed {
                    connection_id,
                    error: format!("{error:#}"),
                });
            }
        });
    }
}

async fn forward_connection(
    request: ConnectionRequest,
    mut endpoint_tls: EndpointTls,
    target: SocketAddr,
) -> Result<()> {
    let tunnel_stream = request.connect().await?;
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
    forwarder: &Arc<Forwarder>,
    hostname: String,
    url: String,
    target: SocketAddr,
    background_tasks: &mut JoinSet<()>,
) -> Result<EndpointTls> {
    let cache_directory = forwarder.cache_directory.clone();
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

    let forwarder = Arc::clone(forwarder);
    background_tasks.spawn(async move {
        while let Some(event) = state.next().await {
            match event {
                Ok(EventOk::DeployedCachedCert | EventOk::DeployedNewCert) => {
                    certificate_ready.send_replace(true);
                    forwarder.emit(ClientEvent::Ready {
                        url: url.clone(),
                        target,
                    });
                }
                Ok(EventOk::CertCacheStore | EventOk::AccountCacheStore) => {}
                Err(error) => {
                    let retry_in =
                        matches!(error, EventError::Order(_)).then_some(ACME_ORDER_RETRY_DELAY);
                    forwarder.emit(ClientEvent::CertificateError {
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
