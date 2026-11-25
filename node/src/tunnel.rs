//! Keeps a tunnel registered and dispatches its visitors to a handler.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Error, Result, bail};
use rand::Rng;
use tnl_core::{
    Session, SessionConfig, SidebandStream, Stream, TunnelId, data_transport, net,
    protocol::{ACTIVATION_MARKER, FORWARD_TAG, parse_sideband_tag},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::oneshot,
    task::JoinSet,
    time::Instant,
};
use tracing::{info, warn};
use url::Url;

use crate::{
    Handler, NodeConfig, Visitor,
    client::{Client, Registration, Rejection},
};

const RECONNECT_INITIAL_DELAY: Duration = Duration::from_secs(1);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);
/// A connection that lasted this long resets the reconnect delay.
const RECONNECT_RESET_AFTER: Duration = Duration::from_secs(60);
const RECONNECT_JITTER_MAX: Duration = Duration::from_millis(250);
const TRANSPORT_RETRY_DELAY: Duration = Duration::from_secs(1);

/// A tunnel registered with a relay.
pub struct Tunnel {
    client: Arc<Client>,
    id: TunnelId,
    url: String,
    hostname: String,
    session: SessionConfig,
    first: Option<Registration>,
}

impl Tunnel {
    /// Registers `id` with the relay, retrying until it succeeds or the relay
    /// rejects the request.
    pub async fn register(config: NodeConfig, id: TunnelId) -> crate::Result<Self> {
        let client = Arc::new(Client::new(config));
        let mut backoff = Backoff::default();
        let registration = loop {
            match client
                .register(&id)
                .await
                .with_context(|| format!("could not register tunnel {id}"))
            {
                Ok(registration) => break registration,
                Err(error) if is_fatal(&error, false) => return Err(error.into()),
                Err(error) => backoff.wait(&error).await,
            }
        };
        let hostname = hostname(&registration.url)?;
        Ok(Self {
            client,
            id,
            url: registration.url.clone(),
            hostname,
            session: SessionConfig::default(),
            first: Some(registration),
        })
    }

    /// Sets how control sessions with the relay behave.
    pub fn session_config(mut self, config: SessionConfig) -> Self {
        self.session = config;
        self
    }

    pub fn id(&self) -> &TunnelId {
        &self.id
    }

    /// Returns the tunnel's public URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the hostname of the tunnel's public URL.
    pub fn hostname(&self) -> &str {
        &self.hostname
    }

    /// Serves visitors with `handler`, reconnecting whenever the connection to
    /// the relay fails. Returns only once reconnecting is pointless.
    pub async fn serve(mut self, handler: impl Handler) -> crate::Error {
        let handler = Arc::new(handler);
        let mut backoff = Backoff::default();
        loop {
            let started = Instant::now();
            let error = match self.first.take() {
                Some(registration) => self.run(registration, &handler).await,
                None => match self.reregister().await {
                    Ok(registration) => {
                        info!("Reconnected {}", self.url);
                        self.run(registration, &handler).await
                    }
                    Err(error) => error,
                },
            };
            if is_fatal(&error, true) {
                return error.into();
            }
            if started.elapsed() >= RECONNECT_RESET_AFTER {
                backoff = Backoff::default();
            }
            backoff.wait(&error).await;
        }
    }

    async fn reregister(&self) -> Result<Registration> {
        let registration = self
            .client
            .register(&self.id)
            .await
            .with_context(|| format!("could not register tunnel {}", self.id))?;
        self.check_hostname(&registration)?;
        Ok(registration)
    }

    fn check_hostname(&self, registration: &Registration) -> Result<()> {
        let hostname = hostname(&registration.url)?;
        if hostname != self.hostname {
            bail!(
                "tunnel hostname changed from {} to {hostname}",
                self.hostname
            );
        }
        Ok(())
    }

    /// Serves one registration until any of its control sessions fails.
    async fn run<H: Handler>(&self, first: Registration, handler: &Arc<H>) -> Error {
        match self.run_sessions(first, handler).await {
            Ok(never) => match never {},
            Err(error) => error,
        }
    }

    async fn run_sessions<H: Handler>(
        &self,
        first: Registration,
        handler: &Arc<H>,
    ) -> Result<std::convert::Infallible> {
        let control_sessions = first.control_sessions;
        let transport_pool = first.transport_pool;
        let mut sessions = vec![Session::client(first.stream, self.session).await?];
        for _ in 1..control_sessions {
            let registration = self
                .client
                .register(&self.id)
                .await
                .with_context(|| format!("could not open another session for {}", self.id))?;
            self.check_hostname(&registration)?;
            sessions.push(Session::client(registration.stream, self.session).await?);
        }

        let sidebands = Arc::new(Sidebands::default());
        // Dropping these sets aborts the remaining sessions and workers.
        let mut session_tasks = JoinSet::new();
        for session in sessions {
            session_tasks.spawn(serve_session(
                session,
                Arc::clone(handler),
                Arc::clone(&sidebands),
            ));
        }
        let mut transport_tasks = JoinSet::new();
        for _ in 0..transport_pool {
            transport_tasks.spawn(serve_transports(
                Arc::clone(&self.client),
                self.id.clone(),
                Arc::clone(handler),
                Arc::clone(&sidebands),
            ));
        }

        session_tasks
            .join_next()
            .await
            .context("tunnel did not start any control sessions")?
            .context("control session task failed")?
    }
}

fn hostname(url: &str) -> Result<String> {
    Ok(Url::parse(url)
        .context("relay returned an invalid tunnel URL")?
        .host_str()
        .context("relay tunnel URL did not contain a hostname")?
        .to_owned())
}

/// Client errors other than a conflict after a successful registration mean
/// retrying cannot help. A conflict usually means the relay has not yet
/// noticed the previous connection dropped.
fn is_fatal(error: &Error, has_registered: bool) -> bool {
    error.downcast_ref::<Rejection>().is_some_and(|rejection| {
        (400..500).contains(&rejection.status) && !(rejection.status == 409 && has_registered)
    })
}

struct Backoff {
    delay: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            delay: RECONNECT_INITIAL_DELAY,
        }
    }
}

impl Backoff {
    async fn wait(&mut self, error: &Error) {
        let jitter = rand::thread_rng().gen_range(Duration::ZERO..=RECONNECT_JITTER_MAX);
        let wait = self.delay + jitter;
        warn!(
            "Tunnel connection lost: {error:#}. Reconnecting in {:.1}s...",
            wait.as_secs_f32()
        );
        tokio::time::sleep(wait).await;
        self.delay = (self.delay * 2).min(RECONNECT_MAX_DELAY);
    }
}

async fn serve_session<H: Handler>(
    mut session: Session,
    handler: Arc<H>,
    sidebands: Arc<Sidebands>,
) -> Result<std::convert::Infallible> {
    let mut visitors = JoinSet::new();
    loop {
        let mut stream = session.accept().await?;
        if let Some(transport_id) = parse_sideband_tag(stream.tag()) {
            let transport_id = transport_id.context("relay sent an invalid sideband tag")?;
            if !sidebands.deliver(transport_id, stream) {
                bail!("relay sent a sideband for unknown data transport {transport_id}");
            }
            continue;
        }
        if stream.tag() != FORWARD_TAG {
            warn!("ignoring stream with unsupported tag {}", stream.tag());
            continue;
        }
        let handler = Arc::clone(&handler);
        while visitors.try_join_next().is_some() {}
        visitors.spawn(async move {
            if let Err(error) = handler.handle(Visitor::new(&mut stream)).await {
                log_error("tunnel connection failed", &*error);
            }
        });
    }
}

/// Keeps one data transport open, replacing it whenever it fails.
async fn serve_transports<H: Handler>(
    client: Arc<Client>,
    tunnel_id: TunnelId,
    handler: Arc<H>,
    sidebands: Arc<Sidebands>,
) {
    loop {
        if let Err(error) = serve_transport(&client, &tunnel_id, &*handler, &sidebands).await {
            log_error("data transport failed", error.as_ref());
        }
        tokio::time::sleep(TRANSPORT_RETRY_DELAY).await;
    }
}

async fn serve_transport<H: Handler>(
    client: &Client,
    tunnel_id: &TunnelId,
    handler: &H,
    sidebands: &Arc<Sidebands>,
) -> Result<()> {
    let (mut data, transport_id) = client.request_data_transport(tunnel_id).await?;
    let mut sideband = {
        let (sideband, _expectation) = sidebands.expect(transport_id)?;
        data_transport::confirm(&mut data).await?;
        sideband
            .await
            .context("control sessions closed while adding a data transport")?
    };
    serve_visitors(&mut data, &mut sideband, handler).await
}

/// Serves visitors one after another over a data transport.
async fn serve_visitors<H: Handler>(
    data: &mut TcpStream,
    sideband: &mut Stream,
    handler: &H,
) -> Result<()> {
    loop {
        let mut marker = [0; ACTIVATION_MARKER.len()];
        data.read_exact(&mut marker)
            .await
            .context("relay closed an idle data transport")?;
        if &marker != ACTIVATION_MARKER {
            bail!("relay sent an invalid data transport activation marker");
        }

        let mut visitor = SidebandStream::new(&mut *data, &mut *sideband);
        handler
            .handle(Visitor::new(&mut visitor))
            .await
            .map_err(|error| anyhow::anyhow!(error))?;
        tokio::io::copy(&mut visitor, &mut tokio::io::sink()).await?;
        visitor.shutdown().await?;
        visitor.finish().await?;
    }
}

fn log_error(message: &str, error: &(dyn std::error::Error + 'static)) {
    if !net::is_routine_disconnect(error) {
        let causes: Vec<String> = anyhow::Chain::new(error).map(ToString::to_string).collect();
        warn!("{message}: {}", causes.join(": "));
    }
}

/// Routes sideband streams to the data transports waiting for them.
#[derive(Default)]
struct Sidebands {
    waiting: Mutex<HashMap<u64, oneshot::Sender<Stream>>>,
}

/// Stops waiting for a sideband when dropped.
struct Expectation<'a> {
    sidebands: &'a Sidebands,
    transport_id: u64,
}

impl Sidebands {
    fn expect(&self, transport_id: u64) -> Result<(oneshot::Receiver<Stream>, Expectation<'_>)> {
        let (sender, receiver) = oneshot::channel();
        let previous = self
            .waiting
            .lock()
            .expect("sideband lock was poisoned")
            .insert(transport_id, sender);
        if previous.is_some() {
            bail!("relay reused data transport identifier {transport_id}");
        }
        Ok((
            receiver,
            Expectation {
                sidebands: self,
                transport_id,
            },
        ))
    }

    fn deliver(&self, transport_id: u64, stream: Stream) -> bool {
        self.waiting
            .lock()
            .expect("sideband lock was poisoned")
            .remove(&transport_id)
            .is_some_and(|sender| sender.send(stream).is_ok())
    }
}

impl Drop for Expectation<'_> {
    fn drop(&mut self) {
        self.sidebands
            .waiting
            .lock()
            .expect("sideband lock was poisoned")
            .remove(&self.transport_id);
    }
}
