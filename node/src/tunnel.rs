//! Keeps a tunnel registered and dispatches its visitors to a handler.

use std::{
    collections::HashMap,
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use anyhow::{Context as _, Error, Result, bail};
use rand::Rng;
use tnl_core::{
    Session, SessionConfig, SidebandStream, Stream, TunnelId, data_transport, net,
    protocol::{ACTIVATION_MARKER, DEMAND_TAG, FORWARD_TAG, MAX_DATA_TRANSPORTS, parse_sideband_tag},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::TcpStream,
    sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot},
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
const INITIAL_READ_BUF_SIZE: usize = 2048;

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
        let target_idle = first.transport_pool;
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
        let pool = Arc::new(ElasticPool::new(target_idle));
        // Dropping these sets aborts the remaining sessions and workers.
        let mut session_tasks = JoinSet::new();
        for session in sessions {
            session_tasks.spawn(serve_session(
                session,
                Arc::clone(handler),
                Arc::clone(&sidebands),
                Arc::clone(&pool),
            ));
        }
        let mut supervisor = JoinSet::new();
        supervisor.spawn(supervise_data_transports(
            Arc::clone(&self.client),
            self.id.clone(),
            Arc::clone(&pool),
            Arc::clone(handler),
            Arc::clone(&sidebands),
        ));

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
    pool: Arc<ElasticPool>,
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
        if stream.tag() == DEMAND_TAG {
            let pool = Arc::clone(&pool);
            while visitors.try_join_next().is_some() {}
            visitors.spawn(async move {
                serve_demand_stream(stream, pool).await;
            });
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

async fn serve_demand_stream(mut stream: Stream, pool: Arc<ElasticPool>) {
    let mut buf = [0_u8; 2];
    while stream.read_exact(&mut buf).await.is_ok() {
        let waiting = usize::from(u16::from_be_bytes(buf));
        pool.record_demand(waiting);
    }
}

struct ElasticPool {
    target_idle: usize,
    idle_count: AtomicUsize,
    opening_count: AtomicUsize,
    /// Relay-reported number of visitors currently blocked waiting for a
    /// data transport, enabling single-wave elastic expansion over WAN links.
    demand_count: AtomicUsize,
    replenish: Notify,
}

impl ElasticPool {
    fn new(target_idle: usize) -> Self {
        Self {
            target_idle: target_idle.max(1),
            idle_count: AtomicUsize::new(0),
            opening_count: AtomicUsize::new(0),
            demand_count: AtomicUsize::new(0),
            replenish: Notify::new(),
        }
    }

    fn record_demand(&self, waiting: usize) {
        self.demand_count.fetch_max(waiting, Ordering::AcqRel);
        if self.deficit() > 0 {
            self.replenish.notify_one();
        }
    }

    #[inline]
    fn deficit(&self) -> usize {
        let idle = self.idle_count.load(Ordering::Acquire);
        let opening = self.opening_count.load(Ordering::Acquire);
        let demand = self.demand_count.load(Ordering::Acquire);
        let desired = if demand > 0 {
            // Open enough transports in one parallel wave to satisfy all
            // waiting visitors plus the warm idle floor.
            demand.saturating_add(self.target_idle).min(MAX_DATA_TRANSPORTS)
        } else {
            self.target_idle
        };
        desired.saturating_sub(idle + opening)
    }

    #[inline]
    fn maybe_notify_replenish(&self) {
        if self.deficit() > 0 {
            self.replenish.notify_one();
        }
    }
}

struct ReplenishOnDrop(Arc<ElasticPool>);

impl Drop for ReplenishOnDrop {
    fn drop(&mut self) {
        self.0.maybe_notify_replenish();
    }
}

struct CounterGuard<'a>(&'a AtomicUsize);

impl<'a> CounterGuard<'a> {
    fn enter(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self(counter)
    }
}

impl Drop for CounterGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Maintains a lean warm idle pool (`target_idle`) of reusable data transports
/// that expands elastically up to `MAX_DATA_TRANSPORTS` during bursts and
/// shrinks automatically when the relay closes excess idle transports.
async fn supervise_data_transports<H: Handler>(
    client: Arc<Client>,
    tunnel_id: TunnelId,
    pool: Arc<ElasticPool>,
    handler: Arc<H>,
    sidebands: Arc<Sidebands>,
) {
    let permits = Arc::new(Semaphore::new(MAX_DATA_TRANSPORTS));
    let mut workers = JoinSet::new();

    loop {
        while workers.try_join_next().is_some() {}
        let needed = pool.deficit();
        if needed > 0 {
            let mut spawned = 0;
            for _ in 0..needed {
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                    break;
                };
                pool.opening_count.fetch_add(1, Ordering::AcqRel);
                spawned += 1;
                workers.spawn(run_data_transport(
                    Arc::clone(&client),
                    tunnel_id.clone(),
                    permit,
                    Arc::clone(&pool),
                    Arc::clone(&handler),
                    Arc::clone(&sidebands),
                ));
            }
            if spawned > 0 {
                // Consume the satisfied portion of relay-reported demand.
                let _ = pool.demand_count.fetch_update(
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    |d| Some(d.saturating_sub(spawned)),
                );
            } else {
                pool.replenish.notified().await;
            }
            continue;
        }
        pool.replenish.notified().await;
    }
}

async fn run_data_transport<H: Handler>(
    client: Arc<Client>,
    tunnel_id: TunnelId,
    permit: OwnedSemaphorePermit,
    pool: Arc<ElasticPool>,
    handler: Arc<H>,
    sidebands: Arc<Sidebands>,
) {
    // Drop order is reverse of declaration: `_permit` releases the semaphore
    // slot first, then `_notify` wakes the supervisor if `idle < target_idle`.
    let _notify = ReplenishOnDrop(Arc::clone(&pool));
    let _permit = permit;

    struct OpeningRelease<'a>(&'a AtomicUsize, bool);
    impl Drop for OpeningRelease<'_> {
        fn drop(&mut self) {
            if self.1 {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }
    let mut opening = OpeningRelease(&pool.opening_count, true);

    let opened = open_data_transport(&client, &tunnel_id, &sidebands).await;
    opening.1 = false;
    pool.opening_count.fetch_sub(1, Ordering::AcqRel);

    let (mut data, mut sideband) = match opened {
        Ok(pair) => pair,
        Err(error) => {
            log_error("data transport failed", error.as_ref());
            tokio::time::sleep(TRANSPORT_RETRY_DELAY).await;
            return;
        }
    };

    if let Err(error) = serve_visitors(&mut data, &mut sideband, &*handler, &pool).await {
        log_error("data transport failed", error.as_ref());
    }
}

async fn open_data_transport(
    client: &Client,
    tunnel_id: &TunnelId,
    sidebands: &Arc<Sidebands>,
) -> Result<(TcpStream, Stream)> {
    let (mut data, transport_id) = client.request_data_transport(tunnel_id).await?;
    let sideband = {
        let (sideband, _expectation) = sidebands.expect(transport_id)?;
        data_transport::confirm(&mut data).await?;
        sideband
            .await
            .context("control sessions closed while adding a data transport")?
    };
    Ok((data, sideband))
}

/// Serves visitors one after another over a reusable data transport.
async fn serve_visitors<H: Handler>(
    data: &mut TcpStream,
    sideband: &mut Stream,
    handler: &H,
    pool: &ElasticPool,
) -> Result<()> {
    let mut initial = [0_u8; INITIAL_READ_BUF_SIZE];
    loop {
        let idle_guard = CounterGuard::enter(&pool.idle_count);
        let initial_len = match read_activation_and_initial(data, &mut initial).await {
            Ok(Some(len)) => len,
            // Clean EOF while idle (for example, relay closed an excess
            // post-burst transport). Exit cleanly without logging or sleeping.
            Ok(None) => return Ok(()),
            Err(error) => return Err(error),
        };
        drop(idle_guard);
        pool.maybe_notify_replenish();

        let prefixed = PrefixedRead::new(&initial[ACTIVATION_MARKER.len()..initial_len], &mut *data);
        let mut visitor = SidebandStream::new(prefixed, &mut *sideband);
        handler
            .handle(Visitor::new(&mut visitor))
            .await
            .map_err(|error| anyhow::anyhow!(error))?;
        if !visitor.is_read_finished() {
            drain_to_eof(&mut visitor).await?;
        }
        visitor.shutdown().await?;
        visitor.finish().await?;
    }
}

/// Reads `[ACTIVATION_MARKER | initial visitor bytes]` in a single `read()`
/// syscall when the relay coalesces activation with the visitor ClientHello.
/// Returns `Ok(None)` on clean EOF before any activation byte arrives.
async fn read_activation_and_initial(
    data: &mut TcpStream,
    buf: &mut [u8],
) -> Result<Option<usize>> {
    let first = data
        .read(buf)
        .await
        .context("relay closed an idle data transport")?;
    if first == 0 {
        return Ok(None);
    }
    let mut len = first;
    while len < ACTIVATION_MARKER.len() {
        let n = data
            .read(&mut buf[len..ACTIVATION_MARKER.len()])
            .await
            .context("relay closed an idle data transport")?;
        if n == 0 {
            bail!("relay closed an idle data transport mid-marker");
        }
        len += n;
    }
    if &buf[..ACTIVATION_MARKER.len()] != ACTIVATION_MARKER {
        bail!("relay sent an invalid data transport activation marker");
    }
    Ok(Some(len))
}

async fn drain_to_eof<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<()> {
    let mut discard = [0_u8; 4096];
    while reader.read(&mut discard).await? != 0 {}
    Ok(())
}

/// Yields already-read initial bytes before delegating reads to `inner`.
struct PrefixedRead<'a, S> {
    prefix: &'a [u8],
    offset: usize,
    inner: S,
}

impl<'a, S> PrefixedRead<'a, S> {
    fn new(prefix: &'a [u8], inner: S) -> Self {
        Self {
            prefix,
            offset: 0,
            inner,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for PrefixedRead<'_, S> {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.offset < this.prefix.len() {
            let remaining = &this.prefix[this.offset..];
            let take = remaining.len().min(buffer.remaining());
            buffer.put_slice(&remaining[..take]);
            this.offset += take;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.inner).poll_read(context, buffer)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for PrefixedRead<'_, S> {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(context, buffer)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(context, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(context)
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
