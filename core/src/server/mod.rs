use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use rand::{Rng, distributions::Alphanumeric};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    sync::{mpsc, oneshot, watch},
    time::{Instant, MissedTickBehavior, interval_at, timeout},
};

use crate::{
    TunnelId,
    protocol::{ClientControlMessage, ServerControlMessage},
};

const DATA_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(60);

type ReadyValue = Arc<dyn Fn(&TunnelId) -> String + Send + Sync>;

/// Pairs registered nodes with central-initiated bidirectional connections.
pub struct Broker<D> {
    ready_value: ReadyValue,
    state: Arc<Mutex<State<D>>>,
    shutdown: watch::Sender<bool>,
}

struct State<D> {
    shutdown: bool,
    sessions: HashMap<String, Session>,
    pending_connections: HashMap<String, PendingConnection<D>>,
}

impl<D> Default for State<D> {
    fn default() -> Self {
        Self {
            shutdown: false,
            sessions: HashMap::new(),
            pending_connections: HashMap::new(),
        }
    }
}

struct Session {
    session_id: u64,
    owner: String,
    sender: mpsc::Sender<String>,
}

struct PendingConnection<D> {
    owner: String,
    sender: oneshot::Sender<D>,
}

struct PendingRequest<D> {
    connection_id: String,
    state: Arc<Mutex<State<D>>>,
}

impl<D> Drop for PendingRequest<D> {
    fn drop(&mut self) {
        self.state
            .lock()
            .expect("broker lock was poisoned")
            .pending_connections
            .remove(&self.connection_id);
    }
}

impl<D> Clone for Broker<D> {
    fn clone(&self) -> Self {
        Self {
            ready_value: Arc::clone(&self.ready_value),
            state: Arc::clone(&self.state),
            shutdown: self.shutdown.clone(),
        }
    }
}

impl<D> Broker<D> {
    /// Creates a broker whose ready value is the registered node ID.
    pub fn new() -> Self {
        Self::with_ready_value(|tunnel_id| tunnel_id.to_string())
    }

    /// Creates a broker with an application-defined ready value.
    pub fn with_ready_value(
        ready_value: impl Fn(&TunnelId) -> String + Send + Sync + 'static,
    ) -> Self {
        let (shutdown, _) = watch::channel(false);
        Self {
            ready_value: Arc::new(ready_value),
            state: Arc::new(Mutex::new(State::default())),
            shutdown,
        }
    }

    /// Registers a node after the transport adapter has authenticated it.
    pub fn register(
        &self,
        tunnel_id: TunnelId,
        owner: impl Into<String>,
    ) -> Option<Registration<D>> {
        let mut state = self.state.lock().expect("broker lock was poisoned");
        if state.shutdown || state.sessions.contains_key(tunnel_id.as_str()) {
            return None;
        }

        let session_id = rand::thread_rng().r#gen();
        let (sender, receiver) = mpsc::channel(32);
        state.sessions.insert(
            tunnel_id.to_string(),
            Session {
                session_id,
                owner: owner.into(),
                sender,
            },
        );

        Some(Registration {
            broker: self.clone(),
            tunnel_id,
            session_id,
            receiver,
        })
    }

    /// Claims a pending connection after the transport adapter authenticates it.
    pub fn claim(&self, connection_id: &str, owner: &str) -> Option<ConnectionAttachment<D>> {
        let mut state = self.state.lock().expect("broker lock was poisoned");
        if state.shutdown
            || state
                .pending_connections
                .get(connection_id)
                .is_none_or(|connection| connection.owner != owner)
        {
            return None;
        }

        let connection = state
            .pending_connections
            .remove(connection_id)
            .expect("pending connection was checked above");
        Some(ConnectionAttachment {
            sender: connection.sender,
        })
    }

    /// Requests a raw bidirectional connection from a registered node.
    pub async fn connect(&self, tunnel_id: &TunnelId) -> Result<Option<D>> {
        let connection_id: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(24)
            .map(char::from)
            .collect();
        let (data_sender, data_receiver) = oneshot::channel();
        let sender = {
            let mut state = self.state.lock().expect("broker lock was poisoned");
            if state.shutdown {
                bail!("broker is shut down");
            }
            let Some(session) = state.sessions.get(tunnel_id.as_str()) else {
                return Ok(None);
            };
            let owner = session.owner.clone();
            let sender = session.sender.clone();
            state.pending_connections.insert(
                connection_id.clone(),
                PendingConnection {
                    owner,
                    sender: data_sender,
                },
            );
            sender
        };
        let pending_request = PendingRequest {
            connection_id: connection_id.clone(),
            state: Arc::clone(&self.state),
        };

        if sender.send(connection_id).await.is_err() {
            bail!("node disconnected");
        }

        let stream = match timeout(DATA_CONNECTION_TIMEOUT, data_receiver).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(_)) => bail!("node did not establish the data connection"),
            Err(_) => bail!("timed out waiting for the node data connection"),
        };
        drop(pending_request);
        Ok(Some(stream))
    }

    /// Closes control sessions and cancels pending connection requests.
    pub fn shutdown(&self) {
        let mut state = self.state.lock().expect("broker lock was poisoned");
        state.shutdown = true;
        state.sessions.clear();
        state.pending_connections.clear();
        drop(state);
        self.shutdown.send_replace(true);
    }

    pub fn is_shutdown(&self) -> bool {
        self.state
            .lock()
            .expect("broker lock was poisoned")
            .shutdown
    }

    /// Completes when shutdown begins.
    pub async fn wait_for_shutdown(&self) {
        let mut shutdown = self.shutdown.subscribe();
        loop {
            if *shutdown.borrow() {
                return;
            }
            if shutdown.changed().await.is_err() {
                return;
            }
        }
    }
}

/// A validated pending connection waiting for its transport stream.
pub struct ConnectionAttachment<D> {
    sender: oneshot::Sender<D>,
}

impl<D> ConnectionAttachment<D> {
    /// Completes the pending connection with an established stream.
    pub fn attach(self, stream: D) -> Result<(), D> {
        self.sender.send(stream)
    }
}

impl<D> Default for Broker<D> {
    fn default() -> Self {
        Self::new()
    }
}

/// A registered node whose control protocol can run over any Tokio stream.
pub struct Registration<D> {
    broker: Broker<D>,
    tunnel_id: TunnelId,
    session_id: u64,
    receiver: mpsc::Receiver<String>,
}

impl<D> Registration<D> {
    pub async fn serve<S>(mut self, stream: S) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let (reader, mut writer) = tokio::io::split(stream);
        let mut reader = BufReader::new(reader);
        write_message(
            &mut writer,
            &ServerControlMessage::Ready {
                url: (self.broker.ready_value)(&self.tunnel_id),
            },
        )
        .await?;

        let mut heartbeat = interval_at(Instant::now() + HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut shutdown = self.broker.shutdown.subscribe();
        let mut last_pong = Instant::now();
        let mut line = String::new();

        loop {
            tokio::select! {
                result = reader.read_line(&mut line) => {
                    if result? == 0 {
                        break;
                    }
                    match serde_json::from_str::<ClientControlMessage>(&line)
                        .context("node sent an invalid control message")?
                    {
                        ClientControlMessage::Pong => last_pong = Instant::now(),
                    }
                    line.clear();
                }
                connection_id = self.receiver.recv() => {
                    let Some(connection_id) = connection_id else {
                        break;
                    };
                    write_message(
                        &mut writer,
                        &ServerControlMessage::Connection { id: connection_id },
                    )
                    .await?;
                }
                _ = heartbeat.tick() => {
                    if last_pong.elapsed() >= HEARTBEAT_TIMEOUT {
                        bail!("node missed the heartbeat deadline");
                    }
                    write_message(&mut writer, &ServerControlMessage::Ping).await?;
                }
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        break;
                    }
                },
            }
        }

        Ok(())
    }
}

impl<D> Drop for Registration<D> {
    fn drop(&mut self) {
        let mut state = self.broker.state.lock().expect("broker lock was poisoned");
        if state
            .sessions
            .get(self.tunnel_id.as_str())
            .is_some_and(|session| session.session_id == self.session_id)
        {
            state.sessions.remove(self.tunnel_id.as_str());
        }
    }
}

async fn write_message(
    stream: &mut (impl AsyncWrite + Unpin),
    message: &ServerControlMessage,
) -> Result<()> {
    let mut json = serde_json::to_vec(message)?;
    json.push(b'\n');
    stream.write_all(&json).await?;
    Ok(())
}
