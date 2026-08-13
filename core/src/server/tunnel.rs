use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use rand::{Rng, distributions::Alphanumeric};
use tokio::{
    io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, copy_bidirectional},
    sync::{mpsc, oneshot, watch},
    time::{Instant, MissedTickBehavior, interval_at, timeout},
};

use crate::{
    TunnelId,
    protocol::{ClientControlMessage, ServerControlMessage},
};

use super::tls::TlsConnection;

const DATA_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(60);

type PublicUrl = Arc<dyn Fn(&TunnelId) -> String + Send + Sync>;

/// Tracks registered nodes and pairs central-initiated connections with them.
#[derive(Clone)]
pub struct TunnelRegistry {
    public_url: PublicUrl,
    state: Arc<Mutex<State>>,
    shutdown: watch::Sender<bool>,
}

#[derive(Default)]
struct State {
    tunnels: HashMap<String, Tunnel>,
    pending_connections: HashMap<String, PendingConnection>,
}

struct Tunnel {
    session_id: u64,
    owner: String,
    sender: mpsc::Sender<String>,
}

struct PendingConnection {
    owner: String,
    sender: oneshot::Sender<TunnelConnection>,
}

struct PendingRequest {
    connection_id: String,
    state: Arc<Mutex<State>>,
}

impl Drop for PendingRequest {
    fn drop(&mut self) {
        self.state
            .lock()
            .expect("tunnel registry lock was poisoned")
            .pending_connections
            .remove(&self.connection_id);
    }
}

/// The central side of a raw connection requested from a registered node.
pub type TunnelConnection = TokioIo<Upgraded>;

pub(crate) struct Registration {
    pub(crate) session_id: u64,
    pub(crate) receiver: mpsc::Receiver<String>,
}

impl TunnelRegistry {
    /// Creates a registry whose ready value is the registered tunnel ID.
    pub fn new() -> Self {
        Self::with_public_url(|tunnel_id| tunnel_id.to_string())
    }

    /// Creates a registry with a custom ready value, such as a public tunnel URL.
    pub fn with_public_url(
        public_url: impl Fn(&TunnelId) -> String + Send + Sync + 'static,
    ) -> Self {
        let (shutdown, _) = watch::channel(false);
        Self {
            public_url: Arc::new(public_url),
            state: Arc::new(Mutex::new(State::default())),
            shutdown,
        }
    }

    /// Closes registered control sessions and cancels pending connection requests.
    pub fn shutdown(&self) {
        let mut state = self
            .state
            .lock()
            .expect("tunnel registry lock was poisoned");
        state.tunnels.clear();
        state.pending_connections.clear();
        drop(state);
        self.shutdown.send_replace(true);
    }

    /// Returns whether this registry has begun shutting down.
    pub fn is_shutdown(&self) -> bool {
        *self.shutdown.borrow()
    }

    pub(crate) fn shutdown_signal(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    pub(crate) async fn register(&self, tunnel_id: &TunnelId, owner: &str) -> Option<Registration> {
        if self.is_shutdown() {
            return None;
        }

        let mut state = self
            .state
            .lock()
            .expect("tunnel registry lock was poisoned");
        if state.tunnels.contains_key(tunnel_id.as_str()) {
            return None;
        }

        let session_id = rand::thread_rng().r#gen();
        let (sender, receiver) = mpsc::channel(32);
        state.tunnels.insert(
            tunnel_id.to_string(),
            Tunnel {
                session_id,
                owner: owner.to_owned(),
                sender,
            },
        );
        Some(Registration {
            session_id,
            receiver,
        })
    }

    pub(crate) async fn unregister(&self, tunnel_id: &TunnelId, session_id: u64) {
        let mut state = self
            .state
            .lock()
            .expect("tunnel registry lock was poisoned");
        if state
            .tunnels
            .get(tunnel_id.as_str())
            .is_some_and(|tunnel| tunnel.session_id == session_id)
        {
            state.tunnels.remove(tunnel_id.as_str());
        }
    }

    pub(crate) async fn attach(
        &self,
        connection_id: &str,
        owner: &str,
    ) -> Option<oneshot::Sender<TunnelConnection>> {
        let mut state = self
            .state
            .lock()
            .expect("tunnel registry lock was poisoned");
        if state
            .pending_connections
            .get(connection_id)
            .is_some_and(|connection| connection.owner == owner)
        {
            return state
                .pending_connections
                .remove(connection_id)
                .map(|connection| connection.sender);
        }
        None
    }

    pub(crate) async fn serve_control(
        &self,
        tunnel_id: TunnelId,
        mut receiver: mpsc::Receiver<String>,
        upgraded: Upgraded,
    ) -> Result<()> {
        let (reader, mut writer) = tokio::io::split(TokioIo::new(upgraded));
        let mut reader = BufReader::new(reader);
        write_message(
            &mut writer,
            &ServerControlMessage::Ready {
                url: (self.public_url)(&tunnel_id),
            },
        )
        .await?;

        let mut heartbeat = interval_at(Instant::now() + HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_pong = Instant::now();
        let mut line = String::new();

        loop {
            tokio::select! {
                result = reader.read_line(&mut line) => {
                    if result? == 0 {
                        break;
                    }
                    match serde_json::from_str::<ClientControlMessage>(&line)
                        .context("tunnel client sent an invalid control message")?
                    {
                        ClientControlMessage::Pong => last_pong = Instant::now(),
                    }
                    line.clear();
                }
                connection_id = receiver.recv() => {
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
                        bail!("tunnel client missed the heartbeat deadline");
                    }
                    write_message(&mut writer, &ServerControlMessage::Ping).await?;
                }
            }
        }

        Ok(())
    }

    /// Requests a raw bidirectional connection from a registered node.
    ///
    /// Returns `Ok(None)` when the node is not currently registered.
    pub async fn connect(&self, tunnel_id: &TunnelId) -> Result<Option<TunnelConnection>> {
        if self.is_shutdown() {
            bail!("tunnel registry is shut down");
        }

        let (owner, sender) = {
            let state = self
                .state
                .lock()
                .expect("tunnel registry lock was poisoned");
            let Some(tunnel) = state.tunnels.get(tunnel_id.as_str()) else {
                return Ok(None);
            };
            (tunnel.owner.clone(), tunnel.sender.clone())
        };

        let connection_id: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(24)
            .map(char::from)
            .collect();
        let (data_sender, data_receiver) = oneshot::channel();
        self.state
            .lock()
            .expect("tunnel registry lock was poisoned")
            .pending_connections
            .insert(
                connection_id.clone(),
                PendingConnection {
                    owner,
                    sender: data_sender,
                },
            );
        let pending_request = PendingRequest {
            connection_id: connection_id.clone(),
            state: Arc::clone(&self.state),
        };

        if sender.send(connection_id.clone()).await.is_err() {
            bail!("tunnel client disconnected");
        }

        let data_stream = match timeout(DATA_CONNECTION_TIMEOUT, data_receiver).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(_)) => bail!("tunnel data connection was not established"),
            Err(_) => bail!("timed out waiting for the tunnel data connection"),
        };
        drop(pending_request);

        Ok(Some(data_stream))
    }

    pub(crate) async fn forward(
        &self,
        tunnel_id: &TunnelId,
        connection: TlsConnection,
    ) -> Result<()> {
        let Some(mut data_stream) = self.connect(tunnel_id).await? else {
            return Ok(());
        };

        let (client_hello, mut visitor_stream) = connection.into_raw_parts();
        data_stream
            .write_all(&client_hello)
            .await
            .context("could not forward the visitor TLS ClientHello")?;
        copy_bidirectional(&mut visitor_stream, &mut data_stream)
            .await
            .context("tunnel forwarding failed")?;
        Ok(())
    }
}

impl Default for TunnelRegistry {
    fn default() -> Self {
        Self::new()
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
