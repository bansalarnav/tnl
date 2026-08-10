use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use rand::{Rng, distributions::Alphanumeric};
use serde::Serialize;
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, copy_bidirectional},
    sync::{Mutex, mpsc, oneshot},
    time::timeout,
};

use super::tls::TlsConnection;

const DATA_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct TunnelRegistry {
    domain: Arc<str>,
    state: Arc<Mutex<State>>,
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
    sender: oneshot::Sender<TokioIo<Upgraded>>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ControlMessage<'a> {
    Ready { url: &'a str },
    Connection { id: &'a str },
}

pub struct Registration {
    pub session_id: u64,
    pub receiver: mpsc::Receiver<String>,
}

impl TunnelRegistry {
    pub fn new(domain: String) -> Self {
        Self {
            domain: domain.into(),
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    pub async fn register(&self, tunnel_id: &str, owner: &str) -> Option<Registration> {
        let mut state = self.state.lock().await;
        if state.tunnels.contains_key(tunnel_id) {
            return None;
        }

        let session_id = rand::thread_rng().r#gen();
        let (sender, receiver) = mpsc::channel(32);
        state.tunnels.insert(
            tunnel_id.to_owned(),
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

    pub async fn unregister(&self, tunnel_id: &str, session_id: u64) {
        let mut state = self.state.lock().await;
        if state
            .tunnels
            .get(tunnel_id)
            .is_some_and(|tunnel| tunnel.session_id == session_id)
        {
            state.tunnels.remove(tunnel_id);
        }
    }

    pub async fn attach(
        &self,
        connection_id: &str,
        owner: &str,
    ) -> Option<oneshot::Sender<TokioIo<Upgraded>>> {
        let mut state = self.state.lock().await;
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

    pub async fn serve_control(
        &self,
        tunnel_id: String,
        mut receiver: mpsc::Receiver<String>,
        upgraded: Upgraded,
    ) -> Result<()> {
        let (mut reader, mut writer) = tokio::io::split(TokioIo::new(upgraded));
        let url = format!("https://{tunnel_id}.{}", self.domain);
        write_message(&mut writer, &ControlMessage::Ready { url: &url }).await?;

        let mut unexpected_input = [0];
        loop {
            tokio::select! {
                result = reader.read(&mut unexpected_input) => {
                    match result? {
                        0 => break,
                        _ => bail!("tunnel client sent unexpected control data"),
                    }
                }
                connection_id = receiver.recv() => {
                    let Some(connection_id) = connection_id else {
                        break;
                    };
                    write_message(
                        &mut writer,
                        &ControlMessage::Connection { id: &connection_id },
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    pub async fn forward(&self, tunnel_id: &str, connection: TlsConnection) -> Result<()> {
        let (owner, sender) = {
            let state = self.state.lock().await;
            let Some(tunnel) = state.tunnels.get(tunnel_id) else {
                return Ok(());
            };
            (tunnel.owner.clone(), tunnel.sender.clone())
        };

        let connection_id: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(24)
            .map(char::from)
            .collect();
        let (data_sender, data_receiver) = oneshot::channel();
        self.state.lock().await.pending_connections.insert(
            connection_id.clone(),
            PendingConnection {
                owner,
                sender: data_sender,
            },
        );

        if sender.send(connection_id.clone()).await.is_err() {
            self.state
                .lock()
                .await
                .pending_connections
                .remove(&connection_id);
            bail!("tunnel client disconnected");
        }

        let mut data_stream = match timeout(DATA_CONNECTION_TIMEOUT, data_receiver).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(_)) => bail!("tunnel data connection was not established"),
            Err(_) => {
                self.state
                    .lock()
                    .await
                    .pending_connections
                    .remove(&connection_id);
                bail!("timed out waiting for the tunnel data connection");
            }
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

async fn write_message(
    stream: &mut (impl AsyncWrite + Unpin),
    message: &ControlMessage<'_>,
) -> Result<()> {
    let mut json = serde_json::to_vec(message)?;
    json.push(b'\n');
    stream.write_all(&json).await?;
    Ok(())
}
