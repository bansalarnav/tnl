use anyhow::{Context, Result, bail};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

use crate::protocol::ServerControlMessage;

const PONG_MESSAGE: &[u8] = b"{\"type\":\"pong\"}\n";

/// A registered node session over an application-provided control stream.
pub struct ClientSession<S> {
    stream: BufReader<S>,
    ready_value: Option<String>,
}

impl<S> ClientSession<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(stream: S) -> Self {
        Self {
            stream: BufReader::new(stream),
            ready_value: None,
        }
    }

    /// Waits for registration to complete and returns the server-provided value.
    pub async fn wait_until_ready(&mut self) -> Result<&str> {
        if self.ready_value.is_none() {
            loop {
                match self.next_message().await? {
                    ServerControlMessage::Ready { url } => {
                        self.ready_value = Some(url);
                        break;
                    }
                    ServerControlMessage::Connection { .. } => {
                        bail!("server sent a connection before the session was ready");
                    }
                    ServerControlMessage::Ping => self.answer_heartbeat().await?,
                }
            }
        }

        Ok(self
            .ready_value
            .as_deref()
            .expect("ready value was set above"))
    }

    /// Waits for the next connection requested by the central server.
    pub async fn accept(&mut self) -> Result<ConnectionRequest> {
        self.wait_until_ready().await?;
        loop {
            match self.next_message().await? {
                ServerControlMessage::Ready { .. } => {
                    bail!("server sent more than one ready message");
                }
                ServerControlMessage::Connection { id } => return Ok(ConnectionRequest { id }),
                ServerControlMessage::Ping => self.answer_heartbeat().await?,
            }
        }
    }

    async fn next_message(&mut self) -> Result<ServerControlMessage> {
        let mut line = String::new();
        if self.stream.read_line(&mut line).await? == 0 {
            bail!("server closed the control connection");
        }
        serde_json::from_str(&line).context("invalid server message")
    }

    async fn answer_heartbeat(&mut self) -> Result<()> {
        self.stream
            .get_mut()
            .write_all(PONG_MESSAGE)
            .await
            .context("could not answer session heartbeat")
    }
}

/// A central-initiated connection that the transport adapter should open.
pub struct ConnectionRequest {
    id: String,
}

impl ConnectionRequest {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn into_id(self) -> String {
        self.id
    }
}
