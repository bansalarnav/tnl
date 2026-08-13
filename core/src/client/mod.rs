use std::sync::Arc;

use muxado::{Accept, MuxadoAccept, MuxadoOpen, OpenClose, Session, SessionBuilder};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Mutex,
};

use crate::{ConnectionError, Stream};

/// The node side of a multiplexed tunnel session.
#[derive(Clone)]
pub struct ClientSession {
    opener: MuxadoOpen,
    accepter: Arc<Mutex<MuxadoAccept>>,
}

impl ClientSession {
    /// Starts a session over an already-established transport stream.
    pub fn new<S>(stream: S) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (opener, accepter) = SessionBuilder::new(stream).client().start().split();
        Self {
            opener,
            accepter: Arc::new(Mutex::new(accepter)),
        }
    }

    /// Opens a new bidirectional stream to the central server.
    pub async fn open(&self, tag: impl AsRef<str>) -> Result<Stream, ConnectionError> {
        let tag = tag.as_ref().to_owned();
        Stream::validate_tag(&tag)?;
        let stream = self.opener.clone().open().await?;
        Stream::outgoing(stream, tag).await
    }

    /// Waits for the central server to open the next bidirectional stream.
    pub async fn accept(&self) -> Result<Stream, ConnectionError> {
        let stream = self
            .accepter
            .lock()
            .await
            .accept()
            .await
            .ok_or(muxado::Error::SessionClosed)?;
        Stream::incoming(stream).await
    }
}
