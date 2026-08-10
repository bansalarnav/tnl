use std::{
    io::{self, Cursor},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::{Context as _, Result, bail};
use rustls::{ServerConfig, server::Acceptor};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf},
    net::TcpStream,
};
use tokio_rustls::{StartHandshake, server::TlsStream};

const MAX_CLIENT_HELLO_LENGTH: usize = 64 * 1024;

pub enum Inspection {
    Plain(PrefixedStream),
    Tls(TlsConnection),
}

pub struct TlsConnection {
    accepted: rustls::server::Accepted,
    server_name: Option<String>,
    prefix: Vec<u8>,
    stream: TcpStream,
}

impl TlsConnection {
    pub fn server_name(&self) -> Option<&str> {
        self.server_name.as_deref()
    }

    pub async fn terminate(self, config: Arc<ServerConfig>) -> Result<TlsStream<TcpStream>> {
        StartHandshake::from_parts(self.accepted, self.stream)
            .into_stream(config)
            .await
            .context("could not complete the API TLS handshake")
    }

    pub fn into_raw_parts(self) -> (Vec<u8>, TcpStream) {
        (self.prefix, self.stream)
    }
}

pub async fn inspect(mut stream: TcpStream) -> Result<Inspection> {
    let mut first_byte = [0];
    stream
        .read_exact(&mut first_byte)
        .await
        .context("could not read the connection preface")?;

    if first_byte[0] != 22 {
        return Ok(Inspection::Plain(PrefixedStream::new(
            first_byte.to_vec(),
            stream,
        )));
    }

    let mut acceptor = Acceptor::default();
    let mut prefix = first_byte.to_vec();
    feed_acceptor(&mut acceptor, &first_byte)?;

    loop {
        let accepted = acceptor
            .accept()
            .map_err(|(error, _)| error)
            .context("could not parse the TLS ClientHello")?;
        if let Some(accepted) = accepted {
            let server_name = accepted
                .client_hello()
                .server_name()
                .map(|name| name.trim_end_matches('.').to_ascii_lowercase());
            return Ok(Inspection::Tls(TlsConnection {
                accepted,
                server_name,
                prefix,
                stream,
            }));
        }

        let mut buffer = [0; 4096];
        let bytes_read = stream
            .read(&mut buffer)
            .await
            .context("could not read the TLS ClientHello")?;
        if bytes_read == 0 {
            bail!("connection closed before sending a complete TLS ClientHello");
        }
        if prefix.len() + bytes_read > MAX_CLIENT_HELLO_LENGTH {
            bail!("TLS ClientHello is too large");
        }

        let bytes = &buffer[..bytes_read];
        prefix.extend_from_slice(bytes);
        feed_acceptor(&mut acceptor, bytes)?;
    }
}

fn feed_acceptor(acceptor: &mut Acceptor, bytes: &[u8]) -> Result<()> {
    let mut cursor = Cursor::new(bytes);
    while cursor.position() < bytes.len() as u64 {
        if acceptor.read_tls(&mut cursor)? == 0 {
            bail!("Rustls did not consume the ClientHello bytes");
        }
    }
    Ok(())
}

pub struct PrefixedStream {
    prefix: Vec<u8>,
    prefix_offset: usize,
    stream: TcpStream,
}

impl PrefixedStream {
    fn new(prefix: Vec<u8>, stream: TcpStream) -> Self {
        Self {
            prefix,
            prefix_offset: 0,
            stream,
        }
    }
}

impl AsyncRead for PrefixedStream {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.prefix_offset < this.prefix.len() {
            let remaining = &this.prefix[this.prefix_offset..];
            let length = remaining.len().min(buffer.remaining());
            buffer.put_slice(&remaining[..length]);
            this.prefix_offset += length;
            return Poll::Ready(Ok(()));
        }

        Pin::new(&mut this.stream).poll_read(context, buffer)
    }
}

impl AsyncWrite for PrefixedStream {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.get_mut().stream).poll_write(context, buffer)
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(context)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(context)
    }
}
