//! Forwards visitor connections, still TLS-encrypted, to the tunnel's node.

use std::{
    future::poll_fn,
    io,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
    time::Duration,
};

use anyhow::{Context as _, Result};
use tnl_core::{SidebandStream, TunnelId, protocol::FORWARD_TAG};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf},
    time::timeout,
};

use crate::{registry::Registry, tls::PrefixedHello};

const OPEN_STREAM_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a visitor waits for a busy tunnel to free or open a data transport
/// before falling back to a session stream.
const TRANSPORT_WAIT: Duration = Duration::from_millis(400);
const BUFFER_SIZE: usize = 64 * 1024;
const MAX_POOLED_BUFFERS: usize = 128;

static BUFFER_POOL: Mutex<Vec<Box<[u8]>>> = Mutex::new(Vec::new());

struct PooledBuffer(Option<Box<[u8]>>);

impl PooledBuffer {
    fn acquire() -> Self {
        let buffer = BUFFER_POOL
            .lock()
            .expect("relay buffer pool lock poisoned")
            .pop()
            .unwrap_or_else(|| vec![0_u8; BUFFER_SIZE].into_boxed_slice());
        Self(Some(buffer))
    }

    #[inline]
    fn as_slice(&self) -> &[u8] {
        self.0.as_deref().expect("pooled buffer still present")
    }

    #[inline]
    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.0.as_deref_mut().expect("pooled buffer still present")
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(buffer) = self.0.take() {
            let mut pool = BUFFER_POOL.lock().expect("relay buffer pool lock poisoned");
            if pool.len() < MAX_POOLED_BUFFERS {
                pool.push(buffer);
            }
        }
    }
}

/// Forwards `visitor` to the node serving `tunnel_id`. `prefixed_hello` holds
/// the activation marker immediately followed by the visitor's TLS ClientHello.
pub(crate) async fn forward<S>(
    registry: &Registry,
    tunnel_id: &TunnelId,
    prefixed_hello: &PrefixedHello,
    visitor: &mut S,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let transport = if registry.prefers_transports(tunnel_id) {
        registry
            .take_transport_within(tunnel_id, TRANSPORT_WAIT)
            .await
    } else {
        None
    };

    if let Some(mut transport) = transport {
        // Send [ACTIVATION_MARKER | ClientHello] in a single write syscall / TCP segment.
        transport
            .data
            .write_all(prefixed_hello.activation_and_hello())
            .await
            .context("could not activate the data transport")?;
        let mut node = SidebandStream::with_initial_written(
            &mut transport.data,
            &mut transport.sideband,
            prefixed_hello.client_hello().len() as u64,
        );
        copy_bidirectional_pooled(visitor, &mut node)
            .await
            .context("tunnel forwarding failed")?;
        node.shutdown().await?;
        node.finish().await?;
        registry.recycle_transport(tunnel_id, transport);
        return Ok(());
    }

    let Some(stream) = timeout(OPEN_STREAM_TIMEOUT, registry.open(tunnel_id, FORWARD_TAG))
        .await
        .context("timed out opening a stream to the node")?
    else {
        return Ok(());
    };
    let mut stream = stream?;
    stream
        .write_all(prefixed_hello.client_hello())
        .await
        .context("could not forward the visitor's TLS ClientHello")?;
    copy_bidirectional_pooled(visitor, &mut stream)
        .await
        .context("tunnel forwarding failed")
}

enum HalfState {
    Transferring,
    ShuttingDown,
    Done,
}

struct Half {
    buf: PooledBuffer,
    pos: usize,
    cap: usize,
    read_done: bool,
    state: HalfState,
}

impl Half {
    fn new() -> Self {
        Self {
            buf: PooledBuffer::acquire(),
            pos: 0,
            cap: 0,
            read_done: false,
            state: HalfState::Transferring,
        }
    }

    fn poll_transfer<R, W>(
        &mut self,
        cx: &mut Context<'_>,
        reader: &mut R,
        writer: &mut W,
    ) -> Poll<io::Result<()>>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        loop {
            match self.state {
                HalfState::Transferring => {
                    if self.pos == self.cap && !self.read_done {
                        let mut rb = ReadBuf::new(self.buf.as_mut_slice());
                        match Pin::new(&mut *reader).poll_read(cx, &mut rb) {
                            Poll::Ready(Ok(())) => {
                                let n = rb.filled().len();
                                if n == 0 {
                                    self.read_done = true;
                                } else {
                                    self.pos = 0;
                                    self.cap = n;
                                }
                            }
                            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                            Poll::Pending => return Poll::Pending,
                        }
                    }

                    while self.pos < self.cap {
                        match Pin::new(&mut *writer).poll_write(cx, &self.buf.as_slice()[self.pos..self.cap]) {
                            Poll::Ready(Ok(0)) => {
                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::WriteZero,
                                    "write zero byte into writer",
                                )));
                            }
                            Poll::Ready(Ok(n)) => self.pos += n,
                            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                            Poll::Pending => return Poll::Pending,
                        }
                    }

                    if self.pos == self.cap {
                        self.pos = 0;
                        self.cap = 0;
                        if self.read_done {
                            self.state = HalfState::ShuttingDown;
                        }
                    }
                }
                HalfState::ShuttingDown => match Pin::new(&mut *writer).poll_shutdown(cx) {
                    Poll::Ready(Ok(())) => {
                        self.state = HalfState::Done;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => return Poll::Pending,
                },
                HalfState::Done => return Poll::Ready(Ok(())),
            }
        }
    }
}

async fn copy_bidirectional_pooled<A, B>(a: &mut A, b: &mut B) -> io::Result<()>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    let mut a_to_b = Half::new();
    let mut b_to_a = Half::new();

    poll_fn(|cx| {
        let ab = a_to_b.poll_transfer(cx, a, b)?;
        let ba = b_to_a.poll_transfer(cx, b, a)?;
        if ab.is_ready() && ba.is_ready() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    })
    .await
}
