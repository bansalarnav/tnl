//! Zero-per-record-allocation TLS terminator and bidirectional TCP relay built
//! on `rustls::server::UnbufferedServerConnection`.
//!
//! Compared to `tokio-rustls`'s buffered `ServerConnection`:
//! - Eliminates `DeframerVecBuffer`'s shrink-to-4KiB reallocation churn on uploads.
//! - Eliminates `ChunkVecBuffer`'s per-record 16 KiB heap allocations on downloads.
//! - Pools 72 KiB TLS I/O buffers across connections (bounded to cap idle RAM).
//! - Uses an L2-hot thread-local 64 KiB plaintext scratch buffer to batch up to
//!   4 × 16 KiB download records per write syscall and coalesce multi-record
//!   bulk uploads into a single backend write syscall.
//! - Overlaps the local backend TCP connect with the visitor TLS handshake.

use std::{
    cell::RefCell,
    future::poll_fn,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use anyhow::{Context as _, Result, bail};
use rustls::{
    ServerConfig,
    server::UnbufferedServerConnection,
    unbuffered::{ConnectionState, EncodeError, UnbufferedStatus},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::TcpStream,
};

use crate::TunnelCertificate;

/// 72 KiB holds 4 full 16 KiB TLS records plus headers, auth tags, and headroom.
const TLS_BUF_SIZE: usize = 72 * 1024;
const PLAIN_SCRATCH_SIZE: usize = 64 * 1024;
const MAX_TLS_RECORD_PLAINTEXT: usize = 16 * 1024;
/// Cap pooled TLS buffers (128 × 72 KiB ≈ 9 MiB max).
const MAX_POOLED_BUFFERS: usize = 128;

static TLS_BUF_POOL: Mutex<Vec<Box<[u8]>>> = Mutex::new(Vec::new());

thread_local! {
    static PLAIN_SCRATCH: RefCell<Box<[u8; PLAIN_SCRATCH_SIZE]>> =
        RefCell::new(Box::new([0_u8; PLAIN_SCRATCH_SIZE]));
}

struct PooledBuf(Option<Box<[u8]>>);

impl PooledBuf {
    fn acquire() -> Self {
        let buf = TLS_BUF_POOL
            .lock()
            .expect("tls buffer pool lock poisoned")
            .pop()
            .unwrap_or_else(|| vec![0_u8; TLS_BUF_SIZE].into_boxed_slice());
        Self(Some(buf))
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

impl Drop for PooledBuf {
    fn drop(&mut self) {
        if let Some(buf) = self.0.take() {
            let mut pool = TLS_BUF_POOL.lock().expect("tls buffer pool lock poisoned");
            if pool.len() < MAX_POOLED_BUFFERS {
                pool.push(buf);
            }
        }
    }
}

/// Terminates visitor TLS on `visitor` using `certificate` and relays
/// decrypted traffic bidirectionally with a TCP connection to `target`.
pub(crate) async fn forward_visitor_tls<S>(
    mut visitor: S,
    certificate: &TunnelCertificate,
    target: SocketAddr,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut rx_buf = PooledBuf::acquire();
    let mut rx_len = visitor
        .read(rx_buf.as_mut_slice())
        .await
        .context("could not read the visitor TLS ClientHello")?;
    if rx_len == 0 {
        return Ok(());
    }

    // Wait for a complete first TLS record if the ClientHello arrived fragmented
    // across TCP segments so ALPN detection sees the full ClientHello.
    while rx_len < 5
        || (rx_buf.as_slice()[0] == 22
            && rx_len < 5 + u16::from_be_bytes([rx_buf.as_slice()[3], rx_buf.as_slice()[4]]) as usize
            && rx_len < TLS_BUF_SIZE)
    {
        let n = visitor
            .read(&mut rx_buf.as_mut_slice()[rx_len..])
            .await
            .context("could not read the visitor TLS ClientHello")?;
        if n == 0 {
            bail!("visitor closed connection before completing TLS ClientHello");
        }
        rx_len += n;
    }

    if is_acme_tls_alpn_client_hello(&rx_buf.as_slice()[..rx_len]) {
        let mut tx_buf = PooledBuf::acquire();
        let mut hs = complete_handshake(
            &mut visitor,
            certificate.challenge_config(),
            &mut rx_buf,
            0,
            rx_len,
            &mut tx_buf,
        )
        .await
        .context("could not complete ACME TLS-ALPN-01 handshake")?;
        if hs.tx_len > 0 {
            visitor.write_all(&tx_buf.as_slice()[..hs.tx_len]).await?;
        }
        send_close_notify(&mut visitor, &mut hs.conn, &mut tx_buf).await?;
        return Ok(());
    }

    certificate
        .ready()
        .await
        .context("TLS certificate manager stopped before obtaining a certificate")?;

    let mut tx_buf = PooledBuf::acquire();
    let config = certificate.server_config();

    // Overlap the local backend TCP connect with the visitor TLS handshake so
    // the localhost SYN/ACK completes during the visitor's TLS RTT.
    let (hs, local) = tokio::try_join!(
        async {
            complete_handshake(&mut visitor, config, &mut rx_buf, 0, rx_len, &mut tx_buf)
                .await
                .context("could not complete visitor TLS handshake")
        },
        async {
            let stream = TcpStream::connect(target)
                .await
                .with_context(|| format!("could not connect to {target}"))?;
            stream
                .set_nodelay(true)
                .with_context(|| format!("could not disable Nagle's algorithm for {target}"))?;
            Ok::<_, anyhow::Error>(stream)
        }
    )?;

    relay_unbuffered_tls(&mut visitor, local, hs, rx_buf, tx_buf)
        .await
        .context("could not forward tunnel connection")
}

struct HandshakeOutcome {
    conn: UnbufferedServerConnection,
    rx_pos: usize,
    rx_len: usize,
    tx_len: usize,
    initial_plain: Vec<u8>,
    visitor_eof: bool,
    tls_closed: bool,
}

async fn complete_handshake<S>(
    visitor: &mut S,
    config: Arc<ServerConfig>,
    rx_buf: &mut PooledBuf,
    mut rx_pos: usize,
    mut rx_len: usize,
    tx_buf: &mut PooledBuf,
) -> Result<HandshakeOutcome>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut conn = UnbufferedServerConnection::new(config)?;
    let mut tx_len = 0_usize;

    loop {
        let UnbufferedStatus { discard, state } =
            conn.process_tls_records(&mut rx_buf.as_mut_slice()[rx_pos..rx_len]);
        let state = state?;

        match state {
            ConnectionState::EncodeTlsData(mut state) => {
                match state.encode(&mut tx_buf.as_mut_slice()[tx_len..]) {
                    Ok(written) => {
                        tx_len += written;
                        rx_pos += discard;
                    }
                    Err(EncodeError::InsufficientSize(_)) => {
                        if tx_len > 0 {
                            visitor.write_all(&tx_buf.as_slice()[..tx_len]).await?;
                        }
                        tx_len = state.encode(tx_buf.as_mut_slice())?;
                        rx_pos += discard;
                    }
                    Err(err) => return Err(err.into()),
                }
            }
            ConnectionState::TransmitTlsData(state) => {
                if tx_len > 0 {
                    visitor.write_all(&tx_buf.as_slice()[..tx_len]).await?;
                    tx_len = 0;
                }
                state.done();
                rx_pos += discard;
            }
            ConnectionState::BlockedHandshake => {
                rx_pos += discard;
                if tx_len > 0 {
                    visitor.write_all(&tx_buf.as_slice()[..tx_len]).await?;
                    tx_len = 0;
                }
                compact_rx(rx_buf, &mut rx_pos, &mut rx_len);
                if rx_len == rx_buf.as_slice().len() {
                    bail!("TLS handshake record exceeded buffer capacity");
                }
                let n = visitor.read(&mut rx_buf.as_mut_slice()[rx_len..]).await?;
                if n == 0 {
                    bail!("visitor closed connection during TLS handshake");
                }
                rx_len += n;
            }
            ConnectionState::ReadTraffic(mut state) => {
                // `process_tls_records` already decrypted these bytes in-place
                // and incremented the TLS record sequence number, so we MUST
                // extract them and advance `rx_pos += discard`.
                let mut initial_plain = Vec::new();
                while let Some(res) = state.next_record() {
                    let record = res?;
                    initial_plain.extend_from_slice(record.payload);
                }
                drop(state);
                rx_pos += discard;
                return Ok(HandshakeOutcome {
                    conn,
                    rx_pos,
                    rx_len,
                    tx_len,
                    initial_plain,
                    visitor_eof: false,
                    tls_closed: false,
                });
            }
            ConnectionState::WriteTraffic(_) => {
                rx_pos += discard;
                return Ok(HandshakeOutcome {
                    conn,
                    rx_pos,
                    rx_len,
                    tx_len,
                    initial_plain: Vec::new(),
                    visitor_eof: false,
                    tls_closed: false,
                });
            }
            ConnectionState::PeerClosed => {
                rx_pos += discard;
                return Ok(HandshakeOutcome {
                    conn,
                    rx_pos,
                    rx_len,
                    tx_len,
                    initial_plain: Vec::new(),
                    visitor_eof: true,
                    tls_closed: false,
                });
            }
            ConnectionState::Closed => {
                rx_pos += discard;
                return Ok(HandshakeOutcome {
                    conn,
                    rx_pos,
                    rx_len,
                    tx_len,
                    initial_plain: Vec::new(),
                    visitor_eof: true,
                    tls_closed: true,
                });
            }
            ConnectionState::ReadEarlyData(mut state) => {
                while let Some(res) = state.next_record() {
                    let _ = res?;
                }
                rx_pos += discard;
            }
            _ => {
                rx_pos += discard;
            }
        }
    }
}

async fn send_close_notify<S>(
    visitor: &mut S,
    conn: &mut UnbufferedServerConnection,
    tx_buf: &mut PooledBuf,
) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let status = conn.process_tls_records(&mut []);
    if let Ok(ConnectionState::WriteTraffic(mut writer)) = status.state
        && let Ok(n) = writer.queue_close_notify(tx_buf.as_mut_slice())
        && n > 0
    {
        visitor.write_all(&tx_buf.as_slice()[..n]).await?;
    }
    Ok(())
}

#[inline]
fn compact_rx(rx_buf: &mut PooledBuf, rx_pos: &mut usize, rx_len: &mut usize) {
    if *rx_pos == *rx_len {
        *rx_pos = 0;
        *rx_len = 0;
    } else if *rx_pos > 0 {
        rx_buf.as_mut_slice().copy_within(*rx_pos..*rx_len, 0);
        *rx_len -= *rx_pos;
        *rx_pos = 0;
    }
}

fn tls_error(error: rustls::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

fn is_routine_write_shutdown_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::NotConnected
            | io::ErrorKind::ConnectionAborted
    )
}

struct PendingLocal {
    buf: Vec<u8>,
    pos: usize,
}

impl PendingLocal {
    fn new(initial: Vec<u8>) -> Self {
        Self {
            buf: initial,
            pos: 0,
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    #[inline]
    fn push(&mut self, data: &[u8]) {
        if self.is_empty() {
            self.buf.clear();
            self.pos = 0;
        }
        self.buf.extend_from_slice(data);
    }

    #[inline]
    fn remaining(&self) -> &[u8] {
        &self.buf[self.pos..]
    }

    #[inline]
    fn advance(&mut self, n: usize) {
        self.pos += n;
        if self.pos >= self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }
    }
}

async fn relay_unbuffered_tls<S>(
    visitor: &mut S,
    mut local: TcpStream,
    hs: HandshakeOutcome,
    mut rx_buf: PooledBuf,
    mut tx_buf: PooledBuf,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let HandshakeOutcome {
        mut conn,
        mut rx_pos,
        mut rx_len,
        mut tx_len,
        initial_plain,
        mut visitor_eof,
        mut tls_closed,
    } = hs;
    let mut tx_pos = 0_usize;
    let mut pending_local = PendingLocal::new(initial_plain);
    let mut local_eof = false;
    let mut local_shutdown_done = false;
    let mut close_notify_queued = false;

    poll_fn(|cx| {
        // Avoid re-polling a read side that already returned `Pending` during
        // this same wakeup (cuts redundant EAGAIN syscalls on request/response
        // workloads like `latency_c1`/`latency_c64`).
        let mut local_read_pending = false;
        let mut visitor_read_pending = false;

        loop {
            let mut progressed = false;

            // 1. Flush any encrypted TLS bytes waiting in `tx_buf` to `visitor`.
            while tx_pos < tx_len {
                match Pin::new(&mut *visitor).poll_write(cx, &tx_buf.as_slice()[tx_pos..tx_len]) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "visitor socket accepted 0 bytes",
                        )));
                    }
                    Poll::Ready(Ok(written)) => {
                        tx_pos += written;
                        progressed = true;
                        if tx_pos == tx_len {
                            tx_pos = 0;
                            tx_len = 0;
                        }
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => break,
                }
            }

            // 2. Flush any backpressured plaintext waiting for `local`.
            while !pending_local.is_empty() {
                match Pin::new(&mut local).poll_write(cx, pending_local.remaining()) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "local backend accepted 0 bytes",
                        )));
                    }
                    Poll::Ready(Ok(written)) => {
                        pending_local.advance(written);
                        progressed = true;
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => break,
                }
            }

            // 3. Process incoming TLS records already buffered in `rx_buf`.
            if pending_local.is_empty()
                && rx_pos < rx_len
                && !tls_closed
                && let Some(true) = process_rx_records(
                    cx,
                    &mut conn,
                    &mut rx_buf,
                    &mut rx_pos,
                    &mut rx_len,
                    &mut tx_buf,
                    &mut tx_pos,
                    &mut tx_len,
                    &mut local,
                    &mut pending_local,
                    &mut visitor_eof,
                    &mut tls_closed,
                )?
            {
                continue;
            }

            // Compact `rx_buf` when empty or when remaining tail space is small.
            if rx_pos == rx_len {
                rx_pos = 0;
                rx_len = 0;
            } else if rx_pos > 0 && TLS_BUF_SIZE - rx_len < 18 * 1024 {
                compact_rx(&mut rx_buf, &mut rx_pos, &mut rx_len);
            }

            // 4. Shut down `local` write half once the visitor has finished sending.
            if visitor_eof && pending_local.is_empty() && !local_shutdown_done {
                match Pin::new(&mut local).poll_shutdown(cx) {
                    Poll::Ready(Ok(())) => {
                        local_shutdown_done = true;
                        progressed = true;
                    }
                    Poll::Ready(Err(err)) if is_routine_write_shutdown_error(&err) => {
                        local_shutdown_done = true;
                        progressed = true;
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => {}
                }
            }

            // 5. Read plaintext from `local` and batch-encrypt up to 4 × 16 KiB
            //    TLS records straight into `tx_buf` when `tx_buf` is empty.
            if !local_eof && !local_read_pending && !tls_closed && tx_len == 0 {
                // Always inspect rustls state BEFORE reading from `local` so
                // control frames (NewSessionTicket, KeyUpdate) are emitted first
                // and plaintext bytes are never dropped.
                let UnbufferedStatus { state, .. } = conn.process_tls_records(&mut []);
                match state.map_err(tls_error)? {
                    ConnectionState::EncodeTlsData(mut state) => {
                        let written = state.encode(tx_buf.as_mut_slice()).map_err(|err| {
                            io::Error::new(io::ErrorKind::InvalidData, err.to_string())
                        })?;
                        tx_pos = 0;
                        tx_len = written;
                        continue;
                    }
                    ConnectionState::TransmitTlsData(state) => {
                        state.done();
                        continue;
                    }
                    ConnectionState::Closed => {
                        tls_closed = true;
                        visitor_eof = true;
                        continue;
                    }
                    ConnectionState::WriteTraffic(mut writer) => {
                        let read_outcome = PLAIN_SCRATCH.with(|cell| -> Poll<io::Result<usize>> {
                            let mut scratch = cell.borrow_mut();
                            let mut rb = ReadBuf::new(&mut scratch[..]);
                            match Pin::new(&mut local).poll_read(cx, &mut rb) {
                                Poll::Pending => Poll::Pending,
                                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
                                Poll::Ready(Ok(())) => {
                                    let filled = rb.filled();
                                    let n = filled.len();
                                    if n == 0 {
                                        return Poll::Ready(Ok(0));
                                    }
                                    let mut out_len = 0_usize;
                                    let out_slice = tx_buf.as_mut_slice();
                                    for chunk in filled.chunks(MAX_TLS_RECORD_PLAINTEXT) {
                                        let enc = writer
                                            .encrypt(chunk, &mut out_slice[out_len..])
                                            .map_err(|err| {
                                                io::Error::new(
                                                    io::ErrorKind::InvalidData,
                                                    err.to_string(),
                                                )
                                            })?;
                                        out_len += enc;
                                    }
                                    tx_pos = 0;
                                    tx_len = out_len;
                                    Poll::Ready(Ok(n))
                                }
                            }
                        });

                        match read_outcome {
                            Poll::Ready(Ok(0)) => {
                                local_eof = true;
                                if !close_notify_queued
                                    && let Ok(enc) = writer.queue_close_notify(tx_buf.as_mut_slice())
                                {
                                    tx_pos = 0;
                                    tx_len = enc;
                                    close_notify_queued = true;
                                }
                                continue;
                            }
                            Poll::Ready(Ok(_)) => {
                                continue;
                            }
                            Poll::Ready(Err(err))
                                if visitor_eof && is_routine_write_shutdown_error(&err) =>
                            {
                                local_eof = true;
                                continue;
                            }
                            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                            Poll::Pending => {
                                local_read_pending = true;
                            }
                        }
                    }
                    _ => {}
                }
            }

            // 6. Queue TLS close_notify if `local_eof` happened while `tx_buf` was busy.
            if local_eof && !close_notify_queued && !tls_closed && tx_len == 0 {
                let UnbufferedStatus { state, .. } = conn.process_tls_records(&mut []);
                match state.map_err(tls_error)? {
                    ConnectionState::WriteTraffic(mut writer) => {
                        if let Ok(enc) = writer.queue_close_notify(tx_buf.as_mut_slice()) {
                            tx_pos = 0;
                            tx_len = enc;
                            close_notify_queued = true;
                            continue;
                        }
                        close_notify_queued = true;
                    }
                    ConnectionState::EncodeTlsData(mut state) => {
                        let written = state.encode(tx_buf.as_mut_slice()).map_err(|err| {
                            io::Error::new(io::ErrorKind::InvalidData, err.to_string())
                        })?;
                        tx_pos = 0;
                        tx_len = written;
                        continue;
                    }
                    ConnectionState::TransmitTlsData(state) => {
                        state.done();
                        continue;
                    }
                    ConnectionState::Closed => {
                        tls_closed = true;
                        visitor_eof = true;
                        continue;
                    }
                    _ => {
                        close_notify_queued = true;
                    }
                }
            }

            // 7. Read more encrypted TLS bytes from `visitor` into `rx_buf`.
            if !visitor_eof && !visitor_read_pending && !tls_closed && rx_len < TLS_BUF_SIZE {
                let mut rb = ReadBuf::new(&mut rx_buf.as_mut_slice()[rx_len..]);
                match Pin::new(&mut *visitor).poll_read(cx, &mut rb) {
                    Poll::Ready(Ok(())) => {
                        let n = rb.filled().len();
                        if n == 0 {
                            visitor_eof = true;
                        } else {
                            rx_len += n;
                        }
                        continue;
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => {
                        visitor_read_pending = true;
                    }
                }
            }

            // 8. Check for clean bidirectional completion.
            if tx_pos == tx_len && pending_local.is_empty() {
                if tls_closed {
                    if !local_shutdown_done {
                        visitor_eof = true;
                        continue;
                    }
                    return Poll::Ready(Ok(()));
                }
                if visitor_eof && local_eof && local_shutdown_done {
                    return Poll::Ready(Ok(()));
                }
            }

            if !progressed {
                return Poll::Pending;
            }
        }
    })
    .await
}

/// Processes one or more TLS records from `rx_buf[rx_pos..rx_len]`.
/// Returns `Ok(Some(true))` if forward progress was made, `Ok(Some(false))` if
/// more network I/O is needed.
#[allow(clippy::too_many_arguments)]
fn process_rx_records(
    cx: &mut Context<'_>,
    conn: &mut UnbufferedServerConnection,
    rx_buf: &mut PooledBuf,
    rx_pos: &mut usize,
    rx_len: &mut usize,
    tx_buf: &mut PooledBuf,
    tx_pos: &mut usize,
    tx_len: &mut usize,
    local: &mut TcpStream,
    pending_local: &mut PendingLocal,
    visitor_eof: &mut bool,
    tls_closed: &mut bool,
) -> io::Result<Option<bool>> {
    let UnbufferedStatus { discard, state } =
        conn.process_tls_records(&mut rx_buf.as_mut_slice()[*rx_pos..*rx_len]);
    match state.map_err(tls_error)? {
        ConnectionState::ReadTraffic(mut state) => {
            // Fast path: if this record consumes all buffered ciphertext, write
            // directly to `local` without copying through `PLAIN_SCRATCH`.
            let more_ciphertext = *rx_pos + discard < *rx_len;
            if !more_ciphertext {
                let mut wrote_any = false;
                while let Some(res) = state.next_record() {
                    let record = res.map_err(tls_error)?;
                    if !record.payload.is_empty() {
                        write_or_stash_local(cx, local, record.payload, pending_local)?;
                        wrote_any = true;
                    }
                }
                drop(state);
                *rx_pos += discard;
                return Ok(Some(wrote_any || discard > 0));
            }

            // Bulk upload path: coalesce consecutive decrypted records from
            // `rx_buf` into L2-hot `PLAIN_SCRATCH` (up to 64 KiB) so we flush
            // up to 4 × 16 KiB records to `local` in a single write syscall.
            let mut scratch_len = 0_usize;
            PLAIN_SCRATCH.with(|cell| -> io::Result<()> {
                let mut scratch = cell.borrow_mut();
                while let Some(res) = state.next_record() {
                    let record = res.map_err(tls_error)?;
                    let payload = record.payload;
                    if scratch_len + payload.len() <= PLAIN_SCRATCH_SIZE {
                        scratch[scratch_len..scratch_len + payload.len()].copy_from_slice(payload);
                        scratch_len += payload.len();
                    } else {
                        if scratch_len > 0 {
                            write_or_stash_local(cx, local, &scratch[..scratch_len], pending_local)?;
                            scratch_len = 0;
                        }
                        write_or_stash_local(cx, local, payload, pending_local)?;
                    }
                }
                Ok(())
            })?;
            drop(state);
            *rx_pos += discard;

            PLAIN_SCRATCH.with(|cell| -> io::Result<()> {
                let mut scratch = cell.borrow_mut();
                // Keep decrypting additional records already present in `rx_buf`
                // while we still have room for another full 16 KiB record and
                // `local` hasn't backpressured.
                while pending_local.is_empty()
                    && *rx_pos < *rx_len
                    && PLAIN_SCRATCH_SIZE - scratch_len >= MAX_TLS_RECORD_PLAINTEXT
                {
                    let UnbufferedStatus {
                        discard: next_discard,
                        state: next_state,
                    } = conn.process_tls_records(&mut rx_buf.as_mut_slice()[*rx_pos..*rx_len]);
                    match next_state.map_err(tls_error)? {
                        ConnectionState::ReadTraffic(mut next_read) => {
                            while let Some(res) = next_read.next_record() {
                                let record = res.map_err(tls_error)?;
                                let payload = record.payload;
                                if scratch_len + payload.len() <= PLAIN_SCRATCH_SIZE {
                                    scratch[scratch_len..scratch_len + payload.len()]
                                        .copy_from_slice(payload);
                                    scratch_len += payload.len();
                                } else {
                                    if scratch_len > 0 {
                                        write_or_stash_local(
                                            cx,
                                            local,
                                            &scratch[..scratch_len],
                                            pending_local,
                                        )?;
                                        scratch_len = 0;
                                    }
                                    write_or_stash_local(cx, local, payload, pending_local)?;
                                }
                            }
                            drop(next_read);
                            *rx_pos += next_discard;
                        }
                        ConnectionState::PeerClosed => {
                            *rx_pos += next_discard;
                            *visitor_eof = true;
                            break;
                        }
                        ConnectionState::Closed => {
                            *rx_pos += next_discard;
                            *visitor_eof = true;
                            *tls_closed = true;
                            break;
                        }
                        ConnectionState::EncodeTlsData(mut enc) => {
                            *rx_pos += next_discard;
                            if *tx_pos == *tx_len {
                                *tx_pos = 0;
                                *tx_len = 0;
                            }
                            if let Ok(written) = enc.encode(&mut tx_buf.as_mut_slice()[*tx_len..]) {
                                *tx_len += written;
                            }
                            break;
                        }
                        ConnectionState::TransmitTlsData(tx) => {
                            *rx_pos += next_discard;
                            if *tx_pos == *tx_len {
                                tx.done();
                            }
                            break;
                        }
                        _ => {
                            *rx_pos += next_discard;
                            break;
                        }
                    }
                }

                if scratch_len > 0 {
                    write_or_stash_local(cx, local, &scratch[..scratch_len], pending_local)?;
                }
                Ok(())
            })?;
            Ok(Some(true))
        }
        ConnectionState::EncodeTlsData(mut state) => {
            *rx_pos += discard;
            if *tx_pos == *tx_len {
                *tx_pos = 0;
                *tx_len = 0;
            }
            match state.encode(&mut tx_buf.as_mut_slice()[*tx_len..]) {
                Ok(written) => {
                    *tx_len += written;
                    Ok(Some(true))
                }
                Err(EncodeError::InsufficientSize(_)) => Ok(Some(discard > 0)),
                Err(err) => Err(io::Error::new(io::ErrorKind::InvalidData, err.to_string())),
            }
        }
        ConnectionState::TransmitTlsData(state) => {
            *rx_pos += discard;
            if *tx_pos == *tx_len {
                state.done();
                Ok(Some(true))
            } else {
                Ok(Some(discard > 0))
            }
        }
        ConnectionState::PeerClosed => {
            *rx_pos += discard;
            *visitor_eof = true;
            Ok(Some(true))
        }
        ConnectionState::Closed => {
            *rx_pos += discard;
            *visitor_eof = true;
            *tls_closed = true;
            Ok(Some(true))
        }
        _ => {
            if discard > 0 {
                *rx_pos += discard;
                Ok(Some(true))
            } else {
                Ok(Some(false))
            }
        }
    }
}

fn write_or_stash_local(
    cx: &mut Context<'_>,
    local: &mut TcpStream,
    mut data: &[u8],
    pending_local: &mut PendingLocal,
) -> io::Result<()> {
    if !pending_local.is_empty() {
        pending_local.push(data);
        return Ok(());
    }

    while !data.is_empty() {
        match Pin::new(&mut *local).poll_write(cx, data) {
            Poll::Ready(Ok(0)) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "local backend accepted 0 bytes",
                ));
            }
            Poll::Ready(Ok(written)) => data = &data[written..],
            Poll::Ready(Err(err)) => return Err(err),
            Poll::Pending => {
                pending_local.push(data);
                return Ok(());
            }
        }
    }
    Ok(())
}

/// Zero-allocation detector for ACME TLS-ALPN-01 (`acme-tls/1`) ClientHellos.
/// Returns `true` only when the ClientHello's ALPN extension offers solely
/// `acme-tls/1`, matching `rustls_acme::is_tls_alpn_challenge`.
pub(crate) fn is_acme_tls_alpn_client_hello(data: &[u8]) -> bool {
    if data.len() < 5 || data[0] != 22 {
        return false;
    }
    let record_len = u16::from_be_bytes([data[3], data[4]]) as usize;
    if data.len() < 5 + record_len {
        return false;
    }
    let record = &data[5..5 + record_len];
    if record.len() < 4 || record[0] != 1 {
        return false;
    }
    let hs_len = ((record[1] as usize) << 16) | ((record[2] as usize) << 8) | (record[3] as usize);
    if 4 + hs_len > record.len() {
        return false;
    }
    let body = &record[4..4 + hs_len];

    // legacy_version(2) + random(32)
    let mut pos = 34;
    if body.len() < pos + 1 {
        return false;
    }
    pos += 1 + body[pos] as usize;
    if body.len() < pos + 2 {
        return false;
    }
    let cs_len = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
    pos += 2 + cs_len;
    if body.len() < pos + 1 {
        return false;
    }
    pos += 1 + body[pos] as usize;
    if body.len() < pos + 2 {
        return false;
    }
    let ext_total_len = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
    pos += 2;
    let ext_end = pos + ext_total_len;
    if ext_end > body.len() {
        return false;
    }

    while pos + 4 <= ext_end {
        let ext_type = u16::from_be_bytes([body[pos], body[pos + 1]]);
        let ext_len = u16::from_be_bytes([body[pos + 2], body[pos + 3]]) as usize;
        pos += 4;
        if pos + ext_len > ext_end {
            return false;
        }
        // ALPN extension = 0x0010
        if ext_type == 0x0010 {
            let alpn = &body[pos..pos + ext_len];
            if alpn.len() < 2 {
                return false;
            }
            let list_len = u16::from_be_bytes([alpn[0], alpn[1]]) as usize;
            if 2 + list_len != alpn.len() || list_len == 0 {
                return false;
            }
            let proto_len = alpn[2] as usize;
            return 1 + proto_len == list_len && &alpn[3..3 + proto_len] == b"acme-tls/1";
        }
        pos += ext_len;
    }

    false
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rustls::{
        ClientConfig, RootCertStore, ServerConfig,
        crypto::ring,
        pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };
    use tokio_rustls::TlsConnector;

    use super::{
        PooledBuf, complete_handshake, is_acme_tls_alpn_client_hello, relay_unbuffered_tls,
    };

    fn make_client_hello_with_alpn(protocols: &[&[u8]]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&[0x03, 0x03]);
        body.extend_from_slice(&[0_u8; 32]);
        body.push(0);
        body.extend_from_slice(&[0x00, 0x02, 0x13, 0x01]);
        body.extend_from_slice(&[0x01, 0x00]);
        if !protocols.is_empty() {
            let mut list = Vec::new();
            for proto in protocols {
                list.push(proto.len() as u8);
                list.extend_from_slice(proto);
            }
            let mut ext = Vec::new();
            ext.extend_from_slice(&0x0010_u16.to_be_bytes());
            ext.extend_from_slice(&((2 + list.len()) as u16).to_be_bytes());
            ext.extend_from_slice(&(list.len() as u16).to_be_bytes());
            ext.extend_from_slice(&list);
            body.extend_from_slice(&(ext.len() as u16).to_be_bytes());
            body.extend_from_slice(&ext);
        }
        let mut hs = vec![1];
        let hs_len = body.len();
        hs.extend_from_slice(&[(hs_len >> 16) as u8, (hs_len >> 8) as u8, hs_len as u8]);
        hs.extend_from_slice(&body);
        let mut rec = vec![22, 0x03, 0x01];
        rec.extend_from_slice(&(hs.len() as u16).to_be_bytes());
        rec.extend_from_slice(&hs);
        rec
    }

    #[test]
    fn detects_acme_tls_alpn_only() {
        assert!(is_acme_tls_alpn_client_hello(
            &make_client_hello_with_alpn(&[b"acme-tls/1"])
        ));
        assert!(!is_acme_tls_alpn_client_hello(
            &make_client_hello_with_alpn(&[b"h2", b"http/1.1"])
        ));
        assert!(!is_acme_tls_alpn_client_hello(
            &make_client_hello_with_alpn(&[b"acme-tls/1", b"http/1.1"])
        ));
        assert!(!is_acme_tls_alpn_client_hello(
            &make_client_hello_with_alpn(&[])
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn relays_full_duplex_and_piggybacked_tls_traffic() {
        let generated = rcgen::generate_simple_self_signed(vec!["relay.test".to_owned()]).unwrap();
        let der = generated.cert.der().clone();
        let key =
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(generated.key_pair.serialize_der()));
        let provider = Arc::new(ring::default_provider());
        let server_cfg = Arc::new(
            ServerConfig::builder_with_provider(Arc::clone(&provider))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(vec![der.clone()], key)
                .unwrap(),
        );
        let mut roots = RootCertStore::empty();
        roots.add(der).unwrap();
        let client_cfg = Arc::new(
            ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );

        // Local backend: full-duplex echo server (reads and writes concurrently).
        let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_addr = backend_listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = backend_listener.accept().await {
                tokio::spawn(async move {
                    let (mut r, mut w) = sock.split();
                    let _ = tokio::io::copy(&mut r, &mut w).await;
                    let _ = w.shutdown().await;
                });
            }
        });

        let tls_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tls_addr = tls_listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut visitor, _)) = tls_listener.accept().await {
                let cfg = Arc::clone(&server_cfg);
                tokio::spawn(async move {
                    let mut rx_buf = PooledBuf::acquire();
                    let rx_len = visitor.read(rx_buf.as_mut_slice()).await.unwrap();
                    let mut tx_buf = PooledBuf::acquire();
                    let hs = complete_handshake(&mut visitor, cfg, &mut rx_buf, 0, rx_len, &mut tx_buf)
                        .await
                        .unwrap();
                    let local = TcpStream::connect(backend_addr).await.unwrap();
                    relay_unbuffered_tls(&mut visitor, local, hs, rx_buf, tx_buf)
                        .await
                        .unwrap();
                });
            }
        });

        // 16 concurrent full-duplex 512 KiB transfers where client writes and
        // reads simultaneously (exercises piggybacked data + backpressure).
        let mut tasks = Vec::new();
        for id in 0..16_u8 {
            let client_cfg = Arc::clone(&client_cfg);
            tasks.push(tokio::spawn(async move {
                let tcp = TcpStream::connect(tls_addr).await.unwrap();
                let tls = TlsConnector::from(client_cfg)
                    .connect(ServerName::try_from("relay.test".to_owned()).unwrap(), tcp)
                    .await
                    .unwrap();
                let payload = vec![id.wrapping_add(1); 512 * 1024];
                let (mut r, mut w) = tokio::io::split(tls);
                let send_payload = payload.clone();
                let writer = tokio::spawn(async move {
                    w.write_all(&send_payload).await.unwrap();
                    w.shutdown().await.unwrap();
                });
                let mut received = Vec::with_capacity(payload.len());
                r.read_to_end(&mut received).await.unwrap();
                writer.await.unwrap();
                assert_eq!(received, payload);
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
    }
}
