use std::{error::Error, fmt};

#[cfg(any(feature = "client", feature = "server"))]
use std::{
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(any(feature = "client", feature = "server"))]
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

#[cfg(any(feature = "client", feature = "server"))]
pub const TCP_DATA_REGISTRATION_MAGIC: &[u8; 5] = b"TNLD\x06";
#[cfg(any(feature = "client", feature = "server"))]
pub const TCP_DATA_CHALLENGE_LENGTH: usize = 32;
#[cfg(any(feature = "client", feature = "server"))]
pub const TCP_DATA_PROOF_LENGTH: usize = 32;
#[cfg(any(feature = "client", feature = "server"))]
const TCP_DATA_FRAME_HEADER_LENGTH: usize = size_of::<u32>();
#[cfg(any(feature = "client", feature = "server"))]
const TCP_DATA_MAX_FRAME_LENGTH: usize = 64 * 1024;

/// Presents one framed visitor byte stream over a reusable data transport.
///
/// Each direction consists of 4-byte big-endian lengths followed by data. A
/// zero length ends that direction without shutting down the underlying
/// transport, allowing another visitor stream to use it afterwards.
#[cfg(any(feature = "client", feature = "server"))]
pub struct ReusableTransportStream<S> {
    inner: S,
    read_header: [u8; TCP_DATA_FRAME_HEADER_LENGTH],
    read_header_offset: usize,
    read_remaining: usize,
    read_finished: bool,
    write_header: [u8; TCP_DATA_FRAME_HEADER_LENGTH],
    write_header_offset: usize,
    write_remaining: usize,
    write_finished: bool,
}

#[cfg(any(feature = "client", feature = "server"))]
impl<S> ReusableTransportStream<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            read_header: [0; TCP_DATA_FRAME_HEADER_LENGTH],
            read_header_offset: 0,
            read_remaining: 0,
            read_finished: false,
            write_header: [0; TCP_DATA_FRAME_HEADER_LENGTH],
            write_header_offset: TCP_DATA_FRAME_HEADER_LENGTH,
            write_remaining: 0,
            write_finished: false,
        }
    }

    pub fn is_finished(&self) -> bool {
        self.read_finished && self.write_finished && self.write_remaining == 0
    }

    fn error(kind: io::ErrorKind, message: &'static str) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::new(kind, message)))
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl<S> AsyncRead for ReusableTransportStream<S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buffer.remaining() == 0 || this.read_finished {
            return Poll::Ready(Ok(()));
        }

        loop {
            if this.read_remaining > 0 {
                let capacity = buffer.remaining().min(this.read_remaining);
                let destination = &mut buffer.initialize_unfilled()[..capacity];
                let mut limited = ReadBuf::new(destination);
                match Pin::new(&mut this.inner).poll_read(context, &mut limited) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Ready(Ok(())) => {
                        let read = limited.filled().len();
                        if read == 0 {
                            return Self::error(
                                io::ErrorKind::UnexpectedEof,
                                "reusable transport closed during a data frame",
                            );
                        }
                        buffer.advance(read);
                        this.read_remaining -= read;
                        return Poll::Ready(Ok(()));
                    }
                }
            }

            while this.read_header_offset < TCP_DATA_FRAME_HEADER_LENGTH {
                let mut header = ReadBuf::new(
                    &mut this.read_header[this.read_header_offset..TCP_DATA_FRAME_HEADER_LENGTH],
                );
                match Pin::new(&mut this.inner).poll_read(context, &mut header) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Ready(Ok(())) => {
                        let read = header.filled().len();
                        if read == 0 {
                            return Self::error(
                                io::ErrorKind::UnexpectedEof,
                                "reusable transport closed before a frame header",
                            );
                        }
                        this.read_header_offset += read;
                    }
                }
            }

            let length = u32::from_be_bytes(this.read_header) as usize;
            this.read_header_offset = 0;
            if length == 0 {
                this.read_finished = true;
                return Poll::Ready(Ok(()));
            }
            if length > TCP_DATA_MAX_FRAME_LENGTH {
                return Self::error(
                    io::ErrorKind::InvalidData,
                    "reusable transport data frame is too large",
                );
            }
            this.read_remaining = length;
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl<S> AsyncWrite for ReusableTransportStream<S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.write_finished {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "reusable transport stream is finished",
            )));
        }
        if buffer.is_empty() {
            return Poll::Ready(Ok(0));
        }

        if this.write_remaining == 0 {
            let length = buffer.len().min(TCP_DATA_MAX_FRAME_LENGTH);
            this.write_header = (length as u32).to_be_bytes();
            this.write_header_offset = 0;
            this.write_remaining = length;

            let slices = [
                IoSlice::new(&this.write_header),
                IoSlice::new(&buffer[..length]),
            ];
            match Pin::new(&mut this.inner).poll_write_vectored(context, &slices) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "could not write reusable transport data frame",
                    )));
                }
                Poll::Ready(Ok(written)) => {
                    this.write_header_offset = written.min(TCP_DATA_FRAME_HEADER_LENGTH);
                    let payload_written = written
                        .saturating_sub(TCP_DATA_FRAME_HEADER_LENGTH)
                        .min(length);
                    this.write_remaining -= payload_written;
                    if payload_written > 0 {
                        return Poll::Ready(Ok(payload_written));
                    }
                }
            }
        }

        while this.write_header_offset < TCP_DATA_FRAME_HEADER_LENGTH {
            match Pin::new(&mut this.inner)
                .poll_write(context, &this.write_header[this.write_header_offset..])
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "could not write reusable transport frame header",
                    )));
                }
                Poll::Ready(Ok(written)) => this.write_header_offset += written,
            }
        }

        let length = buffer.len().min(this.write_remaining);
        match Pin::new(&mut this.inner).poll_write(context, &buffer[..length]) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(0)) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "could not write reusable transport data frame",
            ))),
            Poll::Ready(Ok(written)) => {
                this.write_remaining -= written;
                Poll::Ready(Ok(written))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.write_finished {
            return Poll::Ready(Ok(()));
        }
        if this.write_remaining != 0 {
            return Self::error(
                io::ErrorKind::InvalidData,
                "cannot finish an incomplete reusable transport frame",
            );
        }

        if this.write_header_offset == TCP_DATA_FRAME_HEADER_LENGTH {
            this.write_header = 0u32.to_be_bytes();
            this.write_header_offset = 0;
        }
        while this.write_header_offset < TCP_DATA_FRAME_HEADER_LENGTH {
            match Pin::new(&mut this.inner)
                .poll_write(context, &this.write_header[this.write_header_offset..])
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "could not finish reusable transport stream",
                    )));
                }
                Poll::Ready(Ok(written)) => this.write_header_offset += written,
            }
        }

        match Pin::new(&mut this.inner).poll_flush(context) {
            Poll::Ready(Ok(())) => {
                this.write_finished = true;
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
const SIDEBAND_COUNT_LENGTH: usize = size_of::<u64>();
#[cfg(any(feature = "client", feature = "server"))]
const SIDEBAND_ACK: u8 = 0xa5;
/// Presents one raw visitor byte stream whose boundaries travel over a
/// separate control stream.
///
/// Payload reads and writes go directly to `inner`. On shutdown, each peer
/// sends its payload byte count over `sideband`. The receiver returns EOF only
/// after consuming exactly that many bytes. Both peers acknowledge consumption
/// before the underlying data transport may be reused.
#[cfg(any(feature = "client", feature = "server"))]
pub struct SidebandTransportStream<S, C> {
    inner: S,
    sideband: C,
    bytes_read: u64,
    read_count: [u8; SIDEBAND_COUNT_LENGTH],
    read_count_offset: usize,
    read_limit: Option<u64>,
    read_finished: bool,
    bytes_written: u64,
    write_count: [u8; SIDEBAND_COUNT_LENGTH],
    write_count_offset: usize,
    write_count_started: bool,
    write_finished: bool,
}

#[cfg(any(feature = "client", feature = "server"))]
impl<S, C> SidebandTransportStream<S, C> {
    pub fn new(inner: S, sideband: C) -> Self {
        Self {
            inner,
            sideband,
            bytes_read: 0,
            read_count: [0; SIDEBAND_COUNT_LENGTH],
            read_count_offset: 0,
            read_limit: None,
            read_finished: false,
            bytes_written: 0,
            write_count: [0; SIDEBAND_COUNT_LENGTH],
            write_count_offset: 0,
            write_count_started: false,
            write_finished: false,
        }
    }

    pub fn is_finished(&self) -> bool {
        self.read_finished && self.write_finished
    }

    /// Completes the boundary handshake after both payload directions finish.
    pub async fn finish(&mut self) -> io::Result<()>
    where
        C: AsyncRead + AsyncWrite + Unpin,
    {
        if !self.is_finished() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "sideband transport payload did not finish in both directions",
            ));
        }
        // The payload and byte count use different TCP connections. Do not let
        // either peer reuse the payload connection until both boundaries have
        // arrived and been consumed.
        self.sideband.write_all(&[SIDEBAND_ACK]).await?;
        self.sideband.flush().await?;
        let acknowledgement = self.sideband.read_u8().await?;
        if acknowledgement != SIDEBAND_ACK {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid sideband transport acknowledgement",
            ));
        }
        Ok(())
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl<S, C> AsyncRead for SidebandTransportStream<S, C>
where
    S: AsyncRead + Unpin,
    C: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buffer.remaining() == 0 || this.read_finished {
            return Poll::Ready(Ok(()));
        }

        if this.read_limit.is_none() {
            while this.read_count_offset < SIDEBAND_COUNT_LENGTH {
                let mut count = ReadBuf::new(
                    &mut this.read_count[this.read_count_offset..SIDEBAND_COUNT_LENGTH],
                );
                match Pin::new(&mut this.sideband).poll_read(context, &mut count) {
                    Poll::Pending => break,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Ready(Ok(())) => {
                        let read = count.filled().len();
                        if read == 0 {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "sideband closed before the payload byte count",
                            )));
                        }
                        this.read_count_offset += read;
                    }
                }
            }
            if this.read_count_offset == SIDEBAND_COUNT_LENGTH {
                let limit = u64::from_be_bytes(this.read_count);
                if limit < this.bytes_read {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "sideband payload byte count is smaller than data already received",
                    )));
                }
                this.read_limit = Some(limit);
            }
        }

        if this.read_limit == Some(this.bytes_read) {
            this.read_finished = true;
            return Poll::Ready(Ok(()));
        }

        let capacity = match this.read_limit {
            Some(limit) => buffer
                .remaining()
                .min(usize::try_from(limit - this.bytes_read).unwrap_or(usize::MAX)),
            None => buffer.remaining(),
        };
        let destination = &mut buffer.initialize_unfilled()[..capacity];
        let mut limited = ReadBuf::new(destination);
        match Pin::new(&mut this.inner).poll_read(context, &mut limited) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {
                let read = limited.filled().len();
                if read == 0 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "raw data transport closed before its sideband boundary",
                    )));
                }
                this.bytes_read = this.bytes_read.checked_add(read as u64).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "payload byte count overflow")
                })?;
                buffer.advance(read);
                Poll::Ready(Ok(()))
            }
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
impl<S, C> AsyncWrite for SidebandTransportStream<S, C>
where
    S: AsyncWrite + Unpin,
    C: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.write_count_started {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "sideband transport stream is finished",
            )));
        }
        match Pin::new(&mut this.inner).poll_write(context, buffer) {
            Poll::Ready(Ok(written)) => {
                this.bytes_written =
                    this.bytes_written
                        .checked_add(written as u64)
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "payload byte count overflow",
                            )
                        })?;
                Poll::Ready(Ok(written))
            }
            result => result,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.write_finished {
            return Poll::Ready(Ok(()));
        }
        if !this.write_count_started {
            this.write_count = this.bytes_written.to_be_bytes();
            this.write_count_started = true;
        }
        while this.write_count_offset < SIDEBAND_COUNT_LENGTH {
            match Pin::new(&mut this.sideband)
                .poll_write(context, &this.write_count[this.write_count_offset..])
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "could not write sideband payload byte count",
                    )));
                }
                Poll::Ready(Ok(written)) => this.write_count_offset += written,
            }
        }
        match Pin::new(&mut this.sideband).poll_flush(context) {
            Poll::Ready(Ok(())) => {
                this.write_finished = true;
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }
}

#[cfg(any(feature = "client", feature = "server"))]
pub async fn write_tcp_data_registration<W>(stream: &mut W, tunnel_id: &TunnelId) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let tunnel = tunnel_id.as_str().as_bytes();
    if tunnel.len() > u8::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "tunnel ID is too large",
        ));
    }
    stream.write_all(TCP_DATA_REGISTRATION_MAGIC).await?;
    stream.write_u8(tunnel.len() as u8).await?;
    stream.write_all(tunnel).await?;
    stream.flush().await
}

#[cfg(any(feature = "client", feature = "server"))]
pub async fn read_tcp_data_registration<R>(stream: &mut R, first_byte: u8) -> io::Result<TunnelId>
where
    R: AsyncRead + Unpin,
{
    let mut magic = [0; TCP_DATA_REGISTRATION_MAGIC.len()];
    magic[0] = first_byte;
    stream.read_exact(&mut magic[1..]).await?;
    if &magic != TCP_DATA_REGISTRATION_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid TCP data registration magic",
        ));
    }
    let tunnel_len = stream.read_u8().await? as usize;
    if tunnel_len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty tunnel ID",
        ));
    }
    let mut tunnel = vec![0; tunnel_len];
    stream.read_exact(&mut tunnel).await?;
    let tunnel = String::from_utf8(tunnel)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "tunnel ID is not UTF-8"))?;
    TunnelId::new(tunnel)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid tunnel ID"))
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TunnelId(String);

impl TunnelId {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidTunnelId> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 63
            || value.starts_with('-')
            || value.ends_with('-')
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(InvalidTunnelId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TunnelId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct InvalidTunnelId;

impl fmt::Display for InvalidTunnelId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "tunnel name must contain only lowercase letters, numbers, and internal hyphens",
        )
    }
}

impl Error for InvalidTunnelId {}

#[cfg(test)]
mod tests {
    use super::{
        ReusableTransportStream, SidebandTransportStream, TCP_DATA_REGISTRATION_MAGIC, TunnelId,
        read_tcp_data_registration, write_tcp_data_registration,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn validates_tunnel_ids() {
        for valid in ["a", "my-app", "app123"] {
            assert!(TunnelId::new(valid).is_ok(), "{valid}");
        }
        for invalid in ["", "-app", "app-", "MyApp", "a.b", &"a".repeat(64)] {
            assert!(TunnelId::new(invalid).is_err(), "{invalid}");
        }
    }

    #[tokio::test]
    async fn round_trips_tcp_data_registration() {
        let tunnel_id = TunnelId::new("my-app").unwrap();
        let (mut client, mut server) = tokio::io::duplex(1024);
        let expected = tunnel_id.clone();
        let writer = tokio::spawn(async move {
            write_tcp_data_registration(&mut client, &expected)
                .await
                .unwrap();
        });
        let mut first = [0];
        server.read_exact(&mut first).await.unwrap();
        assert_eq!(first[0], TCP_DATA_REGISTRATION_MAGIC[0]);
        assert_eq!(
            read_tcp_data_registration(&mut server, first[0])
                .await
                .unwrap(),
            tunnel_id
        );
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn carries_sequential_streams_without_closing_the_transport() {
        let (mut left, mut right) = tokio::io::duplex(1024);
        let left_task = tokio::spawn(async move {
            for (sent, expected) in [
                (b"left one".as_slice(), b"right one".as_slice()),
                (b"left two".as_slice(), b"right two".as_slice()),
            ] {
                let mut stream = ReusableTransportStream::new(&mut left);
                stream.write_all(sent).await.unwrap();
                stream.shutdown().await.unwrap();
                let mut received = Vec::new();
                stream.read_to_end(&mut received).await.unwrap();
                assert!(stream.is_finished());
                assert_eq!(received, expected);
            }
        });
        let right_task = tokio::spawn(async move {
            for (sent, expected) in [
                (b"right one".as_slice(), b"left one".as_slice()),
                (b"right two".as_slice(), b"left two".as_slice()),
            ] {
                let mut stream = ReusableTransportStream::new(&mut right);
                stream.write_all(sent).await.unwrap();
                stream.shutdown().await.unwrap();
                let mut received = Vec::new();
                stream.read_to_end(&mut received).await.unwrap();
                assert!(stream.is_finished());
                assert_eq!(received, expected);
            }
        });

        left_task.await.unwrap();
        right_task.await.unwrap();
    }

    #[tokio::test]
    async fn carries_sequential_raw_streams_with_sideband_boundaries() {
        let (mut left_data, mut right_data) = tokio::io::duplex(1024);
        let (mut left_control, mut right_control) = tokio::io::duplex(128);
        let left_task = tokio::spawn(async move {
            for (sent, expected) in [
                (b"left one".as_slice(), b"right one".as_slice()),
                (b"left two".as_slice(), b"right two".as_slice()),
            ] {
                let mut stream = SidebandTransportStream::new(&mut left_data, &mut left_control);
                stream.write_all(sent).await.unwrap();
                stream.shutdown().await.unwrap();
                let mut received = Vec::new();
                stream.read_to_end(&mut received).await.unwrap();
                assert_eq!(received, expected);
                stream.finish().await.unwrap();
            }
        });
        let right_task = tokio::spawn(async move {
            for (sent, expected) in [
                (b"right one".as_slice(), b"left one".as_slice()),
                (b"right two".as_slice(), b"left two".as_slice()),
            ] {
                let mut stream = SidebandTransportStream::new(&mut right_data, &mut right_control);
                stream.write_all(sent).await.unwrap();
                stream.shutdown().await.unwrap();
                let mut received = Vec::new();
                stream.read_to_end(&mut received).await.unwrap();
                assert_eq!(received, expected);
                stream.finish().await.unwrap();
            }
        });

        left_task.await.unwrap();
        right_task.await.unwrap();
    }
}
