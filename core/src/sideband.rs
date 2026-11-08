use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

const SIDEBAND_COUNT_LENGTH: usize = size_of::<u64>();
const SIDEBAND_ACK: u8 = 0xa5;

/// One visitor byte stream over a reusable data transport, with its end
/// signalled over a separate sideband stream.
///
/// Payload reads and writes go directly to `inner`, unframed. On shutdown, each
/// peer sends its payload byte count over `sideband`. The receiver returns EOF
/// only after consuming exactly that many bytes. After both directions finish,
/// [`finish`](Self::finish) exchanges acknowledgements so neither peer reuses
/// `inner` before the other has consumed everything.
pub struct SidebandStream<S, C> {
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

impl<S, C> SidebandStream<S, C> {
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

    /// Returns whether both directions have reached their boundary.
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

impl<S, C> AsyncRead for SidebandStream<S, C>
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

impl<S, C> AsyncWrite for SidebandStream<S, C>
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

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::SidebandStream;

    #[tokio::test]
    async fn carries_sequential_raw_streams_with_sideband_boundaries() {
        let (mut left_data, mut right_data) = tokio::io::duplex(1024);
        let (mut left_control, mut right_control) = tokio::io::duplex(128);
        let left_task = tokio::spawn(async move {
            for (sent, expected) in [
                (b"left one".as_slice(), b"right one".as_slice()),
                (b"left two".as_slice(), b"right two".as_slice()),
            ] {
                let mut stream = SidebandStream::new(&mut left_data, &mut left_control);
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
                let mut stream = SidebandStream::new(&mut right_data, &mut right_control);
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
