//! Inspects incoming connections in a single initial read so data transports,
//! API/ACME connections, and visitor TLS connections can be dispatched without
//! allocating a `rustls::server::Acceptor` on the visitor hot path.

use std::{io::Cursor, sync::Arc};

use anyhow::{Context as _, Result, bail};
use rustls::{
    ServerConfig,
    server::{Accepted, Acceptor},
};
use tnl_core::{data_transport, protocol::ACTIVATION_MARKER};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::{StartHandshake, server::TlsStream};

/// First byte of a TLS handshake record.
const HANDSHAKE_RECORD: u8 = 22;
const MAX_CLIENT_HELLO_LENGTH: usize = 64 * 1024;
const ACTIVATION_LEN: usize = ACTIVATION_MARKER.len();
const INLINE_HELLO_CAPACITY: usize = 1536;
const MAX_DATA_PREFACE_INLINE: usize = 512;

#[allow(clippy::large_enum_variant)]
pub(crate) enum InspectedConnection<S> {
    DataTransport {
        prefix: DataPreface,
        stream: S,
    },
    Api(Box<ApiClientHello<S>>),
    Tunnel(TunnelClientHello<S>),
}

pub(crate) struct DataPreface {
    buf: [u8; MAX_DATA_PREFACE_INLINE],
    len: usize,
}

impl DataPreface {
    pub fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

pub(crate) struct ApiClientHello<S> {
    accepted: Accepted,
    stream: S,
}

impl<S> ApiClientHello<S> {
    pub fn is_acme_challenge(&self) -> bool {
        self.accepted
            .client_hello()
            .alpn()
            .into_iter()
            .flatten()
            .eq([b"acme-tls/1".as_slice()])
    }

    pub async fn terminate(self, config: Arc<ServerConfig>) -> Result<TlsStream<S>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        StartHandshake::from_parts(self.accepted, self.stream)
            .into_stream(config)
            .await
            .context("could not complete the TLS handshake")
    }
}

/// Visitor TLS ClientHello prefixed with [`ACTIVATION_MARKER`] so dedicated
/// data transports can transmit `[ACTIVATION_MARKER | ClientHello]` in a single
/// write syscall and TCP segment without copying or heap allocation.
#[allow(clippy::large_enum_variant)]
pub(crate) enum PrefixedHello {
    Inline {
        buf: [u8; INLINE_HELLO_CAPACITY],
        len: usize,
    },
    Heap(Vec<u8>),
}

impl PrefixedHello {
    #[inline]
    pub fn activation_and_hello(&self) -> &[u8] {
        match self {
            Self::Inline { buf, len } => &buf[..*len],
            Self::Heap(vec) => vec.as_slice(),
        }
    }

    #[inline]
    pub fn client_hello(&self) -> &[u8] {
        &self.activation_and_hello()[ACTIVATION_LEN..]
    }
}

pub(crate) struct TunnelClientHello<S> {
    server_name: String,
    prefixed_hello: PrefixedHello,
    stream: S,
}

impl<S> TunnelClientHello<S> {
    pub fn server_name(&self) -> &str {
        &self.server_name
    }

    pub fn into_parts(self) -> (PrefixedHello, S) {
        (self.prefixed_hello, self.stream)
    }
}

/// Inspects an incoming connection in a single initial `read()` syscall.
pub(crate) async fn inspect_connection<S>(
    mut stream: S,
    api_domain: &str,
) -> Result<Option<InspectedConnection<S>>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut stack_buf = [0_u8; ACTIVATION_LEN + 4096];
    stack_buf[..ACTIVATION_LEN].copy_from_slice(ACTIVATION_MARKER);

    let first_read = stream
        .read(&mut stack_buf[ACTIVATION_LEN..])
        .await
        .context("could not read the connection preface")?;
    if first_read == 0 {
        return Ok(None);
    }

    let first_byte = stack_buf[ACTIVATION_LEN];
    if first_byte == data_transport::PREFACE_BYTE {
        if first_read > MAX_DATA_PREFACE_INLINE {
            bail!("data transport preface is too large");
        }
        let mut prefix_buf = [0_u8; MAX_DATA_PREFACE_INLINE];
        prefix_buf[..first_read]
            .copy_from_slice(&stack_buf[ACTIVATION_LEN..ACTIVATION_LEN + first_read]);
        return Ok(Some(InspectedConnection::DataTransport {
            prefix: DataPreface {
                buf: prefix_buf,
                len: first_read,
            },
            stream,
        }));
    }

    if first_byte != HANDSHAKE_RECORD {
        stream.shutdown().await?;
        return Ok(None);
    }

    let first_total = ACTIVATION_LEN + first_read;
    if let SniParse::Found(sni) = parse_client_hello_sni(&stack_buf[ACTIVATION_LEN..first_total]) {
        if sni.trim_end_matches('.').eq_ignore_ascii_case(api_domain) {
            return finish_api_acceptor(stream, stack_buf[ACTIVATION_LEN..first_total].to_vec())
                .await;
        }
        let server_name = sni.trim_end_matches('.').to_ascii_lowercase();
        let prefixed_hello = if first_total <= INLINE_HELLO_CAPACITY {
            let mut inline = [0_u8; INLINE_HELLO_CAPACITY];
            inline[..first_total].copy_from_slice(&stack_buf[..first_total]);
            PrefixedHello::Inline {
                buf: inline,
                len: first_total,
            }
        } else {
            PrefixedHello::Heap(stack_buf[..first_total].to_vec())
        };
        return Ok(Some(InspectedConnection::Tunnel(TunnelClientHello {
            server_name,
            prefixed_hello,
            stream,
        })));
    }

    let mut buf = Vec::with_capacity(first_total);
    buf.extend_from_slice(&stack_buf[..first_total]);

    loop {
        let hello_bytes = &buf[ACTIVATION_LEN..];
        match parse_client_hello_sni(hello_bytes) {
            SniParse::Found(sni) => {
                if sni.trim_end_matches('.').eq_ignore_ascii_case(api_domain) {
                    return finish_api_acceptor(stream, buf[ACTIVATION_LEN..].to_vec()).await;
                }
                let server_name = sni.trim_end_matches('.').to_ascii_lowercase();
                return Ok(Some(InspectedConnection::Tunnel(TunnelClientHello {
                    server_name,
                    prefixed_hello: PrefixedHello::Heap(buf),
                    stream,
                })));
            }
            SniParse::NoSni => return Ok(None),
            SniParse::Invalid => {
                return finish_with_acceptor_fallback(stream, buf, api_domain).await;
            }
            SniParse::Incomplete => {
                if hello_bytes.len() >= MAX_CLIENT_HELLO_LENGTH {
                    bail!("TLS ClientHello is too large");
                }
                let old_len = buf.len();
                let to_read = (MAX_CLIENT_HELLO_LENGTH - hello_bytes.len()).min(4096);
                buf.resize(old_len + to_read, 0);
                let read = stream
                    .read(&mut buf[old_len..])
                    .await
                    .context("could not read the TLS ClientHello")?;
                if read == 0 {
                    bail!("connection closed before sending a complete TLS ClientHello");
                }
                buf.truncate(old_len + read);
            }
        }
    }
}

async fn finish_api_acceptor<S>(
    mut stream: S,
    initial: Vec<u8>,
) -> Result<Option<InspectedConnection<S>>>
where
    S: AsyncRead + Unpin,
{
    let mut acceptor = Acceptor::default();
    feed(&mut acceptor, &initial)?;
    let mut total_len = initial.len();

    loop {
        let accepted = acceptor
            .accept()
            .map_err(|(error, _)| error)
            .context("could not parse the TLS ClientHello")?;
        if let Some(accepted) = accepted {
            return Ok(Some(InspectedConnection::Api(Box::new(ApiClientHello {
                accepted,
                stream,
            }))));
        }

        let mut buffer = [0; 4096];
        let read = stream
            .read(&mut buffer)
            .await
            .context("could not read the TLS ClientHello")?;
        if read == 0 {
            bail!("connection closed before sending a complete TLS ClientHello");
        }
        total_len += read;
        if total_len > MAX_CLIENT_HELLO_LENGTH {
            bail!("TLS ClientHello is too large");
        }
        feed(&mut acceptor, &buffer[..read])?;
    }
}

async fn finish_with_acceptor_fallback<S>(
    mut stream: S,
    mut buf: Vec<u8>,
    api_domain: &str,
) -> Result<Option<InspectedConnection<S>>>
where
    S: AsyncRead + Unpin,
{
    let mut acceptor = Acceptor::default();
    feed(&mut acceptor, &buf[ACTIVATION_LEN..])?;

    loop {
        let accepted = acceptor
            .accept()
            .map_err(|(error, _)| error)
            .context("could not parse the TLS ClientHello")?;
        if let Some(accepted) = accepted {
            let Some(server_name) = accepted
                .client_hello()
                .server_name()
                .map(|name| name.trim_end_matches('.').to_ascii_lowercase())
            else {
                return Ok(None);
            };
            if server_name == api_domain {
                return Ok(Some(InspectedConnection::Api(Box::new(ApiClientHello {
                    accepted,
                    stream,
                }))));
            }
            return Ok(Some(InspectedConnection::Tunnel(TunnelClientHello {
                server_name,
                prefixed_hello: PrefixedHello::Heap(buf),
                stream,
            })));
        }

        let mut buffer = [0; 4096];
        let read = stream
            .read(&mut buffer)
            .await
            .context("could not read the TLS ClientHello")?;
        if read == 0 {
            bail!("connection closed before sending a complete TLS ClientHello");
        }
        if (buf.len() - ACTIVATION_LEN) + read > MAX_CLIENT_HELLO_LENGTH {
            bail!("TLS ClientHello is too large");
        }
        buf.extend_from_slice(&buffer[..read]);
        feed(&mut acceptor, &buffer[..read])?;
    }
}

fn feed(acceptor: &mut Acceptor, bytes: &[u8]) -> Result<()> {
    let mut cursor = Cursor::new(bytes);
    while cursor.position() < bytes.len() as u64 {
        if acceptor.read_tls(&mut cursor)? == 0 {
            bail!("rustls did not consume the ClientHello bytes");
        }
    }
    Ok(())
}

enum SniParse<'a> {
    Found(&'a str),
    NoSni,
    Incomplete,
    Invalid,
}

/// Zero-allocation parser for SNI inside a single-record TLS ClientHello.
/// Returns `Invalid` for multi-record fragmented ClientHellos so the caller
/// transparently falls back to `rustls::server::Acceptor`.
fn parse_client_hello_sni(data: &[u8]) -> SniParse<'_> {
    if data.len() < 5 {
        return SniParse::Incomplete;
    }
    if data[0] != HANDSHAKE_RECORD {
        return SniParse::Invalid;
    }
    let record_len = u16::from_be_bytes([data[3], data[4]]) as usize;
    if data.len() < 5 + record_len {
        return SniParse::Incomplete;
    }
    let record = &data[5..5 + record_len];
    if record.len() < 4 || record[0] != 1 {
        return SniParse::Invalid;
    }
    let hs_len = ((record[1] as usize) << 16) | ((record[2] as usize) << 8) | (record[3] as usize);
    if 4 + hs_len > record.len() {
        return SniParse::Invalid;
    }
    let body = &record[4..4 + hs_len];

    // legacy_version(2) + random(32)
    let mut pos = 34;
    if body.len() < pos + 1 {
        return SniParse::Invalid;
    }
    let sid_len = body[pos] as usize;
    pos += 1 + sid_len;
    if body.len() < pos + 2 {
        return SniParse::Invalid;
    }
    let cs_len = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
    pos += 2 + cs_len;
    if body.len() < pos + 1 {
        return SniParse::Invalid;
    }
    let cm_len = body[pos] as usize;
    pos += 1 + cm_len;

    if pos == body.len() {
        return SniParse::NoSni;
    }
    if body.len() < pos + 2 {
        return SniParse::Invalid;
    }
    let ext_total_len = u16::from_be_bytes([body[pos], body[pos + 1]]) as usize;
    pos += 2;
    let ext_end = pos + ext_total_len;
    if ext_end > body.len() {
        return SniParse::Invalid;
    }

    while pos + 4 <= ext_end {
        let ext_type = u16::from_be_bytes([body[pos], body[pos + 1]]);
        let ext_len = u16::from_be_bytes([body[pos + 2], body[pos + 3]]) as usize;
        pos += 4;
        if pos + ext_len > ext_end {
            return SniParse::Invalid;
        }
        if ext_type == 0x0000 {
            let sni_ext = &body[pos..pos + ext_len];
            if sni_ext.len() < 2 {
                return SniParse::Invalid;
            }
            let list_len = u16::from_be_bytes([sni_ext[0], sni_ext[1]]) as usize;
            if 2 + list_len > sni_ext.len() {
                return SniParse::Invalid;
            }
            let mut list_pos = 2;
            let list_end = 2 + list_len;
            while list_pos + 3 <= list_end {
                let name_type = sni_ext[list_pos];
                let name_len =
                    u16::from_be_bytes([sni_ext[list_pos + 1], sni_ext[list_pos + 2]]) as usize;
                list_pos += 3;
                if list_pos + name_len > list_end {
                    return SniParse::Invalid;
                }
                if name_type == 0 {
                    return match std::str::from_utf8(&sni_ext[list_pos..list_pos + name_len]) {
                        Ok(host) => SniParse::Found(host),
                        Err(_) => SniParse::Invalid,
                    };
                }
                list_pos += name_len;
            }
            return SniParse::NoSni;
        }
        pos += ext_len;
    }

    SniParse::NoSni
}

#[cfg(test)]
mod tests {
    use super::{SniParse, parse_client_hello_sni};

    fn make_client_hello(sni: Option<&str>) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&[0x03, 0x03]);
        body.extend_from_slice(&[0_u8; 32]);
        body.push(0);
        body.extend_from_slice(&[0x00, 0x02, 0x13, 0x01]);
        body.extend_from_slice(&[0x01, 0x00]);
        if let Some(host) = sni {
            let mut ext = Vec::new();
            let list_len = 3 + host.len();
            let ext_len = 2 + list_len;
            ext.extend_from_slice(&0x0000_u16.to_be_bytes());
            ext.extend_from_slice(&(ext_len as u16).to_be_bytes());
            ext.extend_from_slice(&(list_len as u16).to_be_bytes());
            ext.push(0);
            ext.extend_from_slice(&(host.len() as u16).to_be_bytes());
            ext.extend_from_slice(host.as_bytes());
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
    fn parses_sni_from_client_hello() {
        let hello = make_client_hello(Some("app.tnl.example.com"));
        assert!(matches!(
            parse_client_hello_sni(&hello[..4]),
            SniParse::Incomplete
        ));
        assert!(matches!(
            parse_client_hello_sni(&hello[..20]),
            SniParse::Incomplete
        ));
        assert!(matches!(
            parse_client_hello_sni(&hello),
            SniParse::Found("app.tnl.example.com")
        ));

        let no_sni = make_client_hello(None);
        assert!(matches!(parse_client_hello_sni(&no_sni), SniParse::NoSni));
    }
}
