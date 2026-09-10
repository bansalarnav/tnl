use std::{error::Error, fmt};

#[cfg(any(feature = "client", feature = "server"))]
use std::io;
#[cfg(any(feature = "client", feature = "server"))]
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[cfg(any(feature = "client", feature = "server"))]
pub const TCP_DATA_REGISTRATION_MAGIC: &[u8; 5] = b"TNLD\x04";
#[cfg(any(feature = "client", feature = "server"))]
pub const TCP_DATA_CHALLENGE_LENGTH: usize = 32;
#[cfg(any(feature = "client", feature = "server"))]
pub const TCP_DATA_PROOF_LENGTH: usize = 32;

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
        TCP_DATA_REGISTRATION_MAGIC, TunnelId, read_tcp_data_registration,
        write_tcp_data_registration,
    };
    use tokio::io::AsyncReadExt;

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
}
