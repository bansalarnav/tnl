//! Registration handshake for dedicated data transports.
//!
//! A node opens a raw TCP connection to the relay and authenticates it for a
//! tunnel it has already registered over a control session:
//!
//! ```text
//! node  -> relay  MAGIC, tunnel name length (u8), tunnel name
//! relay -> node   32-byte random challenge
//! node  -> relay  HMAC-SHA256(key, MAGIC || tunnel name || challenge)
//! relay -> node   0 and a u64 transport identifier, or 1 on failure
//! node  -> relay  0 once it is ready to receive the sideband stream
//! ```
//!
//! The relay then opens a [`sideband_tag`](crate::protocol::sideband_tag)
//! stream over a control session. The key is derived from the node's API token,
//! so the token itself never crosses the unencrypted connection.

use std::io;

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::TunnelId;

const MAGIC: &[u8; 5] = b"TNLD\x06";
/// Length of the relay's challenge.
pub const CHALLENGE_LENGTH: usize = 32;
const PROOF_LENGTH: usize = 32;
const ACCEPTED: u8 = 0;
const REJECTED: u8 = 1;

/// First byte of a data transport registration. Relays sharing a port with
/// TLS can dispatch on it, since a TLS record never starts with this byte.
pub const PREFACE_BYTE: u8 = MAGIC[0];

/// Secret proving a data transport belongs to a tunnel's owner.
#[derive(Clone)]
pub struct TransportKey(String);

impl TransportKey {
    /// Derives the key from the API token the node registered its tunnel with.
    pub fn from_token(token: &str) -> Self {
        Self(format!("{:x}", Sha256::digest(token.as_bytes())))
    }

    fn proof(&self, tunnel_id: &TunnelId, challenge: &[u8]) -> Hmac<Sha256> {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(self.0.as_bytes()).expect("HMAC accepts any key size");
        mac.update(MAGIC);
        mac.update(tunnel_id.as_str().as_bytes());
        mac.update(challenge);
        mac
    }
}

/// Node side: requests a data transport and answers the relay's challenge.
///
/// Returns the identifier the relay assigned. The node must prepare to receive
/// the matching sideband stream and then call [`confirm`].
pub async fn request<S>(stream: &mut S, tunnel_id: &TunnelId, key: &TransportKey) -> io::Result<u64>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let name = tunnel_id.as_str().as_bytes();
    let name_length = u8::try_from(name.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "tunnel name is too long"))?;
    stream.write_all(MAGIC).await?;
    stream.write_u8(name_length).await?;
    stream.write_all(name).await?;
    stream.flush().await?;

    let mut challenge = [0; CHALLENGE_LENGTH];
    stream.read_exact(&mut challenge).await?;
    let proof = key.proof(tunnel_id, &challenge).finalize().into_bytes();
    stream.write_all(&proof).await?;
    stream.flush().await?;

    if stream.read_u8().await? != ACCEPTED {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "relay rejected the data transport",
        ));
    }
    stream.read_u64().await
}

/// Node side: tells the relay it may open the sideband stream.
pub async fn confirm<S>(stream: &mut S) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    stream.write_u8(ACCEPTED).await?;
    stream.flush().await
}

/// Relay side: reads the tunnel a data transport is for. `first_byte` is the
/// already consumed [`PREFACE_BYTE`].
pub async fn read_request<S>(stream: &mut S, first_byte: u8) -> io::Result<TunnelId>
where
    S: AsyncRead + Unpin,
{
    let mut magic = [0; MAGIC.len()];
    magic[0] = first_byte;
    stream.read_exact(&mut magic[1..]).await?;
    if &magic != MAGIC {
        return Err(invalid_data("invalid data transport magic"));
    }
    let name_length = stream.read_u8().await? as usize;
    if name_length == 0 {
        return Err(invalid_data("empty tunnel name"));
    }
    let mut name = vec![0; name_length];
    stream.read_exact(&mut name).await?;
    let name = String::from_utf8(name).map_err(|_| invalid_data("tunnel name is not UTF-8"))?;
    TunnelId::new(name).map_err(|_| invalid_data("invalid tunnel name"))
}

/// Relay side: challenges the node to prove it holds `key`. `challenge` must
/// be freshly generated random bytes. On failure, tells the node and returns a
/// `PermissionDenied` error.
pub async fn authenticate<S>(
    stream: &mut S,
    tunnel_id: &TunnelId,
    key: &TransportKey,
    challenge: [u8; CHALLENGE_LENGTH],
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream.write_all(&challenge).await?;
    stream.flush().await?;
    let mut proof = [0; PROOF_LENGTH];
    stream.read_exact(&mut proof).await?;
    if key
        .proof(tunnel_id, &challenge)
        .verify_slice(&proof)
        .is_err()
    {
        stream.write_u8(REJECTED).await?;
        stream.flush().await?;
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "invalid data transport proof",
        ));
    }
    Ok(())
}

/// Relay side: assigns an identifier and waits for the node to [`confirm`].
pub async fn assign<S>(stream: &mut S, transport_id: u64) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream.write_u8(ACCEPTED).await?;
    stream.write_u64(transport_id).await?;
    stream.flush().await?;
    if stream.read_u8().await? != ACCEPTED {
        return Err(invalid_data("node rejected the data transport identifier"));
    }
    Ok(())
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, duplex};

    use super::*;

    async fn handshake(node_key: &str, relay_key: &str) -> (io::Result<u64>, io::Result<()>) {
        let tunnel_id = TunnelId::new("my-app").unwrap();
        let (mut node, mut relay) = duplex(1024);
        let node_key = TransportKey::from_token(node_key);
        let relay_key = TransportKey::from_token(relay_key);
        let node_id = tunnel_id.clone();
        let node_task = tokio::spawn(async move {
            let id = request(&mut node, &node_id, &node_key).await?;
            confirm(&mut node).await?;
            Ok(id)
        });

        let relay_result = async {
            let first_byte = relay.read_u8().await?;
            assert_eq!(first_byte, PREFACE_BYTE);
            assert_eq!(read_request(&mut relay, first_byte).await?, tunnel_id);
            authenticate(&mut relay, &tunnel_id, &relay_key, [7; CHALLENGE_LENGTH]).await?;
            assign(&mut relay, 42).await
        }
        .await;
        drop(relay);
        (node_task.await.unwrap(), relay_result)
    }

    #[tokio::test]
    async fn assigns_an_identifier_to_the_key_holder() {
        let (node, relay) = handshake("token", "token").await;
        assert_eq!(node.unwrap(), 42);
        relay.unwrap();
    }

    #[tokio::test]
    async fn rejects_the_wrong_key() {
        let (node, relay) = handshake("wrong", "token").await;
        assert_eq!(node.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(relay.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    }
}
