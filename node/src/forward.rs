use std::net::SocketAddr;

use anyhow::Context;
use rustls::server::Acceptor;
use rustls_acme::is_tls_alpn_challenge;
use tokio::{
    io::{AsyncWriteExt, copy_bidirectional_with_sizes},
    net::TcpStream,
};
use tokio_rustls::LazyConfigAcceptor;

use crate::{BoxError, Handler, TunnelCertificate, Visitor};

const BUFFER_SIZE: usize = 64 * 1024;

/// Terminates visitors' TLS with a [`TunnelCertificate`] and forwards the
/// decrypted bytes to a local TCP address.
pub struct ForwardTls {
    certificate: TunnelCertificate,
    target: SocketAddr,
}

impl ForwardTls {
    pub fn new(certificate: TunnelCertificate, target: SocketAddr) -> Self {
        Self {
            certificate,
            target,
        }
    }

    async fn forward(&self, visitor: Visitor<'_>) -> anyhow::Result<()> {
        let handshake = LazyConfigAcceptor::new(Acceptor::default(), visitor)
            .await
            .context("could not read the visitor TLS ClientHello")?;

        if is_tls_alpn_challenge(&handshake.client_hello()) {
            let mut stream = handshake
                .into_stream(self.certificate.challenge_config())
                .await
                .context("could not complete ACME TLS-ALPN-01 handshake")?;
            stream.shutdown().await?;
            return Ok(());
        }

        self.certificate
            .ready()
            .await
            .context("TLS certificate manager stopped before obtaining a certificate")?;
        let mut visitor = handshake
            .into_stream(self.certificate.server_config())
            .await
            .context("could not complete visitor TLS handshake")?;
        let mut target = TcpStream::connect(self.target)
            .await
            .with_context(|| format!("could not connect to {}", self.target))?;
        target
            .set_nodelay(true)
            .with_context(|| format!("could not disable Nagle's algorithm for {}", self.target))?;
        copy_bidirectional_with_sizes(&mut visitor, &mut target, BUFFER_SIZE, BUFFER_SIZE)
            .await
            .context("could not forward tunnel connection")?;
        Ok(())
    }
}

impl Handler for ForwardTls {
    async fn handle(&self, visitor: Visitor<'_>) -> Result<(), BoxError> {
        Ok(self.forward(visitor).await?)
    }
}
