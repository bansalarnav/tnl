use std::net::SocketAddr;

use crate::{BoxError, Handler, TunnelCertificate, Visitor, tls_relay};

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
        tls_relay::forward_visitor_tls(visitor, &self.certificate, self.target).await
    }
}

impl Handler for ForwardTls {
    async fn handle(&self, visitor: Visitor<'_>) -> Result<(), BoxError> {
        Ok(self.forward(visitor).await?)
    }
}
