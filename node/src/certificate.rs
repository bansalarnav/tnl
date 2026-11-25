use std::{
    fs, io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use rustls::{
    CipherSuite, ServerConfig,
    crypto::{CryptoProvider, ring},
};
use rustls_acme::{AcmeConfig, EventOk, caches::DirCache};
use tokio::{sync::watch, task::JoinHandle};
use tokio_stream::StreamExt;
use tracing::{info, warn};

/// A Let's Encrypt certificate for a tunnel's hostname, obtained and renewed
/// with TLS-ALPN-01 challenges that arrive through the tunnel itself.
///
/// Since the relay never decrypts visitor traffic, the node holds the only
/// copy of the certificate's private key.
#[derive(Clone)]
pub struct TunnelCertificate {
    inner: Arc<Inner>,
}

struct Inner {
    regular: Arc<ServerConfig>,
    challenge: Arc<ServerConfig>,
    ready_fast: Arc<AtomicBool>,
    ready: watch::Receiver<bool>,
    task: JoinHandle<()>,
}

/// Prefers TLS 1.3 AES-128-GCM, which is cheapest on hardware with AES-NI.
fn crypto_provider() -> Arc<CryptoProvider> {
    let mut provider = ring::default_provider();
    provider
        .cipher_suites
        .sort_by_key(|suite| usize::from(suite.suite() != CipherSuite::TLS13_AES_128_GCM_SHA256));
    Arc::new(provider)
}

impl TunnelCertificate {
    /// Starts managing a certificate for `hostname`, caching account keys and
    /// certificates in `cache_directory`.
    pub fn start(hostname: &str, cache_directory: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&cache_directory)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&cache_directory, fs::Permissions::from_mode(0o700))?;
        }

        let mut state = AcmeConfig::new([hostname.to_owned()])
            .cache(DirCache::new(cache_directory))
            .directory_lets_encrypt(true)
            .state();
        let mut regular = ServerConfig::builder_with_provider(crypto_provider())
            .with_safe_default_protocol_versions()
            .map_err(io::Error::other)?
            .with_no_client_auth()
            .with_cert_resolver(state.resolver());
        // Honor server cipher preference so clients offering AES-256 first
        // still negotiate AES-128-GCM, and emit a single TLS 1.3 ticket.
        regular.ignore_client_order = true;
        regular.send_tls13_tickets = 1;
        let regular = Arc::new(regular);
        let challenge = state.challenge_rustls_config();

        let (ready_sender, ready) = watch::channel(false);
        let ready_fast = Arc::new(AtomicBool::new(false));
        let ready_fast_task = Arc::clone(&ready_fast);
        let hostname = hostname.to_owned();
        let task = tokio::spawn(async move {
            while let Some(event) = state.next().await {
                match event {
                    Ok(EventOk::DeployedCachedCert | EventOk::DeployedNewCert) => {
                        info!("deployed TLS certificate for {hostname}");
                        ready_fast_task.store(true, Ordering::Release);
                        ready_sender.send_replace(true);
                    }
                    Ok(EventOk::CertCacheStore | EventOk::AccountCacheStore) => {}
                    Err(error) => warn!("TLS certificate error for {hostname}: {error}"),
                }
            }
        });
        Ok(Self {
            inner: Arc::new(Inner {
                regular,
                challenge,
                ready_fast,
                ready,
                task,
            }),
        })
    }

    /// Waits until a certificate has been deployed.
    pub async fn ready(&self) -> io::Result<()> {
        if self.inner.ready_fast.load(Ordering::Acquire) {
            return Ok(());
        }
        self.inner
            .ready
            .clone()
            .wait_for(|ready| *ready)
            .await
            .map(|_| ())
            .map_err(|_| io::Error::other("certificate manager stopped"))
    }

    /// Returns the configuration that serves visitors.
    pub fn server_config(&self) -> Arc<ServerConfig> {
        Arc::clone(&self.inner.regular)
    }

    /// Returns the configuration that answers TLS-ALPN-01 challenges.
    pub fn challenge_config(&self) -> Arc<ServerConfig> {
        Arc::clone(&self.inner.challenge)
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.task.abort();
    }
}
