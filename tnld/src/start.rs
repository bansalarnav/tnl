use std::{
    collections::HashMap,
    fs,
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::SystemTime,
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use anyhow::{Context, Result, bail};
use rustls::{
    CipherSuite,
    crypto::{CryptoProvider, ring},
};
use rustls_acme::{AcmeConfig, caches::DirCache};
use tnl_relay::{Authenticator, ClientId, Relay, RelayConfig};
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tracing::{info, warn};

use crate::config::{self, Config};

pub async fn run() -> Result<()> {
    let config_path = Config::path()?;
    if !config_path.exists() {
        bail!("Run setup first");
    }
    let config = Config::get()?;
    let domain = config.domain.trim_end_matches('.').to_ascii_lowercase();

    let mut acme = AcmeConfig::new([domain.clone()])
        .cache(DirCache::new(acme_cache_directory(&config_path)?))
        .directory_lets_encrypt(true)
        .state();
    let relay_config = RelayConfig::new(
        &domain,
        acme.default_rustls_config_with_provider(crypto_provider()),
    )
    .acme_challenge_tls(acme.challenge_rustls_config());
    info!("Managing TLS certificate for {domain}");
    tokio::spawn(async move {
        while let Some(event) = acme.next().await {
            match event {
                Ok(event) => info!("ACME: {event:?}"),
                Err(error) => warn!("ACME error: {error:?}"),
            }
        }
    });

    let relay = Relay::new(relay_config, ConfigAuthenticator::new(config_path));
    let address = SocketAddr::from((Ipv4Addr::UNSPECIFIED, config.listen_port));
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("could not listen on {address}"))?;
    info!("Server listening on {address}");
    relay.serve(listener).await?;
    Ok(())
}

fn acme_cache_directory(config_path: &Path) -> Result<PathBuf> {
    let directory = config_path
        .parent()
        .context("config path does not have a parent directory")?
        .join("acme");
    fs::create_dir_all(&directory).with_context(|| {
        format!(
            "could not create ACME cache directory {}",
            directory.display()
        )
    })?;
    #[cfg(unix)]
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).with_context(|| {
        format!(
            "could not secure ACME cache directory {}",
            directory.display()
        )
    })?;
    Ok(directory)
}

/// Prefers AES-128-GCM for API and control-session TLS.
fn crypto_provider() -> Arc<CryptoProvider> {
    let mut provider = ring::default_provider();
    provider
        .cipher_suites
        .sort_by_key(|suite| suite.suite() != CipherSuite::TLS13_AES_128_GCM_SHA256);
    Arc::new(provider)
}

/// Authenticates against the clients in the config file, rereading it after
/// it changes so `invite-client` takes effect without a restart.
struct ConfigAuthenticator {
    path: PathBuf,
    cache: Mutex<ClientCache>,
}

#[derive(Default)]
struct ClientCache {
    modified: Option<SystemTime>,
    /// Client names by token hash.
    clients: HashMap<String, ClientId>,
}

impl ConfigAuthenticator {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            cache: Mutex::default(),
        }
    }
}

impl Authenticator for ConfigAuthenticator {
    fn authenticate(&self, token: &str) -> Option<ClientId> {
        let modified = fs::metadata(&self.path)
            .and_then(|metadata| metadata.modified())
            .ok();
        let mut cache = self.cache.lock().expect("client cache lock was poisoned");
        if modified.is_none() || cache.modified != modified {
            match Config::get() {
                Ok(config) => {
                    *cache = ClientCache {
                        modified,
                        clients: config
                            .clients
                            .into_iter()
                            .map(|client| (client.token_hash, ClientId::new(client.name)))
                            .collect(),
                    }
                }
                Err(error) => {
                    warn!("could not reload clients: {error:#}");
                    return None;
                }
            }
        }
        cache.clients.get(&config::hash_token(token)).cloned()
    }
}
