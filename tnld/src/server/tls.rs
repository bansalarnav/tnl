use std::{fs, path::PathBuf, sync::Arc, time::Duration};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use anyhow::{Context as _, Result};
use rustls::ServerConfig;
use rustls_acme::{AcmeConfig, EventError, caches::DirCache};
use tokio_stream::StreamExt;

const ACME_ORDER_RETRY_DELAY: Duration = Duration::from_secs(60 * 60);

pub struct Configs {
    pub api: Arc<ServerConfig>,
    pub acme_challenge: Arc<ServerConfig>,
}

pub fn manage_certificate(domain: &str, cache_directory: PathBuf) -> Result<Configs> {
    fs::create_dir_all(&cache_directory).with_context(|| {
        format!(
            "could not create ACME cache directory {}",
            cache_directory.display()
        )
    })?;
    #[cfg(unix)]
    fs::set_permissions(&cache_directory, fs::Permissions::from_mode(0o700)).with_context(
        || {
            format!(
                "could not secure ACME cache directory {}",
                cache_directory.display()
            )
        },
    )?;

    let mut acme = AcmeConfig::new([domain.to_owned()])
        .cache(DirCache::new(cache_directory))
        .directory_lets_encrypt(true)
        .state();
    let configs = Configs {
        api: acme.default_rustls_config(),
        acme_challenge: acme.challenge_rustls_config(),
    };

    println!("Managing TLS certificate for {domain}");
    tokio::spawn(async move {
        while let Some(event) = acme.next().await {
            match event {
                Ok(event) => println!("ACME: {event:?}"),
                Err(error) => {
                    eprintln!("ACME error: {error:?}");
                    if matches!(error, EventError::Order(_)) {
                        eprintln!("Retrying certificate order in 1 hour");
                        tokio::time::sleep(ACME_ORDER_RETRY_DELAY).await;
                    }
                }
            }
        }
    });

    Ok(configs)
}
