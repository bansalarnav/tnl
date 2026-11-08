use std::{
    net::{Ipv4Addr, SocketAddr},
    str::FromStr,
};

use anyhow::{Context, Result, bail};
use rand::{Rng, distributions::Alphanumeric};
use tnl_node::{ForwardTls, NodeConfig, Tunnel, TunnelCertificate, tnl_core::TunnelId};
use url::Url;

use crate::config;

pub async fn run(port: u16, name: Option<String>) -> Result<()> {
    if port == 0 {
        bail!("port must be between 1 and 65535");
    }
    let tunnel_id = match name {
        Some(name) => TunnelId::new(name.to_ascii_lowercase())?,
        None => default_tunnel_id()?,
    };

    let config = config::read()?;
    let api_url = Url::parse(&config.api_url).context("config contains an invalid API URL")?;
    let mut node_config = NodeConfig::new(api_url, config.token).context("invalid config")?;
    if let Some(address) = config.connect_addr {
        node_config = node_config.connect_addr(
            SocketAddr::from_str(&address).context("config contains an invalid connect address")?,
        );
    }
    let cache_directory = config::path()?
        .parent()
        .context("tnlc config path does not have a parent directory")?
        .join("acme");
    let target = SocketAddr::from((Ipv4Addr::LOCALHOST, port));

    let tunnel = Tunnel::register(node_config, tunnel_id).await?;
    println!("Obtaining a TLS certificate for {}...", tunnel.hostname());
    let certificate = TunnelCertificate::start(tunnel.hostname(), cache_directory)
        .context("could not create the TLS certificate cache")?;
    tokio::spawn({
        let certificate = certificate.clone();
        let url = tunnel.url().to_owned();
        async move {
            if certificate.ready().await.is_ok() {
                println!("Forwarding {url} to http://{target}");
            }
        }
    });

    Err(tunnel
        .serve(ForwardTls::new(certificate, target))
        .await
        .into())
}

/// Names the tunnel after the current directory, or randomly if that is not a
/// usable name.
fn default_tunnel_id() -> Result<TunnelId> {
    let directory = std::env::current_dir().context("could not determine current directory")?;
    let name = directory
        .file_name()
        .and_then(|name| name.to_str())
        .map(sanitize_tunnel_id)
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| {
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(8)
                .map(|byte| char::from(byte).to_ascii_lowercase())
                .collect()
        });
    Ok(TunnelId::new(name)?)
}

fn sanitize_tunnel_id(value: &str) -> String {
    let mut result = String::new();
    let mut previous_was_separator = false;
    for character in value.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_lowercase() || character.is_ascii_digit() {
            result.push(character);
            previous_was_separator = false;
        } else if !previous_was_separator && !result.is_empty() {
            result.push('-');
            previous_was_separator = true;
        }
    }
    result.truncate(63);
    result.trim_end_matches('-').to_owned()
}

#[cfg(test)]
mod tests {
    use super::sanitize_tunnel_id;

    #[test]
    fn sanitizes_directory_names() {
        assert_eq!(sanitize_tunnel_id("My App_v2"), "my-app-v2");
        assert_eq!(sanitize_tunnel_id("--x--"), "x");
        assert_eq!(sanitize_tunnel_id("日本"), "");
        assert_eq!(sanitize_tunnel_id(&"a".repeat(70)).len(), 63);
    }
}
