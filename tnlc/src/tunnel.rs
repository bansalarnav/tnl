use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::{Context, Result, bail};
use rand::{Rng, distributions::Alphanumeric};
use tnl::{
    TunnelId,
    client::{Client, ClientEvent, Forwarder},
};
use url::Url;

use crate::config;

pub async fn expose(port: u16, name: Option<String>) -> Result<()> {
    if port == 0 {
        bail!("port must be between 1 and 65535");
    }

    let config = config::read()?;
    let tunnel_id = match name {
        Some(name) => TunnelId::new(name.to_ascii_lowercase())?,
        None => default_tunnel_id()?,
    };
    let api_url = Url::parse(&config.api_url).context("config contains an invalid API URL")?;
    let client = Client::new(api_url)?;
    if config.token.is_empty() || config.token.chars().any(char::is_control) {
        bail!("config contains an invalid token");
    }
    let cache_directory = config::path()?
        .parent()
        .context("tnlc config path does not have a parent directory")?
        .join("acme");
    let client = client.with_authorization(format!("Bearer {}", config.token))?;
    let forwarder = Forwarder::new(client, cache_directory).with_event_handler(print_event);
    let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);

    forwarder.expose(target, tunnel_id).await
}

fn print_event(event: ClientEvent) {
    match event {
        ClientEvent::ObtainingCertificate { hostname } => {
            println!("Obtaining a TLS certificate for {hostname}...");
        }
        ClientEvent::Ready { url, target } => {
            println!("Forwarding {url} to http://{target}");
        }
        ClientEvent::Reconnected { url } => println!("Reconnected {url}"),
        ClientEvent::Disconnected { error, retry_in } => eprintln!(
            "Tunnel connection lost: {error}\nReconnecting in {:.1}s...",
            retry_in.as_secs_f32()
        ),
        ClientEvent::ConnectionFailed {
            connection_id,
            error,
        } => eprintln!("connection {connection_id} failed: {error}"),
        ClientEvent::CertificateError {
            hostname,
            error,
            retry_in,
        } => {
            eprintln!("TLS certificate error for {hostname}: {error}");
            if let Some(delay) = retry_in {
                eprintln!(
                    "Retrying certificate order for {hostname} in {:.0} minutes",
                    delay.as_secs_f32() / 60.0
                );
            }
        }
    }
}

fn default_tunnel_id() -> Result<TunnelId> {
    let directory = std::env::current_dir().context("could not determine current directory")?;
    let name = directory
        .file_name()
        .and_then(|name| name.to_str())
        .map(sanitize_tunnel_id)
        .filter(|name| !name.is_empty());

    let name = name.unwrap_or_else(|| {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .filter(|byte| byte.is_ascii_alphanumeric())
            .take(8)
            .map(char::from)
            .collect::<String>()
            .to_ascii_lowercase()
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
