use std::{fs, path::PathBuf};

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};

const LOGIN_BLOB_PREFIX: &str = "tunnel-login-v1.";

#[derive(Parser)]
#[command(version, about = "Tunnel client")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Login { blob: String },
}

#[derive(Deserialize)]
struct LoginPayload {
    api_url: String,
    token: String,
}

#[derive(Serialize)]
struct Config {
    api_url: String,
    token: String,
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Login { blob } => login(&blob),
    }
}

fn login(blob: &str) -> Result<()> {
    let encoded = blob
        .trim()
        .strip_prefix(LOGIN_BLOB_PREFIX)
        .with_context(|| format!("not a valid {LOGIN_BLOB_PREFIX}* login blob"))?;
    let json = URL_SAFE_NO_PAD
        .decode(encoded)
        .with_context(|| format!("not a valid {LOGIN_BLOB_PREFIX}* login blob"))?;
    let payload: LoginPayload = serde_json::from_slice(&json)
        .with_context(|| format!("not a valid {LOGIN_BLOB_PREFIX}* login blob"))?;

    let api_url =
        url::Url::parse(&payload.api_url).context("login blob contains an invalid API URL")?;
    if api_url.scheme() != "https" || api_url.host().is_none() {
        bail!("login blob API URL must be an HTTPS URL with a host");
    }
    if payload.token.is_empty() {
        bail!("login blob contains an empty token");
    }

    let path = config_path()?;
    let directory = path
        .parent()
        .context("client config path does not have a parent directory")?;
    fs::create_dir_all(directory)
        .with_context(|| format!("could not create {}", directory.display()))?;
    let config = Config {
        api_url: payload.api_url,
        token: payload.token,
    };
    let json =
        serde_json::to_string_pretty(&config).context("could not serialize client config")?;
    fs::write(&path, format!("{json}\n"))
        .with_context(|| format!("could not write config to {}", path.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    }

    println!("Logged in to {}", config.api_url);
    println!("Configuration saved to {}", path.display());
    Ok(())
}

fn config_path() -> Result<PathBuf> {
    Ok(dirs::home_dir()
        .context("could not determine the home directory")?
        .join(".tunnel/config.json"))
}
