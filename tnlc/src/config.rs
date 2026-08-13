use std::{fs, path::PathBuf};

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use url::Url;

const LOGIN_BLOB_PREFIX: &str = "tnl-login-v1.";

#[derive(Deserialize)]
struct LoginPayload {
    api_url: String,
    token: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Config {
    pub api_url: String,
    pub token: String,
}

pub fn login(blob: &str) -> Result<()> {
    let encoded = blob
        .trim()
        .strip_prefix(LOGIN_BLOB_PREFIX)
        .with_context(|| format!("not a valid {LOGIN_BLOB_PREFIX}* login blob"))?;
    let json = URL_SAFE_NO_PAD
        .decode(encoded)
        .with_context(|| format!("not a valid {LOGIN_BLOB_PREFIX}* login blob"))?;
    let payload: LoginPayload = serde_json::from_slice(&json)
        .with_context(|| format!("not a valid {LOGIN_BLOB_PREFIX}* login blob"))?;

    let api_url = Url::parse(&payload.api_url).context("login blob contains an invalid API URL")?;
    if api_url.scheme() != "https" || api_url.host().is_none() {
        bail!("login blob API URL must be an HTTPS URL with a host");
    }
    if payload.token.is_empty() || payload.token.chars().any(char::is_control) {
        bail!("login blob contains an invalid token");
    }

    let path = path()?;
    let directory = path
        .parent()
        .context("tnl config path does not have a parent directory")?;
    fs::create_dir_all(directory)
        .with_context(|| format!("could not create {}", directory.display()))?;
    let config = Config {
        api_url: payload.api_url,
        token: payload.token,
    };
    let json = serde_json::to_string_pretty(&config).context("could not serialize tnl config")?;
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

pub fn read() -> Result<Config> {
    let path = path()?;
    let json = fs::read_to_string(&path).with_context(|| {
        format!(
            "could not read config from {}; log in first",
            path.display()
        )
    })?;
    serde_json::from_str(&json)
        .with_context(|| format!("could not parse config from {}", path.display()))
}

pub fn path() -> Result<PathBuf> {
    Ok(dirs::home_dir()
        .context("could not determine the home directory")?
        .join(".tnl/config.json"))
}
