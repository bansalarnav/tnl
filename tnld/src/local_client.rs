use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{Client, Config};

const LOCAL_CLIENT_NAME: &str = "tnld-local";

#[derive(Deserialize, Serialize)]
struct ClientConfig {
    api_url: String,
    token: String,
    connect_addr: String,
}

pub fn ensure() -> Result<()> {
    let mut server_config = Config::get()?;
    let client_path = client_config_path()?;
    let api_url = server_config.api_url();
    let connect_addr = server_config.local_api_address().to_string();

    if existing_config_works(&client_path, &api_url, &connect_addr, &server_config) {
        println!("tnlc is already configured for this server.");
        return Ok(());
    }

    let mut token_bytes = [0_u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut token_bytes);
    let token = URL_SAFE_NO_PAD.encode(token_bytes);
    let token_hash = hash_token(&token);

    if let Some(client) = server_config
        .clients
        .iter_mut()
        .find(|client| client.name == LOCAL_CLIENT_NAME)
    {
        client.token_hash = token_hash;
    } else {
        server_config.clients.push(Client {
            name: LOCAL_CLIENT_NAME.to_owned(),
            token_hash,
        });
    }
    server_config.write()?;

    write_client_config(
        &client_path,
        &ClientConfig {
            api_url,
            token,
            connect_addr,
        },
    )?;

    println!("Configured tnlc to use this server.");
    println!("Configuration saved to {}", client_path.display());
    Ok(())
}

fn existing_config_works(
    path: &Path,
    api_url: &str,
    connect_addr: &str,
    server_config: &Config,
) -> bool {
    let Ok(json) = fs::read_to_string(path) else {
        return false;
    };
    let Ok(client_config) = serde_json::from_str::<ClientConfig>(&json) else {
        return false;
    };

    client_config.api_url == api_url
        && client_config.connect_addr == connect_addr
        && server_config
            .clients
            .iter()
            .any(|client| client.token_hash == hash_token(&client_config.token))
}

fn write_client_config(path: &Path, config: &ClientConfig) -> Result<()> {
    let directory = path
        .parent()
        .context("tnlc config path does not have a parent directory")?;
    fs::create_dir_all(directory)
        .with_context(|| format!("could not create {}", directory.display()))?;
    let json = serde_json::to_string_pretty(config).context("could not serialize tnlc config")?;
    fs::write(path, format!("{json}\n"))
        .with_context(|| format!("could not write config to {}", path.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }

    Ok(())
}

fn client_config_path() -> Result<PathBuf> {
    Ok(dirs::home_dir()
        .context("could not determine the home directory")?
        .join(".tnl/config.json"))
}

fn hash_token(token: &str) -> String {
    format!("{:x}", Sha256::digest(token.as_bytes()))
}
