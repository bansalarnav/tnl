use anyhow::{Result, bail};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::RngCore;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::config::{Client, Config};

const LOGIN_BLOB_PREFIX: &str = "tunnel-login-v1.";

#[derive(Serialize)]
struct LoginPayload {
    api_url: String,
    token: String,
}

pub fn run(name: &str) -> Result<()> {
    let name = name.trim();
    if name.is_empty() {
        bail!("client name cannot be empty");
    }

    let mut config = Config::get()?;
    let mut token_bytes = [0_u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut token_bytes);
    let token = URL_SAFE_NO_PAD.encode(token_bytes);
    let token_hash = format!("{:x}", Sha256::digest(token.as_bytes()));

    if let Some(client) = config.clients.iter_mut().find(|client| client.name == name) {
        client.token_hash = token_hash;
    } else {
        config.clients.push(Client {
            name: name.to_owned(),
            token_hash,
        });
    }
    config.write()?;

    let port = match config.listen_port {
        443 => String::new(),
        port => format!(":{port}"),
    };
    let payload = LoginPayload {
        api_url: format!("https://{}{port}", config.domain),
        token,
    };
    let json = serde_json::to_vec(&payload)?;
    let blob = format!("{LOGIN_BLOB_PREFIX}{}", URL_SAFE_NO_PAD.encode(json));

    println!("Client {name} can log in with:");
    println!("{blob}");
    Ok(())
}
