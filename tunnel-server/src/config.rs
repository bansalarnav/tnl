use std::{fs, net::IpAddr, path::PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub domain: String,
    pub public_ip: IpAddr,
    pub listen_port: u16,
}

impl Config {
    pub fn path() -> Result<PathBuf> {
        Ok(dirs::home_dir()
            .context("could not determine the home directory")?
            .join(".tunnel-server/config.json"))
    }

    pub fn get() -> Result<Self> {
        let path = Self::path()?;
        let json = fs::read_to_string(&path)
            .with_context(|| format!("could not read config from {}", path.display()))?;

        serde_json::from_str(&json)
            .with_context(|| format!("could not parse config from {}", path.display()))
    }

    pub fn write(&self) -> Result<()> {
        let path = Self::path()?;
        let directory = path
            .parent()
            .context("config path does not have a parent directory")?;
        fs::create_dir_all(directory).with_context(|| {
            format!("could not create config directory {}", directory.display())
        })?;

        let json = serde_json::to_string_pretty(self).context("could not serialize config")?;
        fs::write(&path, format!("{json}\n"))
            .with_context(|| format!("could not write config to {}", path.display()))
    }

    pub fn update(update: impl FnOnce(&mut Self)) -> Result<Self> {
        let mut config = Self::get()?;
        update(&mut config);
        config.write()?;
        Ok(config)
    }
}
