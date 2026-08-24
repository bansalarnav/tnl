use std::{
    fs,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::PathBuf,
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub domain: String,
    pub public_ip: IpAddr,
    pub listen_port: u16,
    #[serde(default)]
    pub clients: Vec<Client>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Client {
    pub name: String,
    pub token_hash: String,
}

impl Config {
    pub fn api_url(&self) -> String {
        let port = match self.listen_port {
            443 => String::new(),
            port => format!(":{port}"),
        };
        format!("https://{}{port}", self.domain)
    }

    pub fn local_api_address(&self) -> SocketAddr {
        let ip = if self.public_ip.is_ipv4() {
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        } else {
            IpAddr::V6(Ipv6Addr::LOCALHOST)
        };
        SocketAddr::new(ip, self.listen_port)
    }

    pub fn path() -> Result<PathBuf> {
        Ok(dirs::home_dir()
            .context("could not determine the home directory")?
            .join(".tnld/config.json"))
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

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    use super::Config;

    #[test]
    fn builds_public_url_and_local_connection_address() {
        let ipv4 = Config {
            domain: "tnl.example.com".to_owned(),
            public_ip: IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)),
            listen_port: 443,
            clients: Vec::new(),
        };
        assert_eq!(ipv4.api_url(), "https://tnl.example.com");
        assert_eq!(
            ipv4.local_api_address(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 443)
        );

        let ipv6 = Config {
            public_ip: IpAddr::V6(Ipv6Addr::LOCALHOST),
            listen_port: 8443,
            ..ipv4
        };
        assert_eq!(ipv6.api_url(), "https://tnl.example.com:8443");
        assert_eq!(
            ipv6.local_api_address(),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 8443)
        );
    }
}
