use std::{io::Write, net::IpAddr, time::Duration};

use anyhow::Result;

use crate::config::Config;

fn prompt(message: &str) -> Result<String> {
    print!("{message}");
    std::io::stdout().flush()?;

    let mut value = String::new();
    std::io::stdin().read_line(&mut value)?;
    Ok(value.trim().to_owned())
}

fn detect_public_ip() -> Option<IpAddr> {
    let config = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(5)))
        .https_only(true)
        .build();
    let agent = ureq::Agent::new_with_config(config);

    ["https://api.ipify.org", "https://api64.ipify.org"]
        .into_iter()
        .find_map(|url| {
            let mut response = agent.get(url).call().ok()?;
            response
                .body_mut()
                .read_to_string()
                .ok()?
                .trim()
                .parse()
                .ok()
        })
}

pub fn run() -> Result<()> {
    println!("Detecting public IP address...");
    let public_ip = match detect_public_ip() {
        Some(ip) => {
            println!("Public IP: {ip}");
            ip
        }
        None => loop {
            let value = prompt("Could not detect the public IP. Enter it manually: ")?;

            match value.parse::<IpAddr>() {
                Ok(ip) => break ip,
                Err(_) => println!("Please enter a valid IPv4 or IPv6 address."),
            }
        },
    };

    let domain = loop {
        let value = prompt("Base domain [nip.io]: ")?;
        let value = if value.is_empty() {
            "nip.io".to_owned()
        } else {
            value.trim_end_matches('.').to_owned()
        };

        if value == "nip.io" && public_ip.is_ipv6() {
            println!("nip.io setup requires an IPv4 address. Enter a custom domain.");
        } else if value.contains(char::is_whitespace) || value.contains('*') || !value.contains('.')
        {
            println!("Please enter a valid base domain.");
        } else {
            break value;
        }
    };

    let listen_port = loop {
        let value = prompt("Listen port [443]: ")?;
        let value = if value.is_empty() { "443" } else { &value };

        match value.parse::<u16>() {
            Ok(port) if port != 0 => break port,
            _ => println!("Please enter a port between 1 and 65535."),
        }
    };

    let uses_nip_io = domain == "nip.io";
    let tunnel_domain = if uses_nip_io {
        format!("tnl-{}.nip.io", public_ip.to_string().replace('.', "-"))
    } else {
        domain
    };
    let listen_address = if public_ip.is_ipv4() {
        format!("0.0.0.0:{listen_port}")
    } else {
        format!("[::]:{listen_port}")
    };
    let port_suffix = if listen_port == 443 {
        String::new()
    } else {
        format!(":{listen_port}")
    };

    println!("\nBrowser URL: https://<tunnel-id>.{tunnel_domain}{port_suffix}");
    println!("Listen address: {listen_address}");

    if uses_nip_io {
        println!("\nDNS records: none");
        println!("nip.io resolves *.{tunnel_domain} to {public_ip} automatically.");
    } else {
        let record_type = if public_ip.is_ipv4() { "A" } else { "AAAA" };
        println!("\nDNS records to add:");
        println!("Type:  {record_type}");
        println!("Name:  {tunnel_domain}");
        println!("Value: {public_ip}");
        println!();
        println!("Type:  CNAME");
        println!("Name:  *.{tunnel_domain}");
        println!("Value: {tunnel_domain}");
    }

    if listen_port == 443 {
        println!("\nMake sure TCP port 443 is publicly accessible.");
    } else {
        println!(
            "\nMake sure TCP port 443 is publicly accessible and correctly forwards to port {listen_port}."
        );
    }
    println!(
        "Make sure DNS proxying is disabled. If a proxy terminates TLS, the tunnel will not be end-to-end encrypted."
    );

    let config = Config {
        domain: tunnel_domain,
        public_ip,
        listen_port,
        clients: Vec::new(),
    };
    config.write()?;

    println!("Configuration saved to {}", Config::path()?.display());

    Ok(())
}
