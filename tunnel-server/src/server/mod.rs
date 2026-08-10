mod api;
mod http;
mod tls;
mod tunnel;

#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::{
    fs::{self, File},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    process::{Command, Stdio},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use rustls::ServerConfig;
use tokio::net::TcpListener;

use crate::config::Config;

fn state_directory() -> Result<PathBuf> {
    Config::path()?
        .parent()
        .map(PathBuf::from)
        .context("config path does not have a parent directory")
}

fn pid_path() -> Result<PathBuf> {
    Ok(state_directory()?.join("server.pid"))
}

fn process_is_running(pid: u32) -> bool {
    // Sending signal 0 checks whether the process exists without changing it.
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

fn read_running_pid() -> Result<Option<u32>> {
    let path = pid_path()?;
    let pid = match fs::read_to_string(&path) {
        Ok(value) => value
            .trim()
            .parse::<u32>()
            .with_context(|| format!("invalid PID in {}", path.display()))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("could not read {}", path.display()));
        }
    };

    if process_is_running(pid) {
        Ok(Some(pid))
    } else {
        fs::remove_file(path)?;
        Ok(None)
    }
}

fn get_config() -> Result<Config> {
    let path = Config::path()?;
    if !path.exists() {
        bail!("Run setup first");
    }

    Config::get()
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    domain: &str,
    api_tls_config: Option<Arc<ServerConfig>>,
) -> Result<()> {
    let connection = match tls::inspect(stream).await? {
        tls::Inspection::Plain(stream) => return http::serve(stream, api::router()).await,
        tls::Inspection::Tls(connection) => connection,
    };

    let Some(server_name) = connection.server_name() else {
        bail!("TLS connection did not include a server name");
    };
    let server_name = server_name.to_owned();
    let domain = domain.trim_end_matches('.').to_ascii_lowercase();

    if server_name == domain {
        let Some(config) = api_tls_config else {
            bail!("TLS termination for the server API is not configured yet");
        };
        let stream = connection.terminate(config).await?;
        return http::serve(stream, api::router()).await;
    }

    let Some(tunnel_id) = server_name.strip_suffix(&format!(".{domain}")) else {
        bail!("connection used an unknown server name: {server_name}");
    };
    if tunnel_id.is_empty() || tunnel_id.contains('.') {
        bail!("invalid tunnel server name: {server_name}");
    }

    tunnel::forward(tunnel_id, connection).await
}

pub async fn start(background: bool) -> Result<()> {
    let config = get_config()?;

    if background {
        if let Some(pid) = read_running_pid()? {
            bail!("server is already running in the background with PID {pid}");
        }

        let directory = state_directory()?;
        fs::create_dir_all(&directory)?;
        let log_path = directory.join("server.log");
        let stdout = File::create(&log_path)
            .with_context(|| format!("could not create {}", log_path.display()))?;
        let stderr = stdout.try_clone()?;

        let mut command = Command::new(std::env::current_exe()?);
        command
            .arg("start")
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        #[cfg(unix)]
        command.process_group(0);

        let child = command
            .spawn()
            .context("could not start server in the background")?;

        fs::write(pid_path()?, format!("{}\n", child.id()))?;
        println!(
            "Server started on 0.0.0.0:{} with PID {}",
            config.listen_port,
            child.id()
        );
        println!("Logs: {}", log_path.display());
        return Ok(());
    }

    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.listen_port);
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("could not listen on {address}"))?;

    println!("Server listening on {address}");

    // Setup does not provision an API certificate yet. Once it does, load its
    // Rustls ServerConfig here; wildcard connections will continue to bypass it.
    let api_tls_config: Option<Arc<ServerConfig>> = None;

    loop {
        let (stream, peer_address) = listener.accept().await?;
        let domain = config.domain.clone();
        let api_tls_config = api_tls_config.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, &domain, api_tls_config).await {
                eprintln!("connection from {peer_address} failed: {error:#}");
            }
        });
    }
}

pub fn stop() -> Result<()> {
    let Some(pid) = read_running_pid()? else {
        bail!("server is not running in the background");
    };

    let result = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    if result != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("could not stop process {pid}"));
    }

    fs::remove_file(pid_path()?)?;
    println!("Server stopped (PID {pid})");
    Ok(())
}
