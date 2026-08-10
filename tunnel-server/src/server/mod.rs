mod api;
mod http;
mod tls;
mod tunnel;

#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::{
    fs::{self, File},
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    process::{Command, Stdio},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use axum::Router;
use rustls::ServerConfig;
use tokio::{io::AsyncWriteExt, net::TcpListener};

use crate::config::Config;

struct ServerState {
    domain: String,
    wildcard_suffix: String,
    api_tls_config: Arc<ServerConfig>,
    acme_tls_config: Arc<ServerConfig>,
    api_router: Router,
    tunnels: tunnel::TunnelRegistry,
}

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

async fn handle_connection(stream: tokio::net::TcpStream, state: &ServerState) -> Result<()> {
    let connection = match tls::inspect(stream).await? {
        Some(connection) => connection,
        None => return Ok(()),
    };

    let Some(server_name) = connection.server_name() else {
        return Ok(());
    };
    let domain = state.domain.as_str();

    if server_name == domain && connection.is_acme_challenge() {
        let mut stream = connection.terminate(state.acme_tls_config.clone()).await?;
        stream.shutdown().await?;
        return Ok(());
    }

    if server_name == domain {
        let stream = connection.terminate(state.api_tls_config.clone()).await?;
        return http::serve(stream, state.api_router.clone()).await;
    }

    let Some(tunnel_id) = server_name.strip_suffix(&state.wildcard_suffix) else {
        return Ok(());
    };
    if tunnel_id.is_empty() || tunnel_id.contains('.') {
        return Ok(());
    }
    let tunnel_id = tunnel_id.to_owned();

    state.tunnels.forward(&tunnel_id, connection).await
}

fn is_routine_connection_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if let Some(error) = cause.downcast_ref::<io::Error>() {
            return matches!(
                error.kind(),
                io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
            );
        }

        cause.downcast_ref::<rustls::Error>().is_some_and(|error| {
            matches!(
                error,
                rustls::Error::InappropriateMessage { .. }
                    | rustls::Error::InappropriateHandshakeMessage { .. }
                    | rustls::Error::InvalidMessage(_)
                    | rustls::Error::PeerIncompatible(_)
                    | rustls::Error::PeerMisbehaved(_)
                    | rustls::Error::AlertReceived(_)
                    | rustls::Error::NoApplicationProtocol
            )
        })
    })
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

    let domain = config.domain.trim_end_matches('.').to_ascii_lowercase();
    let tls_configs = tls::manage_certificate(&domain, state_directory()?.join("acme"))?;

    let tunnels = tunnel::TunnelRegistry::new(domain.clone());
    let state = Arc::new(ServerState {
        wildcard_suffix: format!(".{domain}"),
        domain,
        api_tls_config: tls_configs.api,
        acme_tls_config: tls_configs.acme_challenge,
        api_router: api::router(tunnels.clone()),
        tunnels,
    });

    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.listen_port);
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("could not listen on {address}"))?;

    println!("Server listening on {address}");

    loop {
        let (stream, peer_address) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, &state).await
                && !is_routine_connection_error(&error)
            {
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
