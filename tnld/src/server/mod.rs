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
    time::Duration,
};

use anyhow::{Context, Result, bail};
use axum::Router;
use hmac::{Hmac, Mac};
use rand::RngCore;
use rustls::ServerConfig;
use sha2::Sha256;
use socket2::{SockRef, TcpKeepalive};
use tnl::{
    SessionConfig, TunnelId,
    protocol::{
        TCP_DATA_CHALLENGE_LENGTH, TCP_DATA_PROOF_LENGTH, TCP_DATA_REGISTRATION_MAGIC,
        read_tcp_data_registration,
    },
    server::TunnelServer,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
    time::timeout,
};

use crate::config::Config;

const TCP_KEEPALIVE_IDLE: Duration = Duration::from_secs(30);
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const TCP_KEEPALIVE_RETRIES: u32 = 3;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(60);
const DATA_REGISTRATION_TIMEOUT: Duration = Duration::from_secs(10);

struct ServerState {
    domain: String,
    wildcard_suffix: String,
    api_tls_config: Arc<ServerConfig>,
    acme_tls_config: Arc<ServerConfig>,
    api_router: Router,
    tunnel_server: TunnelServer,
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
    let wildcard_suffix = format!(".{domain}");
    let certificate_cache = state_directory()?.join("acme");
    let tls_configs = tls::manage_certificate(&domain, certificate_cache)?;
    let tunnel_server =
        TunnelServer::new(SessionConfig::new().heartbeat(HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT));
    let api_router = api::router(tunnel_server.clone(), domain.clone());
    let state = Arc::new(ServerState {
        wildcard_suffix,
        domain: domain.clone(),
        api_tls_config: tls_configs.api,
        acme_tls_config: tls_configs.acme_challenge,
        api_router,
        tunnel_server: tunnel_server.clone(),
    });

    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.listen_port);
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("could not listen on {address}"))?;
    println!("Server listening on {address}");

    struct ShutdownOnDrop(TunnelServer);
    impl Drop for ShutdownOnDrop {
        fn drop(&mut self) {
            self.0.shutdown();
        }
    }
    let _shutdown = ShutdownOnDrop(tunnel_server);
    let mut connections = JoinSet::new();
    loop {
        let (stream, peer_address) = listener.accept().await?;
        if let Err(error) = stream.set_nodelay(true) {
            eprintln!("could not disable Nagle's algorithm for {peer_address}: {error}");
        }
        if let Err(error) = configure_tcp_keepalive(&stream) {
            eprintln!("could not configure TCP keepalive for {peer_address}: {error}");
        }
        let state = Arc::clone(&state);
        while connections.try_join_next().is_some() {}
        connections.spawn(async move {
            if let Err(error) = handle_connection(stream, &state).await
                && !is_routine_connection_error(&error)
            {
                eprintln!("connection from {peer_address} failed: {error:#}");
            }
        });
    }
}

async fn handle_connection(mut stream: TcpStream, state: &ServerState) -> Result<()> {
    let first_byte = stream
        .read_u8()
        .await
        .context("could not read the connection preface")?;
    if first_byte == TCP_DATA_REGISTRATION_MAGIC[0] {
        return timeout(
            DATA_REGISTRATION_TIMEOUT,
            register_tcp_data_transport(stream, first_byte, &state.tunnel_server),
        )
        .await
        .context("timed out registering a TCP data transport")?;
    }
    if first_byte != 22 {
        stream.shutdown().await?;
        return Ok(());
    }

    let connection = tls::inspect(stream, first_byte).await?;
    let Some(server_name) = connection.server_name() else {
        return Ok(());
    };

    if server_name == state.domain {
        if connection.is_acme_challenge() {
            let mut stream = connection
                .terminate(Arc::clone(&state.acme_tls_config))
                .await?;
            stream.shutdown().await?;
            return Ok(());
        }
        let stream = connection
            .terminate(Arc::clone(&state.api_tls_config))
            .await?;
        return http::serve(stream, state.api_router.clone()).await;
    }

    let Some(tunnel_id) = server_name.strip_suffix(&state.wildcard_suffix) else {
        return Ok(());
    };
    if tunnel_id.contains('.') {
        return Ok(());
    }
    let Ok(tunnel_id) = TunnelId::new(tunnel_id) else {
        return Ok(());
    };
    tunnel::forward(&state.tunnel_server, &tunnel_id, connection).await
}

async fn register_tcp_data_transport(
    mut stream: TcpStream,
    first_byte: u8,
    tunnel_server: &TunnelServer,
) -> Result<()> {
    let tunnel_id = read_tcp_data_registration(&mut stream, first_byte)
        .await
        .context("invalid TCP data registration")?;
    let owner = tunnel_server
        .transport_owner(&tunnel_id)
        .context("tunnel control connection is not registered")?;

    let mut challenge = [0; TCP_DATA_CHALLENGE_LENGTH];
    rand::thread_rng().fill_bytes(&mut challenge);
    stream.write_all(&challenge).await?;
    stream.flush().await?;

    let mut proof = [0; TCP_DATA_PROOF_LENGTH];
    stream.read_exact(&mut proof).await?;
    let mut mac =
        Hmac::<Sha256>::new_from_slice(owner.as_bytes()).expect("HMAC accepts keys of any size");
    mac.update(TCP_DATA_REGISTRATION_MAGIC);
    mac.update(tunnel_id.as_str().as_bytes());
    mac.update(&challenge);
    if mac.verify_slice(&proof).is_err() {
        stream.write_u8(1).await?;
        bail!("invalid TCP data registration proof");
    }

    stream.write_u8(0).await?;
    stream.flush().await?;
    tunnel_server.register_transport(&tunnel_id, owner, stream)?;
    Ok(())
}

fn configure_tcp_keepalive(stream: &TcpStream) -> io::Result<()> {
    SockRef::from(stream).set_tcp_keepalive(
        &TcpKeepalive::new()
            .with_time(TCP_KEEPALIVE_IDLE)
            .with_interval(TCP_KEEPALIVE_INTERVAL)
            .with_retries(TCP_KEEPALIVE_RETRIES),
    )
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
