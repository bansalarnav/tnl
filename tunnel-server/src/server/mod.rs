mod api;

#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::{
    fs::{self, File},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    process::{Command, Stdio},
};

use anyhow::{Context, Result, bail};
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

    println!("Server listening on http://{address}");
    axum::serve(listener, api::router()).await?;
    Ok(())
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
