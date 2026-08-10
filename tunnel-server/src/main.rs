pub mod config;
mod server;
mod setup;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about = "Tunnel server")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Setup,
    Start {
        #[arg(long)]
        background: bool,
    },
    Stop,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Setup => setup::run(),
        Command::Start { background } => server::start(background).await,
        Command::Stop => server::stop(),
    }
}
