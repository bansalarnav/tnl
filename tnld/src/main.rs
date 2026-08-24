pub mod config;
mod invite_client;
mod local_client;
mod server;
mod setup;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about = "Self-hosted server for tnl")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Setup,
    /// Configure tnlc on this machine to use this tnld server
    SetupClient,
    Start {
        #[arg(long)]
        background: bool,
    },
    Stop,
    InviteClient {
        name: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Setup => setup::run(),
        Command::SetupClient => local_client::ensure(),
        Command::Start { background } => server::start(background).await,
        Command::Stop => server::stop(),
        Command::InviteClient { name } => invite_client::run(&name),
    }
}
