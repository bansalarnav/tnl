mod config;
mod invite_client;
mod local_client;
mod setup;
mod start;

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
    /// Configure this server interactively
    Setup,
    /// Configure tnlc on this machine to use this tnld server
    SetupClient,
    /// Run the server in the foreground
    Start,
    /// Create credentials for a tnlc client
    InviteClient { name: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_target(false)
        .init();

    match Cli::parse().command {
        Command::Setup => setup::run(),
        Command::SetupClient => local_client::ensure(),
        Command::Start => start::run().await,
        Command::InviteClient { name } => invite_client::run(&name),
    }
}
