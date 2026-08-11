mod config;
mod tunnel;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    version,
    about = "Expose local HTTP services through your tnld server with tnlc"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Login {
        blob: String,
    },
    Expose {
        port: u16,
        #[arg(short, long)]
        name: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Login { blob } => config::login(&blob),
        Command::Expose { port, name } => tunnel::expose(port, name).await,
    }
}
