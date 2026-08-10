pub mod config;
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
    /// Configure the public tunnel endpoint
    Setup,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Setup => setup::run(),
    }
}
