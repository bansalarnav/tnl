mod protocol;
mod raw;
mod s2n;
mod tokio_quiche_server;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::net::SocketAddr;

#[derive(Parser)]
#[command(about = "Comparable QUIC stream server benchmark")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// The common raw-quiche load generator used against every server.
    Client {
        #[arg(long)]
        addr: SocketAddr,
        #[arg(long, value_enum)]
        workload: protocol::Workload,
        #[arg(long, default_value_t = 1)]
        streams: usize,
        #[arg(long, default_value_t = 256 * 1024 * 1024)]
        bytes_per_stream: u64,
        #[arg(long, default_value_t = 1)]
        repetitions: usize,
    },
    /// Hand-driven quiche reference server.
    Quiche {
        #[arg(long)]
        addr: SocketAddr,
    },
    /// Cloudflare's Tokio I/O wrapper around the same quiche engine.
    TokioQuiche {
        #[arg(long)]
        addr: SocketAddr,
    },
    /// AWS s2n-quic server.
    S2nQuic {
        #[arg(long)]
        addr: SocketAddr,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Client {
            addr,
            workload,
            streams,
            bytes_per_stream,
            repetitions,
        } => raw::run_client(addr, workload, streams, bytes_per_stream, repetitions),
        Command::Quiche { addr } => raw::run_server(addr),
        Command::TokioQuiche { addr } => {
            tokio_quiche_server::runtime()?.block_on(tokio_quiche_server::run(addr))
        }
        Command::S2nQuic { addr } => s2n::runtime()?.block_on(s2n::run(addr)),
    }
}
