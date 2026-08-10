use clap::Parser;

#[derive(Parser)]
#[command(version, about = "Tunnel client")]
struct Cli;

fn main() {
    Cli::parse();

    println!("Hello from the client!");
}
