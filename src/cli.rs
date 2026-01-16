use clap::{Subcommand, Parser};

#[derive(Parser)]
#[command(name = env!("CARGO_PKG_NAME"))]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = env!("CARGO_PKG_DESCRIPTION"))]
#[command(author = env!("CARGO_PKG_AUTHORS"))]
pub struct Cli {
    /// Sets custom log level
    #[arg(long, short = 'L', env = "RUST_LOG", default_value_t = String::from("goop=info"))]
    pub logging: String,

    #[clap(subcommand)]
    pub cmd: Command
}

#[derive(Subcommand, Clone)]
pub enum Command {
    /// Start goop daemon
    Daemon
}