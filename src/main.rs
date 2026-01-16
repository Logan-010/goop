mod api;
mod consts;
mod swarm;

mod config;
use config::{CONFIG, Config};

mod cli;
use cli::{Cli, Command};

use clap::Parser;
use tokio::{select, signal, task};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    let cli = Cli::parse();

    color_eyre::install()?;

    if tracing_subscriber::registry()
        .with(EnvFilter::new(&cli.logging))
        .with(tracing_subscriber::fmt::layer())
        .try_init()
        .is_err()
    {
        eprintln!("failed to initialize logger");
    }

    let config = Config::new().await?;
    CONFIG.set(config)?;

    match cli.cmd {
        Command::Daemon => {
            let token = CancellationToken::new();

            let exit = signal::ctrl_c();
            let swarm_task = task::spawn(swarm::spawn(token.child_token()));
            let api_task = task::spawn(api::spawn(token.child_token()));

            tracing::info!("started daemon, press ctrl+c to exit...");

            select! {
                exit_res = exit => exit_res?,
                swarm_res = swarm_task => swarm_res??,
                api_res = api_task => api_res??
            }

            tracing::info!("quitting, press ctrl+c again to exit immediately...");

            token.cancel();
        }
    }

    Ok(())
}
