mod config;
mod consts;
mod swarm;

mod cli;
use cli::Cli;

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

    let (blockstore, state, swarm) = swarm::init_swarm().await?;

    tracing::info!("initialized swarm");

    let token = CancellationToken::new();

    let exit = signal::ctrl_c();
    let swarm_task = task::spawn(swarm::spawn(token.child_token(), blockstore, state, swarm));

    tracing::info!("started daemon, press ctrl+c to exit...");

    select! {
        exit_res = exit => exit_res?,
        swarm_res = swarm_task => swarm_res??
    }

    tracing::info!("quitting, press ctrl+c again to exit immediately...");

    token.cancel();

    Ok(())
}
