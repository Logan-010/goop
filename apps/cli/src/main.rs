mod api;
mod cli;
mod config;
mod consts;
mod keystore;
mod swarm;

use clap::Parser;
use cli::{Cli, Command};
use config::{CONFIG, Config};
use redb::Database;
use std::time::Duration;
use tokio::{select, signal, sync::mpsc, task, time::sleep};
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

    match cli.cmd {
        Command::Daemon => {
            let config = Config::new().await?;
            CONFIG.set(config)?;

            let token = CancellationToken::new();

            let (tx, rx) = mpsc::unbounded_channel();

            let exit = signal::ctrl_c();
            let swarm_task = task::spawn(swarm::spawn(token.child_token(), rx));
            let api_task = task::spawn(api::spawn(token.child_token(), tx));

            tracing::info!("started daemon, press ctrl+c to exit...");

            select! {
                exit_res = exit => exit_res?,
                swarm_res = swarm_task => swarm_res??,
                api_res = api_task => api_res??
            }

            tracing::info!("quitting, press ctrl+c again to exit immediately...");

            token.cancel();

            // Wait for tasks to finish and close connection to database before trying to compact
            sleep(Duration::from_millis(500)).await;

            task::spawn_blocking(|| {
                let cfg = CONFIG.get().expect("expected config");

                while Database::open(&cfg.blockstore_path)?.compact()? {}
                while Database::open(&cfg.kadstore_path)?.compact()? {}
                while Database::open(&cfg.keystore_path)?.compact()? {}

                Result::<(), color_eyre::Report>::Ok(())
            })
            .await??;
        }
    }

    Ok(())
}
