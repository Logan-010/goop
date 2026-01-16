use std::net::SocketAddr;
use super::routes;
use crate::config::CONFIG;
use tokio::{net::TcpListener, select};
use tokio_util::sync::CancellationToken;

pub async fn spawn(token: CancellationToken) -> color_eyre::Result<()> {
    let config = CONFIG.get().unwrap();

    select! {
        _ = token.cancelled() => Ok(()),
        task_res = task(config.api_address) => task_res
    }
}

async fn task(addr: SocketAddr) -> color_eyre::Result<()> {
    let listener = TcpListener::bind(addr).await?;

    tracing::info!("api listening on http://{}/", addr);

    let router = routes::build_routes();

    axum::serve(listener, router.into_make_service()).await?;

    Ok(())
}
