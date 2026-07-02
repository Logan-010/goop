use super::routes;
use crate::{config::CONFIG, swarm::Request};
use std::net::SocketAddr;
use tokio::{net::TcpListener, select, sync::mpsc};
use tokio_util::sync::CancellationToken;

pub async fn spawn(
    token: CancellationToken,
    requests: mpsc::UnboundedSender<Request>,
) -> color_eyre::Result<()> {
    let config = CONFIG.get().unwrap();

    select! {
        _ = token.cancelled() => Ok(()),
        task_res = task(config.api_address, requests) => task_res
    }
}

async fn task(
    addr: SocketAddr,
    requests: mpsc::UnboundedSender<Request>,
) -> color_eyre::Result<()> {
    let listener = TcpListener::bind(addr).await?;

    tracing::info!("api listening on http://{}/", addr);

    let router = routes::build_routes(requests);

    axum::serve(listener, router.into_make_service()).await?;

    Ok(())
}
