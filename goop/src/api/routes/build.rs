use super::{cids, frontend, ipns, keys};
use crate::swarm::Request;
use axum::{
    Router,
    routing::{get, post},
};
use tokio::sync::mpsc;
use tower_http::{compression::CompressionLayer, trace::TraceLayer};

pub fn build_routes(requests: mpsc::UnboundedSender<Request>) -> Router {
    Router::new()
        .nest(
            "/api",
            Router::new()
                .nest("/keys", Router::new().route("/get", get(keys::get_keys)))
                .nest("/cid", Router::new().route("/get", post(cids::get_cid)))
                .nest(
                    "/ipns",
                    Router::new()
                        .route("/get", post(ipns::get_ipns))
                        .route("/set", post(ipns::set_ipns)),
                ),
        )
        .fallback(frontend::static_files)
        .layer(TraceLayer::new_for_http())
        .layer(CompressionLayer::new())
        .with_state(requests)
}
