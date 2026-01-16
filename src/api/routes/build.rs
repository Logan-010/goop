use super::{cids, frontend};
use crate::swarm::Request;
use axum::{Router, routing::post};
use tokio::sync::mpsc;
use tower_http::{compression::CompressionLayer, trace::TraceLayer};

pub fn build_routes(requests: mpsc::UnboundedSender<Request>) -> Router {
    Router::new()
        .nest("/api", Router::new().route("/get-cid", post(cids::get_cid)))
        .fallback(frontend::static_files)
        .layer(TraceLayer::new_for_http())
        .layer(CompressionLayer::new())
        .with_state(requests)
}
