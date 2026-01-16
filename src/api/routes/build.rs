use axum::Router;
use tower_http::{compression::CompressionLayer, trace::TraceLayer};
use super::frontend;

pub fn build_routes() -> Router {
    Router::new()
        .fallback(frontend::static_files)
        .layer(TraceLayer::new_for_http())
        .layer(CompressionLayer::new())
}
