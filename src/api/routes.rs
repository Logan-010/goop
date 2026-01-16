use axum::Router;

use crate::api::frontend;

pub fn build_routes() -> Router {
    Router::new().fallback(frontend::static_files)
}
