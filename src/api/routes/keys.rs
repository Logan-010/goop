use axum::{Json, extract::State, http::StatusCode};
use libp2p::PeerId;
use std::collections::HashMap;
use tokio::sync::mpsc;

use crate::swarm::{Request, RequestType, Response};

pub async fn get_keys(
    State(state): State<mpsc::UnboundedSender<Request>>,
) -> Result<(StatusCode, Json<HashMap<String, PeerId>>), (StatusCode, String)> {
    let (req, rx) = Request::new(RequestType::GetKeys);

    if state.send(req).is_err() {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "error sending request".into(),
        ));
    }

    let Ok(res) = rx.await else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "error getting response".into(),
        ));
    };

    match res {
        Response::Keys(k) => Ok((StatusCode::OK, Json(k))),
        _ => unreachable!(),
    }
}
