use axum::{Json, extract::State, http::StatusCode};
use cid::Cid;
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::swarm::{Request, RequestType, Response};

#[derive(Deserialize)]
pub struct GetCid {
    cid: String,
}

pub async fn get_cid(
    State(state): State<mpsc::UnboundedSender<Request>>,
    Json(request): Json<GetCid>,
) -> Result<(StatusCode, Vec<u8>), (StatusCode, String)> {
    let Ok(cid) = request.cid.parse::<Cid>() else {
        return Err((StatusCode::BAD_REQUEST, "invalid cid".into()));
    };

    let (r, rx) = Request::new(RequestType::GetCid(cid));

    if state.send(r).is_err() {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to send request to swarm".into(),
        ));
    }

    let Ok(response) = rx.await else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "error receiving request".into(),
        ));
    };

    match response {
        Response::Cid(data) => Ok((StatusCode::OK, data)),
        Response::Error(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
        _ => unreachable!(),
    }
}
