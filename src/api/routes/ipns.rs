use crate::swarm::{Request, RequestType, Response};
use axum::{Json, extract::State, http::StatusCode};
use cid::Cid;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Deserialize)]
pub struct GetIpns {
    cid: String,
}

#[derive(Serialize)]
pub struct GetIpnsResponse {
    data: String,
    sequence: u64,
}

pub async fn get_ipns(
    State(state): State<mpsc::UnboundedSender<Request>>,
    Json(req): Json<GetIpns>,
) -> Result<(StatusCode, Json<GetIpnsResponse>), (StatusCode, String)> {
    let Ok(cid) = Cid::try_from(req.cid) else {
        return Err((StatusCode::BAD_REQUEST, "invalid cid".into()));
    };

    let (req, rx) = Request::new(RequestType::GetIpns(cid));

    if state.send(req).is_err() {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to send request".into(),
        ));
    }

    let Ok(res) = rx.await else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to get response".into(),
        ));
    };

    match res {
        Response::Ipns { data, seq } => Ok((
            StatusCode::OK,
            Json(GetIpnsResponse {
                data: data.to_string(),
                sequence: seq,
            }),
        )),
        Response::Error(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
        _ => unreachable!(),
    }
}

#[derive(Deserialize)]
pub struct SetIpns {
    cid: String,
}

#[derive(Serialize)]
pub struct SetIpnsResponse {
    cid: String,
}

pub async fn set_ipns(
    State(state): State<mpsc::UnboundedSender<Request>>,
    Json(req): Json<SetIpns>,
) -> Result<(StatusCode, Json<SetIpnsResponse>), (StatusCode, String)> {
    let Ok(cid) = Cid::try_from(req.cid) else {
        return Err((StatusCode::BAD_REQUEST, "invalid cid".into()));
    };

    let (req, rx) = Request::new(RequestType::GetIpns(cid));

    if state.send(req).is_err() {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to send request".into(),
        ));
    }

    let Ok(res) = rx.await else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to get response".into(),
        ));
    };

    let seq = match res {
        Response::Ipns { seq, .. } => seq,
        Response::Error(e) => return Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
        _ => unreachable!(),
    };

    let (req, rx) = Request::new(RequestType::SetIpns {
        data: format!("/ipfs/{}", cid),
        seq: seq + 1,
    });

    if state.send(req).is_err() {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to send request".into(),
        ));
    }

    let Ok(res) = rx.await else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to get response".into(),
        ));
    };

    match res {
        Response::SetIpns { data } => Ok((StatusCode::OK, Json(SetIpnsResponse { cid: data }))),
        Response::Error(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
        _ => unreachable!(),
    }
}
