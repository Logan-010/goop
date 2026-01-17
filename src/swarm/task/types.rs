use cid::Cid;
use tokio::sync::oneshot;

#[derive(Debug)]
pub struct Request {
    pub message: RequestType,
    pub response_channel: oneshot::Sender<Response>,
}

impl Request {
    pub fn new(message: RequestType) -> (Self, oneshot::Receiver<Response>) {
        let (response_channel, rx) = oneshot::channel();

        (
            Self {
                message,
                response_channel,
            },
            rx,
        )
    }

    pub fn send_response(self, response: Response) {
        if self.response_channel.send(response).is_err() {
            tracing::warn!("failed to send response");
        }
    }
}

#[derive(Debug)]
pub enum RequestType {
    GetCid(Cid),
    GetIpns(Cid),
    SetIpns { data: String, seq: u64 },
}

#[non_exhaustive]
#[derive(Debug)]
pub enum Response {
    Cid(Vec<u8>),
    Ipns { data: String, seq: u64 },
    SetIpns { data: String },
    Error(String),
}
