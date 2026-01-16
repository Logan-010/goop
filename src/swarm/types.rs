use cid::Cid;
use tokio::sync::oneshot;

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

pub enum RequestType {
    GetCid(Cid),
}

#[non_exhaustive]
pub enum Response {
    Cid(Vec<u8>),
    Error(String),
}
