use libp2p::StreamProtocol;

pub const HASH_SIZE: usize = 64;
pub const CLIENT_NAME: &str = concat!("/goop/", env!("CARGO_PKG_VERSION"));
pub const KAD_PROTOCOL: StreamProtocol = StreamProtocol::new("/ipfs/kad/1.0.0");
