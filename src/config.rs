use crate::consts::{APPLICATION, ORGANIZATION, QUALIFIER};
use directories::ProjectDirs;
use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    pub identity_path: PathBuf,
    pub blockstore_path: PathBuf,
    pub listen_addresses: Vec<Multiaddr>,
    pub external_addresses: Vec<Multiaddr>,
    pub peers: Vec<PeerType>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PeerType {
    Direct(Multiaddr),
    Dht(PeerId),
}

impl Default for Config {
    fn default() -> Self {
        let dirs = ProjectDirs::from(QUALIFIER, ORGANIZATION, APPLICATION)
            .expect("failed to access goop directories");
        let base = dirs.data_dir().to_path_buf();

        Self {
            identity_path: base.join("keypair.bin"),
            blockstore_path: base.join("blockstore.redb"),
            listen_addresses: vec![
                "/ip4/0.0.0.0/tcp/4001".parse().unwrap(),
                "/ip6/::/tcp/4001".parse().unwrap(),
                "/ip4/0.0.0.0/udp/4001/quic-v1".parse().unwrap(),
                "/ip6/::/udp/4001/quic-v1".parse().unwrap(),
                "/ip4/0.0.0.0/tcp/4002/ws".parse().unwrap(),
                "/ip6/::/tcp/4002/ws".parse().unwrap(),
                "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN/p2p-circuit".parse().unwrap(),
                "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa/p2p-circuit".parse().unwrap(),
                "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb/p2p-circuit".parse().unwrap(),
                "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt/p2p-circuit".parse().unwrap(),
            ],
            external_addresses: Vec::new(),
            peers: Vec::new(),
        }
    }
}
