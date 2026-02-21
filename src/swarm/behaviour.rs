use super::{CLIENT_NAME, HASH_SIZE, KAD_PROTOCOL};
use blockstore::RedbBlockstore;
use libp2p::{
    autonat, dcutr, identify, identity::Keypair, kad, mdns, ping, relay, swarm::NetworkBehaviour,
    upnp,
};
use rand::rngs::OsRng;
use redb::Database;
use std::sync::Arc;

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    ping: ping::Behaviour,
    identify: identify::Behaviour,
    relay: relay::client::Behaviour,
    autonat: autonat::v2::client::Behaviour,
    dcutr: dcutr::Behaviour,
    upnp: upnp::tokio::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
    pub kad: kad::Behaviour<kad_redb::RedbStore>,
    pub bitswap: beetswap::Behaviour<HASH_SIZE, RedbBlockstore>,
}

impl Behaviour {
    pub fn new(
        key: &Keypair,
        relay: relay::client::Behaviour,
        blockstore: Arc<RedbBlockstore>,
        kad_store: Database,
    ) -> color_eyre::Result<Self> {
        Ok(Self {
            ping: ping::Behaviour::new(ping::Config::new()),
            identify: identify::Behaviour::new(identify::Config::new(
                String::from(CLIENT_NAME),
                key.public(),
            )),
            relay,
            autonat: autonat::v2::client::Behaviour::new(
                OsRng,
                autonat::v2::client::Config::default(),
            ),
            dcutr: dcutr::Behaviour::new(key.public().to_peer_id()),
            mdns: mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?,
            upnp: upnp::tokio::Behaviour::default(),
            kad: kad::Behaviour::with_config(
                key.public().to_peer_id(),
                kad_redb::RedbStore::new(key.public().to_peer_id(), kad_store),
                kad::Config::new(KAD_PROTOCOL),
            ),
            bitswap: beetswap::Behaviour::new(blockstore),
        })
    }
}
