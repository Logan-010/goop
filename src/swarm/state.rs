use crate::swarm::Response;
use cid::Cid;
use libp2p::{PeerId, kad};
use std::collections::HashMap;
use tokio::sync::oneshot;

pub struct State {
    pub cache_size: usize,
    peer_queries: HashMap<kad::QueryId, PeerId>,
    cid_provider_queries: HashMap<kad::QueryId, Cid>,
    block_queries: HashMap<beetswap::QueryId, Cid>,
    get_cids: HashMap<Cid, oneshot::Sender<Response>>,
}

impl State {
    pub fn new() -> Self {
        Self {
            cache_size: 0,
            peer_queries: HashMap::new(),
            cid_provider_queries: HashMap::new(),
            block_queries: HashMap::new(),
            get_cids: HashMap::new(),
        }
    }

    pub fn add_peer_query(&mut self, id: kad::QueryId, peer: PeerId) {
        self.peer_queries.insert(id, peer);
    }

    pub fn is_searching_for_peer(&self, peer: PeerId) -> bool {
        self.peer_queries.values().any(|p| *p == peer)
    }

    pub fn remove_peer_query(&mut self, id: &kad::QueryId) -> Option<PeerId> {
        self.peer_queries.remove(id)
    }

    pub fn add_cid_query(&mut self, id: kad::QueryId, cid: Cid) {
        self.cid_provider_queries.insert(id, cid);
    }

    pub fn get_cid_for_id(&self, id: &kad::QueryId) -> Option<Cid> {
        self.cid_provider_queries.get(id).cloned()
    }

    pub fn remove_cid_query(&mut self, id: &kad::QueryId) -> Option<Cid> {
        self.cid_provider_queries.remove(id)
    }

    pub fn add_block_query(&mut self, id: beetswap::QueryId, cid: Cid) {
        self.block_queries.insert(id, cid);
    }

    pub fn remove_block_query(&mut self, id: &beetswap::QueryId) -> Option<Cid> {
        self.block_queries.remove(id)
    }

    pub fn add_get_cid(&mut self, cid: Cid, tx: oneshot::Sender<Response>) {
        self.get_cids.insert(cid, tx);
    }

    pub fn remove_get_cid(&mut self, cid: &Cid) -> Option<oneshot::Sender<Response>> {
        self.get_cids.remove(cid)
    }
}
