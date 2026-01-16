use std::collections::HashMap;

use libp2p::{PeerId, kad};

#[derive(Debug)]
pub struct State {
    peer_queries: HashMap<kad::QueryId, PeerId>,
}

impl State {
    pub fn new() -> Self {
        Self {
            peer_queries: HashMap::new(),
        }
    }

    pub fn add_peer_query(&mut self, id: kad::QueryId, peer: PeerId) {
        self.peer_queries.insert(id, peer);
    }

    pub fn remove_peer_query(&mut self, id: &kad::QueryId) -> Option<PeerId> {
        self.peer_queries.remove(id)
    }
}
