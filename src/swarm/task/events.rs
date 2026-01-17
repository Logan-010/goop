use crate::{
    config::CONFIG,
    consts::CACHE_TABLE,
    swarm::{Behaviour, BehaviourEvent, State, task::Response},
};
use blockstore::{Blockstore, RedbBlockstore};
use cid::Cid;
use color_eyre::eyre::ContextCompat;
use libp2p::{Swarm, identify, identity::PublicKey, kad, mdns, swarm::SwarmEvent, upnp};
use multihash::Multihash;
use std::sync::Arc;
use tokio::task;
use tokio_util::sync::CancellationToken;

pub async fn handle_event(
    event: SwarmEvent<BehaviourEvent>,
    blockstore: &Arc<RedbBlockstore>,
    state: &mut State,
    swarm: &mut Swarm<Behaviour>,
    token: &CancellationToken,
) -> color_eyre::Result<()> {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => tracing::debug!("listening on {}", address),
        SwarmEvent::Behaviour(BehaviourEvent::Autonat(ev)) => match ev.result {
            Ok(_) => {
                tracing::info!("confirmed external addr {}", ev.tested_addr);

                swarm.add_external_address(ev.tested_addr);
            }
            Err(e) => tracing::trace!(
                "autonat tested addr {} and failed with {}",
                ev.tested_addr,
                e
            ),
        },
        SwarmEvent::Behaviour(BehaviourEvent::Dcutr(ev)) => {
            if let Err(e) = ev.result {
                tracing::debug!(
                    "failed to hole punch remote connection to {}, error {}",
                    ev.remote_peer_id,
                    e
                );
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
            for (peer, addr) in list {
                swarm.behaviour_mut().kad.add_address(&peer, addr);
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
            for (peer, addr) in list {
                swarm.behaviour_mut().kad.remove_address(&peer, &addr);
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received {
            peer_id,
            info: identify::Info { listen_addrs, .. },
            ..
        })) => {
            tracing::trace!("connected to {}", peer_id);

            for addr in listen_addrs {
                swarm.behaviour_mut().kad.add_address(&peer_id, addr);
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Upnp(upnp::Event::NewExternalAddr(a))) => {
            tracing::info!("new upnp external address {}", a);

            swarm.add_external_address(a);
        }
        SwarmEvent::Behaviour(BehaviourEvent::Upnp(ev)) => {
            tracing::warn!("upnp error: {:?}", ev);
        }
        SwarmEvent::Behaviour(BehaviourEvent::Kad(kad::Event::OutboundQueryProgressed {
            id,
            result,
            step,
            ..
        })) => match result {
            kad::QueryResult::StartProviding(Ok(v)) => {
                tracing::debug!("started providing {:?}", v.key)
            }
            kad::QueryResult::StartProviding(Err(e)) => {
                tracing::warn!("error providing {:?}", e);
            }
            kad::QueryResult::PutRecord(Ok(p)) => {
                if let Some(response_channel) = state.remove_ipns_query(&id) {
                    let key = p.key.to_vec();

                    let mh_bytes = key.strip_prefix(b"/ipns/").context("invalid record key")?;

                    let Ok(mh) = Multihash::<64>::from_bytes(mh_bytes) else {
                        if response_channel
                            .send(Response::Error("invalid mh bytes".into()))
                            .is_err()
                        {
                            tracing::warn!("failed to send response");
                        }

                        return Ok(());
                    };

                    let cid = Cid::new_v1(0x72, mh);

                    if response_channel
                        .send(Response::SetIpns {
                            data: format!("/ipns/{}", cid),
                        })
                        .is_err()
                    {
                        tracing::warn!("failed to send response");
                    }
                }
            }
            kad::QueryResult::PutRecord(Err(e)) => {
                if let Some(response_channel) = state.remove_ipns_query(&id)
                    && response_channel
                        .send(Response::Error(e.to_string()))
                        .is_err()
                {
                    tracing::warn!("failed to send response");
                }
            }
            kad::QueryResult::GetRecord(Ok(kad::GetRecordOk::FoundRecord(kad::PeerRecord {
                record,
                ..
            }))) => {
                if let Some(response_channel) = state.remove_ipns_query(&id) {
                    let ipns_record = rust_ipns::Record::decode(record.value)?;

                    let key = record.key.to_vec();

                    let mh_bytes = key.strip_prefix(b"/ipns/").context("invalid record key")?;

                    let Ok(mh) = Multihash::<64>::from_bytes(mh_bytes) else {
                        if response_channel
                            .send(Response::Error("invalid mh bytes".into()))
                            .is_err()
                        {
                            tracing::warn!("failed to send response");
                        }

                        return Ok(());
                    };

                    if mh.code() != 0x00 {
                        if response_channel
                            .send(Response::Error("unsupported record key".into()))
                            .is_err()
                        {
                            tracing::warn!("failed to send response");
                        }
                    } else {
                        let pk = PublicKey::try_decode_protobuf(mh.digest())?;

                        ipns_record.verify(pk.to_peer_id())?;

                        let ipns_data = ipns_record.data()?;

                        let Ok(data) = String::from_utf8(ipns_data.value().to_vec()) else {
                            if response_channel
                                .send(Response::Error("invalid record data".into()))
                                .is_err()
                            {
                                tracing::warn!("failed to send response");
                            }

                            return Ok(());
                        };

                        if response_channel
                            .send(Response::Ipns {
                                data,
                                seq: ipns_data.sequence(),
                            })
                            .is_err()
                        {
                            tracing::warn!("failed to send response");
                        }
                    }
                }
            }
            kad::QueryResult::GetRecord(Ok(kad::GetRecordOk::FinishedWithNoAdditionalRecord {
                ..
            })) => {
                if let Some(response_channel) = state.remove_ipns_query(&id)
                    && response_channel
                        .send(Response::Error("no record found".into()))
                        .is_err()
                {
                    tracing::warn!("failed to send response");
                }
            }
            kad::QueryResult::GetRecord(Err(e)) => {
                if let Some(response_channel) = state.remove_ipns_query(&id)
                    && response_channel
                        .send(Response::Error(e.to_string()))
                        .is_err()
                {
                    tracing::warn!("failed to send response");
                }
            }
            kad::QueryResult::GetClosestPeers(Ok(p)) => {
                if let Some(peer) = state.remove_peer_query(&id) {
                    if p.peers.iter().any(|p| p.peer_id == peer) {
                        tracing::debug!("found peer {} in dht", peer);

                        if !swarm.is_connected(&peer) {
                            swarm.dial(peer)?;
                        }

                        if let Some(cid) = state.remove_cid_query(&id) {
                            tracing::debug!("getting cid {}", cid);

                            let b_id = swarm.behaviour_mut().bitswap.get(&cid);

                            state.add_block_query(b_id, cid);
                        }
                    } else {
                        tracing::debug!("peer {} not found in dht", peer);
                    }
                }
            }
            kad::QueryResult::GetClosestPeers(Err(e)) => {
                if let Some(peer) = state.remove_peer_query(&id) {
                    tracing::warn!("failed to get peer {}, {}", peer, e);
                }
            }
            kad::QueryResult::Bootstrap(Ok(bootstrap)) => {
                tracing::debug!(
                    "bootstrap finished with {}, {} remaining",
                    bootstrap.peer,
                    bootstrap.num_remaining
                );
            }
            kad::QueryResult::Bootstrap(Err(e)) => tracing::warn!("bootstrap error: {:?}", e),
            kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders {
                providers,
                ..
            })) => {
                tracing::debug!("found providers {:?}", providers);

                for peer in providers {
                    if swarm.is_connected(&peer)
                        && let Some(cid) = if step.last {
                            state.remove_cid_query(&id)
                        } else {
                            state.get_cid_for_id(&id)
                        }
                    {
                        let b_id = swarm.behaviour_mut().bitswap.get(&cid);

                        state.add_block_query(b_id, cid);
                    } else if !state.is_searching_for_peer(peer)
                        && let Some(cid) = state.get_cid_for_id(&id)
                    {
                        let q_id = swarm.behaviour_mut().kad.get_closest_peers(peer);

                        state.add_peer_query(q_id, peer);
                        state.add_cid_query(q_id, cid);
                    }
                }
            }
            kad::QueryResult::GetProviders(Ok(
                kad::GetProvidersOk::FinishedWithNoAdditionalRecord { .. },
            )) => {
                if let Some(cid) = state.remove_cid_query(&id) {
                    tracing::debug!("no providers found for cid {}", cid);
                }
            }
            kad::QueryResult::GetProviders(Err(e)) => {
                if let Some(cid) = state.remove_cid_query(&id) {
                    tracing::debug!("error getting providers for cid {}, {}", cid, e);
                }
            }
            res => tracing::debug!("kad result: {:?}", res),
        },
        SwarmEvent::Behaviour(BehaviourEvent::Bitswap(beetswap::Event::GetQueryResponse {
            query_id,
            data,
        })) => {
            if let Some(cid) = state.remove_block_query(&query_id) {
                tracing::debug!("got data for cid {}", cid);

                if state.cache_size + data.len() < CONFIG.get().unwrap().max_cache_size
                    && !blockstore.has(&cid).await?
                {
                    blockstore.put_keyed(&cid, &data).await?;

                    state.cache_size += data.len();

                    swarm
                        .behaviour_mut()
                        .kad
                        .start_providing(kad::RecordKey::new(&cid.hash().to_bytes().as_slice()))?;

                    let db = blockstore.raw_db();
                    let t = token.child_token();
                    let l = data.len();
                    task::spawn_blocking(move || {
                        if t.is_cancelled() {
                            return Ok(());
                        }

                        let write_tx = db.begin_write()?;

                        {
                            let mut table = write_tx.open_table(CACHE_TABLE)?;

                            table.insert(cid.to_bytes().as_slice(), l as u64)?;
                        }

                        write_tx.commit()?;

                        Result::<(), redb::Error>::Ok(())
                    });
                }

                if let Some(response_channel) = state.remove_get_cid(&cid)
                    && response_channel.send(Response::Cid(data)).is_err()
                {
                    tracing::warn!("failed to send response");
                }
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Bitswap(beetswap::Event::GetQueryError {
            query_id,
            error,
        })) => {
            if let Some(cid) = state.remove_block_query(&query_id) {
                tracing::debug!("failed to get cid {}, {}", cid, error);

                if let Some(response_channel) = state.remove_get_cid(&cid)
                    && response_channel
                        .send(Response::Error(error.to_string()))
                        .is_err()
                {
                    tracing::warn!("failed to send response");
                }
            }
        }
        ev => tracing::trace!("swarm event: {:?}", ev),
    }

    Ok(())
}
