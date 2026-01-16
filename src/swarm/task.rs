use crate::{
    config::CONFIG,
    consts::CACHE_TABLE,
    swarm::{Behaviour, BehaviourEvent, RequestType, Response, State, init_swarm, types::Request},
};
use blockstore::{Blockstore, RedbBlockstore};
use color_eyre::eyre::ContextCompat;
use libp2p::{Swarm, futures::StreamExt, identify, kad, mdns, swarm::SwarmEvent, upnp};
use redb::{ReadableTable, TableError};
use std::sync::Arc;
use tokio::{select, sync::mpsc, task};
use tokio_util::sync::CancellationToken;

pub async fn spawn(
    cancel: CancellationToken,
    mut requests: mpsc::UnboundedReceiver<Request>,
) -> color_eyre::Result<()> {
    let (blockstore, mut state, mut swarm) = init_swarm().await?;

    let mut cache_size = 0;

    {
        let read_tx = blockstore.raw_db().begin_read()?;

        match read_tx.open_table(CACHE_TABLE) {
            Ok(table) => {
                for entry in table.iter()? {
                    let (_, size) = entry?;

                    cache_size += size.value();
                }
            }
            Err(TableError::TableDoesNotExist(_)) => {
                // No actual error!
            }
            Err(e) => return Err(e.into()),
        }
    }

    state.cache_size = cache_size as usize;

    tracing::info!("initialized swarm");

    loop {
        select! {
            _ = cancel.cancelled() => break,
            req = requests.recv() => match req {
                Some(request) => if let Err(e) = handle_request(request, &blockstore, &mut state, &mut swarm, &cancel).await {
                tracing::warn!("request error: {}", e);
            },
                None => tracing::error!("request channel closed"),
            },
            event = swarm.select_next_some() => if let Err(e) = handle_event(event, &blockstore, &mut state, &mut swarm, &cancel).await {
                tracing::warn!("event error: {}", e);
            }
        }
    }

    Ok(())
}

async fn handle_request(
    request: Request,
    blockstore: &Arc<RedbBlockstore>,
    state: &mut State,
    swarm: &mut Swarm<Behaviour>,
    token: &CancellationToken,
) -> color_eyre::Result<()> {
    match request.message {
        RequestType::GetCid(cid) => {
            if blockstore.has(&cid).await? {
                let content = blockstore
                    .get(&cid)
                    .await?
                    .context("blockstore responded with no content")?;

                request.send_response(Response::Cid(content));
            } else {
                let id = swarm
                    .behaviour_mut()
                    .kad
                    .get_providers(kad::RecordKey::new(&cid.hash().to_bytes().as_slice()));

                state.add_cid_query(id, cid);

                state.add_get_cid(cid, request.response_channel);
            }
        }
    }

    Ok(())
}

async fn handle_event(
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
            Err(e) => tracing::debug!(
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
            kad::QueryResult::GetClosestPeers(Ok(p)) => {
                if let Some(peer) = state.remove_peer_query(&id) {
                    for peer_info in p.peers {
                        if peer_info.peer_id == peer {
                            if !swarm.is_connected(&peer_info.peer_id) {
                                for addr in peer_info.addrs {
                                    if let Err(e) = swarm.dial(addr.clone()) {
                                        tracing::debug!(
                                            "error dialing peer {}'s addr {}, {}",
                                            peer,
                                            addr,
                                            e
                                        );
                                    }
                                }
                            }

                            if let Some(cid) = state.remove_cid_query(&id) {
                                let b_id = swarm.behaviour_mut().bitswap.get(&cid);

                                state.add_block_query(b_id, cid);
                            }
                        }
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

                    let db = blockstore.raw_db();
                    let t = token.child_token();
                    let l = data.len();
                    task::spawn_blocking(move || {
                        if t.is_cancelled() {
                            return Ok(());
                        }

                        let write_tx = db.begin_write()?;

                        if t.is_cancelled() {
                            return Ok(());
                        }

                        {
                            let mut table = write_tx.open_table(CACHE_TABLE)?;

                            table.insert(cid.to_bytes().as_slice(), l as u64)?;
                        }

                        if t.is_cancelled() {
                            return Ok(());
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
