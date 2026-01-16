use crate::swarm::{Behaviour, BehaviourEvent, State, init_swarm};
use blockstore::RedbBlockstore;
use libp2p::{Swarm, futures::StreamExt, identify, kad, mdns, swarm::SwarmEvent, upnp};
use std::sync::Arc;
use tokio::select;
use tokio_util::sync::CancellationToken;

pub async fn spawn(
    cancel: CancellationToken,
) -> color_eyre::Result<()> {
    let (blockstore, mut state, mut swarm) = init_swarm().await?;

    tracing::info!("initialized swarm");

    loop {
        select! {
            _ = cancel.cancelled() => break,
            event = swarm.select_next_some() => if let Err(e) = handle_event(event, &blockstore, &mut state, &mut swarm).await {
                tracing::warn!("event error: {}", e);
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
            res => tracing::debug!("kad result: {:?}", res),
        },
        ev => tracing::trace!("swarm event: {:?}", ev),
    }

    Ok(())
}
