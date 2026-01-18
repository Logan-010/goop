use crate::{
    config::{CONFIG, PeerType},
    consts::{BLOCKS_TABLE, BOOTNODES, BOOTSTRAP_ADDR},
    swarm::{Behaviour, State},
};
use blockstore::RedbBlockstore;
use cid::Cid;
use libp2p::{
    PeerId, Swarm, SwarmBuilder, Transport,
    core::muxing::StreamMuxerBox,
    identity::{Keypair, ed25519},
    kad, noise, tcp, yamux,
};
use libp2p_webrtc::{self as webrtc, tokio::Certificate};
use redb::{ReadableTable, TableError};
use std::{sync::Arc, time::Duration};
use tokio::fs;

pub async fn init_swarm()
-> color_eyre::Result<(Keypair, Arc<RedbBlockstore>, State, Swarm<Behaviour>)> {
    let config = CONFIG.get().unwrap();

    let keypair = {
        let mut content = fs::read(&config.identity_path).await?;

        Keypair::from(ed25519::Keypair::try_from_bytes(&mut content)?)
    };

    let certificate = {
        let content = fs::read_to_string(&config.webrtc_cert_path).await?;

        Certificate::from_pem(&content)?
    };

    tracing::info!("loaded identity {}", keypair.public().to_peer_id());

    let redb = RedbBlockstore::open(&config.blockstore_path).await?;

    tracing::info!("loaded blockstore");

    let blockstore = Arc::new(redb);

    let mut swarm = SwarmBuilder::with_existing_identity(keypair.clone())
        .with_tokio()
        .with_tcp(
            tcp::Config::new().nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_other_transport(|key| {
            webrtc::tokio::Transport::new(key.clone(), certificate)
                .map(|(peer_id, conn), _| (peer_id, StreamMuxerBox::new(conn)))
        })?
        .with_dns()?
        .with_websocket(noise::Config::new, yamux::Config::default)
        .await?
        .with_relay_client(noise::Config::new, yamux::Config::default)?
        .with_behaviour(|k, b| Ok(Behaviour::new(k, b, blockstore.clone())?))?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    swarm.behaviour_mut().kad.set_mode(Some(kad::Mode::Server));

    for id_str in BOOTNODES {
        let id: PeerId = id_str.parse().unwrap();

        swarm
            .behaviour_mut()
            .kad
            .add_address(&id, BOOTSTRAP_ADDR.parse().unwrap());
    }

    {
        let read_tx = blockstore.raw_db().begin_read()?;

        match read_tx.open_table(BLOCKS_TABLE) {
            Ok(table) => {
                for entry in table.iter()? {
                    let cid_bytes = entry?.0;

                    let cid = Cid::read_bytes(cid_bytes.value())?;

                    swarm
                        .behaviour_mut()
                        .kad
                        .start_providing(kad::RecordKey::new(&cid.hash().to_bytes()))?;
                }
            }
            Err(TableError::TableDoesNotExist(_)) => {
                // No error, its just an empty blockstore!
            }
            Err(e) => return Err(e.into()),
        }
    }

    let mut state = State::new();

    for addr in config.listen_addresses.clone() {
        swarm.listen_on(addr.clone())?;

        tracing::info!("listening on {}", addr);
    }

    for external_addr in config.external_addresses.clone() {
        swarm.add_external_address(external_addr.clone());

        tracing::info!("swarm reachable at external addr {}", external_addr);
    }

    for peer in config.peers.clone() {
        match peer {
            PeerType::Direct(ma) => {
                swarm.dial(ma)?;
            }
            PeerType::Dht(peer) => {
                let id = swarm.behaviour_mut().kad.get_closest_peers(peer);

                state.add_peer_query(id, peer);
            }
        }
    }

    Ok((keypair, blockstore, state, swarm))
}
