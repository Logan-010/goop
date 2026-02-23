use crate::{
    config::{CONFIG, PeerType},
    consts::{BLOCKS_TABLE, BOOTNODES, BOOTSTRAP_ADDR},
    keystore::Keystore,
    swarm::{Behaviour, State},
};
use blockstore::RedbBlockstore;
use cid::Cid;
use libp2p::{PeerId, Swarm, SwarmBuilder, kad, noise, tcp, yamux};
use redb::{Database, ReadableTable, TableError};
use std::{sync::Arc, time::Duration};

pub async fn init_swarm(
    keystore: &Keystore,
) -> color_eyre::Result<(Arc<RedbBlockstore>, State, Swarm<Behaviour>)> {
    let config = CONFIG.get().unwrap();

    let keypair = keystore.get_or_init_key("self", None)?;

    tracing::info!("loaded identity {}", keypair.public().to_peer_id());

    let redb = RedbBlockstore::open(&config.blockstore_path).await?;

    tracing::info!("loaded blockstore");

    let store = Database::create(&config.kadstore_path)?;

    tracing::info!("loaded kad store");

    let blockstore = Arc::new(redb);

    let mut swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::new().nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_dns()?
        .with_websocket(noise::Config::new, yamux::Config::default)
        .await?
        .with_relay_client(noise::Config::new, yamux::Config::default)?
        .with_behaviour(|k, b| Ok(Behaviour::new(k, b, blockstore.clone(), store)?))?
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

    Ok((blockstore, state, swarm))
}
