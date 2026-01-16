use crate::{
    config::{Config, PeerType},
    consts::{APPLICATION, BLOCKS_TABLE, BOOTNODES, BOOTSTRAP_ADDR, ORGANIZATION, QUALIFIER},
    swarm::{Behaviour, State},
};
use blockstore::RedbBlockstore;
use cid::Cid;
use directories::ProjectDirs;
use libp2p::{
    PeerId, Swarm, SwarmBuilder,
    identity::{Keypair, ed25519},
    kad, noise, tcp, yamux,
};
use redb::{ReadableTable, TableError};
use std::{sync::Arc, time::Duration};
use tokio::{fs, task};

pub async fn init_swarm() -> color_eyre::Result<(Arc<RedbBlockstore>, State, Swarm<Behaviour>)> {
    let dirs = ProjectDirs::from(QUALIFIER, ORGANIZATION, APPLICATION)
        .expect("failed to access goop directories");
    let config_path = dirs.data_dir().join("config.toml");

    let config = if config_path.exists() {
        let content = fs::read_to_string(config_path).await?;

        let config: Config = task::spawn_blocking(move || toml::from_str(&content)).await??;

        config
    } else {
        let config = Config::default();

        if let Some(parent) = config_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let c = config.clone();
        let content = task::spawn_blocking(move || toml::to_string_pretty(&c)).await??;

        fs::write(config_path, content).await?;

        config
    };

    let keypair = if config.identity_path.exists() {
        let mut key_bytes = fs::read(&config.identity_path).await?;

        Keypair::from(ed25519::Keypair::try_from_bytes(&mut key_bytes)?)
    } else {
        let key = ed25519::Keypair::generate();

        fs::write(&config.identity_path, key.to_bytes()).await?;

        Keypair::from(key)
    };

    tracing::info!(
        "loaded identity {} from {}",
        keypair.public().to_peer_id(),
        config.identity_path.display()
    );

    let redb = RedbBlockstore::open(&config.blockstore_path).await?;

    tracing::info!(
        "loaded blockstore from {}",
        config.blockstore_path.display()
    );

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
        .with_behaviour(|k, b| Ok(Behaviour::new(k, b, blockstore.clone())?))?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

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

    for addr in config.listen_addresses {
        swarm.listen_on(addr.clone())?;

        tracing::info!("listening on {}", addr);
    }

    for external_addr in config.external_addresses {
        swarm.add_external_address(external_addr.clone());

        tracing::info!("swarm reachable at external addr {}", external_addr);
    }

    for peer in config.peers {
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
