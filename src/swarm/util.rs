use crate::{
    config::{Config, PeerType},
    consts::{APPLICATION, BOOTNODES, BOOTSTRAP_ADDR, ORGANIZATION, QUALIFIER},
    swarm::{Behaviour, State},
};
use blockstore::RedbBlockstore;
use color_eyre::eyre::anyhow;
use directories::ProjectDirs;
use libp2p::{
    PeerId, Swarm, SwarmBuilder,
    identity::{Keypair, ed25519},
    noise, tcp, yamux,
};
use std::{sync::Arc, time::Duration};
use tokio::{
    fs::{self, File},
    task,
};

pub async fn init_swarm() -> color_eyre::Result<(Arc<RedbBlockstore>, State, Swarm<Behaviour>)> {
    let dirs = ProjectDirs::from(QUALIFIER, ORGANIZATION, APPLICATION)
        .expect("failed to access goop directories");
    let config_path = dirs.data_dir().join("config.json");

    let config = if config_path.exists() {
        let file = File::open(config_path)
            .await?
            .try_into_std()
            .map_err(|_| anyhow!("failed to transmute file type into std"))?;

        let config: Config = task::spawn_blocking(move || serde_json::from_reader(file)).await??;

        config
    } else {
        let config = Config::default();

        if let Some(parent) = config_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let file = File::create_new(config_path)
            .await?
            .try_into_std()
            .map_err(|_| anyhow!("failed to transmute file type into std"))?;

        let c = config.clone();
        task::spawn_blocking(move || serde_json::to_writer(file, &c)).await??;

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
