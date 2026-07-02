use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};
use std::{env, net::SocketAddr, path::PathBuf};
use tokio::{fs, sync::OnceCell, task};

pub static CONFIG: OnceCell<Config> = OnceCell::const_new();

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    pub keystore_path: PathBuf,
    pub blockstore_path: PathBuf,
    pub kadstore_path: PathBuf,
    pub api_address: SocketAddr,
    pub listen_addresses: Vec<Multiaddr>,
    pub external_addresses: Vec<Multiaddr>,
    pub max_cache_size: usize,
    pub peers: Vec<PeerType>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PeerType {
    Direct(Multiaddr),
    Dht(PeerId),
}

impl Default for Config {
    fn default() -> Self {
        let base = env::home_dir()
            .expect("Expected home directory")
            .join(".goop");

        Self {
            keystore_path: base.join("keystore.redb"),
            blockstore_path: base.join("blockstore.redb"),
            kadstore_path: base.join("kad.redb"),
            api_address: "127.0.0.1:5001".parse().unwrap(),
            listen_addresses: vec![
                "/ip4/0.0.0.0/tcp/4001".parse().unwrap(),
                "/ip6/::/tcp/4001".parse().unwrap(),
                "/ip4/0.0.0.0/udp/4001/quic-v1".parse().unwrap(),
                "/ip6/::/udp/4001/quic-v1".parse().unwrap(),
                "/ip4/0.0.0.0/tcp/4002/ws".parse().unwrap(),
                "/ip6/::/tcp/4002/ws".parse().unwrap(),
                "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN/p2p-circuit".parse().unwrap(),
                "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa/p2p-circuit".parse().unwrap(),
                "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb/p2p-circuit".parse().unwrap(),
                "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt/p2p-circuit".parse().unwrap(),
            ],
            external_addresses: Vec::new(),
            // 10 GB
            max_cache_size: 10 * 1024 * 1024 * 1024,
            peers: Vec::new(),
        }
    }
}

impl Config {
    pub async fn new() -> color_eyre::Result<Self> {
        let base = env::home_dir()
            .expect("Expected home directory")
            .join(".goop");

        let config_path = base.join("config.toml");

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

        Ok(config)
    }
}
