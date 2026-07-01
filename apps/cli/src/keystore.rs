use crate::consts::KEY_TABLE;
use color_eyre::eyre::anyhow;
use libp2p::{
    PeerId,
    identity::{KeyType, Keypair},
};
use redb::{Database, ReadableTable, TableError};
use std::{collections::HashMap, path::Path, sync::Arc};

#[derive(Clone)]
pub struct Keystore {
    db: Arc<Database>,
}

impl Keystore {
    pub fn open<P: AsRef<Path>>(path: P) -> color_eyre::Result<Self> {
        let db = Database::create(path)?;

        Ok(Self { db: Arc::new(db) })
    }

    pub fn get_keys(&self) -> color_eyre::Result<HashMap<String, PeerId>> {
        let read_tx = self.db.begin_read()?;

        match read_tx.open_table(KEY_TABLE) {
            Ok(t) => {
                let mut out = HashMap::new();

                for e in t.iter()? {
                    let (k, v) = e?;

                    let name = String::from_utf8_lossy(k.value()).to_string();
                    let id = Keypair::from_protobuf_encoding(v.value())?
                        .public()
                        .to_peer_id();

                    out.insert(name, id);
                }

                Ok(out)
            }
            Err(TableError::TableDoesNotExist(_)) => {
                // No actual error here
                Ok(HashMap::new())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn get_or_init_key<S: AsRef<str>>(
        &self,
        name: S,
        key_type: Option<KeyType>,
    ) -> color_eyre::Result<Keypair> {
        let read_tx = self.db.begin_read()?;

        let key_bytes = match read_tx.open_table(KEY_TABLE) {
            Ok(t) => t.get(name.as_ref().as_bytes())?.map(|d| d.value().to_vec()),
            Err(TableError::TableDoesNotExist(_)) => {
                // No actual error here
                None
            }
            Err(e) => return Err(e.into()),
        };

        match key_bytes {
            Some(v) => Ok(Keypair::from_protobuf_encoding(&v)?),
            None => {
                let key = match key_type.unwrap_or(KeyType::Ed25519) {
                    KeyType::Ed25519 => Keypair::generate_ed25519(),
                    KeyType::Secp256k1 => Keypair::generate_secp256k1(),
                    KeyType::Ecdsa => Keypair::generate_ecdsa(),
                    KeyType::RSA => return Err(anyhow!("RSA not supported!")),
                };

                let bytes = key.to_protobuf_encoding()?;

                let write_tx = self.db.begin_write()?;

                {
                    let mut table = write_tx.open_table(KEY_TABLE)?;

                    table.insert(name.as_ref().as_bytes(), &bytes.as_slice())?;
                }

                write_tx.commit()?;

                Ok(key)
            }
        }
    }
}
