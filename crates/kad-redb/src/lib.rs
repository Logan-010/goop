use libp2p::Multiaddr;
use libp2p_identity::PeerId;
use libp2p_kad::{ProviderRecord, Record, RecordKey, store::RecordStore};
use redb::{
    Database, MultimapTableDefinition, ReadableMultimapTable, ReadableTable, TableDefinition,
    TableError,
};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, time::Instant, vec::IntoIter};

#[derive(Serialize, Deserialize)]
struct RecordSerde {
    key: RecordKey,
    value: Vec<u8>,
    publisher: Option<PeerId>,
    expires: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct ProviderRecordSerde {
    key: RecordKey,
    provider: PeerId,
    expires: Option<u64>,
    addresses: Vec<Multiaddr>,
}

const RECORDS: TableDefinition<'static, &[u8], &[u8]> = TableDefinition::new("KAD-REDB.RECORDS");
const PROVIDER_RECORDS: MultimapTableDefinition<'static, &[u8], &[u8]> =
    MultimapTableDefinition::new("KAD-REDB.PROVIDER-RECORDS");

/// Redb-based store for Kad storage.
pub struct RedbStore {
    peer: PeerId,
    db: Database,
}

impl RedbStore {
    /// Create a new store object with your own peer id and an open database connection
    pub fn new(peer: PeerId, databse: Database) -> Self {
        Self { peer, db: databse }
    }
}

impl RecordStore for RedbStore {
    type ProvidedIter<'a>
        = IntoIter<Cow<'a, ProviderRecord>>
    where
        Self: 'a;
    type RecordsIter<'a>
        = IntoIter<Cow<'a, Record>>
    where
        Self: 'a;

    fn put(&mut self, r: Record) -> libp2p_kad::store::Result<()> {
        let write = self
            .db
            .begin_write()
            .expect("failed to begin write transaction");

        {
            let mut records = write
                .open_table(RECORDS)
                .expect("failed to open records table");

            let s = postcard::to_stdvec(&RecordSerde {
                key: r.key.clone(),
                value: r.value,
                publisher: r.publisher,
                expires: r
                    .expires
                    .map(|i| i.saturating_duration_since(Instant::now()).as_millis() as u64),
            })
            .expect("failed to serialize record");

            records
                .insert(r.key.to_vec().as_slice(), s.as_slice())
                .expect("failed to insert record");
        }

        write.commit().expect("failed to commit to db");

        Ok(())
    }

    fn get(&self, k: &RecordKey) -> Option<Cow<'_, Record>> {
        let read = self
            .db
            .begin_read()
            .expect("failed to begin read transaction");

        let records = match read.open_table(RECORDS) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return None,
            Err(e) => panic!("failed to open records table: {:?}", e),
        };

        records
            .get(k.to_vec().as_slice())
            .expect("failed to query records table")
            .map(|a| a.value().to_vec())
            .map(|v| {
                let d: RecordSerde = postcard::from_bytes(&v).expect("failed to decode record");

                Record {
                    key: d.key,
                    value: d.value,
                    publisher: d.publisher,
                    expires: d
                        .expires
                        .map(|e| Instant::now() + std::time::Duration::from_millis(e)),
                }
            })
            .map(Cow::Owned)
    }

    fn remove(&mut self, k: &RecordKey) {
        let write = self
            .db
            .begin_write()
            .expect("failed to begin write transaction");

        {
            let mut records = write
                .open_table(RECORDS)
                .expect("failed to open records table");

            records
                .remove(k.to_vec().as_slice())
                .expect("failed to remove record");
        }

        write.commit().expect("failed to commit to db");
    }

    fn add_provider(&mut self, record: ProviderRecord) -> libp2p_kad::store::Result<()> {
        let write = self
            .db
            .begin_write()
            .expect("failed to begin write transaction");

        {
            let mut records = write
                .open_multimap_table(PROVIDER_RECORDS)
                .expect("failed to open records table");

            let s = postcard::to_stdvec(&ProviderRecordSerde {
                key: record.key.clone(),
                provider: record.provider,
                expires: record
                    .expires
                    .map(|i| i.saturating_duration_since(Instant::now()).as_millis() as u64),
                addresses: record.addresses,
            })
            .expect("failed to serialize record");

            records
                .insert(record.key.to_vec().as_slice(), s.as_slice())
                .expect("failed to add provider to table");
        }

        write.commit().expect("failed to commit to db");

        Ok(())
    }

    fn remove_provider(&mut self, k: &RecordKey, p: &PeerId) {
        let write = self
            .db
            .begin_write()
            .expect("failed to begin write transaction");

        {
            let mut records = write
                .open_multimap_table(PROVIDER_RECORDS)
                .expect("failed to open records table");

            let mut to_remove = Vec::new();
            if let Ok(values) = records.get(k.as_ref()) {
                for entry in values {
                    let entry = entry.expect("failed to get entry");
                    let bytes = entry.value().to_vec();
                    let decoded: ProviderRecordSerde = postcard::from_bytes(&bytes)
                        .expect("failed to deserialize provider record");
                    if decoded.provider == *p {
                        to_remove.push(bytes);
                    }
                }
            }

            for value in to_remove {
                records
                    .remove(k.as_ref(), value.as_slice())
                    .expect("failed to remove provider record");
            }
        }

        write.commit().expect("failed to commit to db");
    }

    fn provided(&self) -> Self::ProvidedIter<'_> {
        let mut records = Vec::new();

        let read = self
            .db
            .begin_read()
            .expect("failed to begin read transaction");

        match read.open_multimap_table(PROVIDER_RECORDS) {
            Ok(table) => {
                for e in table.iter().expect("failed to get table iterator") {
                    let (_, v) = e.expect("failed to get entry");
                    for p in v.into_iter() {
                        let record_bytes = p.expect("failed to get peer id value");

                        let r: ProviderRecordSerde = postcard::from_bytes(record_bytes.value())
                            .expect("failed to deserialize provider record");
                        let pr = ProviderRecord {
                            key: r.key,
                            provider: r.provider,
                            expires: r
                                .expires
                                .map(|e| Instant::now() + std::time::Duration::from_millis(e)),
                            addresses: r.addresses,
                        };

                        if pr.provider == self.peer {
                            records.push(Cow::Owned(pr));
                        }
                    }
                }
            }
            Err(TableError::TableDoesNotExist(_)) => {
                // No actual error
            }
            Err(e) => panic!("failed to open records table: {:?}", e),
        }

        records.into_iter()
    }

    fn providers(&self, key: &RecordKey) -> Vec<ProviderRecord> {
        let mut records = Vec::new();

        let read = self
            .db
            .begin_read()
            .expect("failed to begin read transaction");

        match read.open_multimap_table(PROVIDER_RECORDS) {
            Ok(table) => {
                let values = table
                    .get(key.to_vec().as_slice())
                    .expect("failed to query table");

                for r in values.into_iter() {
                    let record_bytes = r.expect("failed to get record");

                    let r: ProviderRecordSerde = postcard::from_bytes(record_bytes.value())
                        .expect("failed to deserialize provider record");
                    let pr = ProviderRecord {
                        key: r.key,
                        provider: r.provider,
                        expires: r
                            .expires
                            .map(|e| Instant::now() + std::time::Duration::from_millis(e)),
                        addresses: r.addresses,
                    };

                    records.push(pr);
                }
            }
            Err(TableError::TableDoesNotExist(_)) => {
                // No actual error
            }
            Err(e) => panic!("failed to open records table: {:?}", e),
        }

        records
    }

    fn records(&self) -> Self::RecordsIter<'_> {
        let mut records = Vec::new();

        let read = self
            .db
            .begin_read()
            .expect("failed to begin read transaction");

        match read.open_table(RECORDS) {
            Ok(table) => {
                for e in table.iter().expect("failed to get table iterator") {
                    let (_, v) = e.expect("failed to get entry");

                    let d: RecordSerde =
                        postcard::from_bytes(v.value()).expect("failed to decode record");
                    let r = Record {
                        key: d.key,
                        value: d.value,
                        publisher: d.publisher,
                        expires: d
                            .expires
                            .map(|e| Instant::now() + std::time::Duration::from_millis(e)),
                    };

                    records.push(Cow::Owned(r));
                }
            }
            Err(TableError::TableDoesNotExist(_)) => {
                // No actual error
            }
            Err(e) => panic!("failed to open records table: {:?}", e),
        }

        records.into_iter()
    }
}

#[cfg(test)]
mod test {
    use super::RedbStore;
    use libp2p::PeerId;
    use libp2p_kad::{Record, RecordKey, store::RecordStore};
    use redb::Database;
    use tempfile::NamedTempFile;

    fn create_store() -> RedbStore {
        let file = NamedTempFile::new().expect("failed to create temporary file");
        let db = Database::create(file.path()).expect("failed to create database object");
        RedbStore::new(PeerId::random(), db)
    }

    #[test]
    fn test_insert_and_retrieval() {
        let mut store = create_store();

        store
            .put(Record::new(b"hello".to_vec(), b"world".to_vec()))
            .expect("failed to put value");

        let record = store
            .get(&RecordKey::new(b"hello"))
            .expect("failed to get value")
            .value
            .clone();

        assert_eq!(record, b"world");
    }
}
