use redb::TableDefinition;

pub const QUALIFIER: &str = "com";
pub const ORGANIZATION: &str = "seedse";
pub const APPLICATION: &str = "goop";

pub const BOOTNODES: [&str; 4] = [
    "QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
    "QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
    "QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
    "QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
];
pub const BOOTSTRAP_ADDR: &str = "/dnsaddr/bootstrap.libp2p.io";

pub const BLOCKS_TABLE: TableDefinition<'static, &[u8], &[u8]> =
    TableDefinition::new("BLOCKSTORE.BLOCKS");
