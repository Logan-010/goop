use crate::swarm::{
    Behaviour, State,
    task::{Request, RequestType, Response},
};
use blockstore::{Blockstore, RedbBlockstore};
use chrono::Duration;
use cid::Cid;
use color_eyre::eyre::{ContextCompat, bail};
use libp2p::{
    Swarm,
    identity::{KeyType, Keypair},
    kad::{self, Quorum},
};
use multihash::Multihash;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub async fn handle_request(
    keypair: &Keypair,
    request: Request,
    blockstore: &Arc<RedbBlockstore>,
    state: &mut State,
    swarm: &mut Swarm<Behaviour>,
    token: &CancellationToken,
) -> color_eyre::Result<()> {
    tracing::debug!("got request: {:?}", request);

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
        RequestType::GetIpns(cid) => {
            if cid.codec() != 0x72 {
                request.send_response(Response::Error("cid is not for an ipns record".into()));
            } else {
                let mut key = b"/ipns/".to_vec();
                key.extend_from_slice(&cid.hash().to_bytes());

                let query_id = swarm
                    .behaviour_mut()
                    .kad
                    .get_record(kad::RecordKey::new(&key));

                state.add_ipns_query(query_id, request.response_channel);
            }
        }
        RequestType::SetIpns { data, seq } => {
            let pk = match keypair.public().key_type() {
                KeyType::Ed25519 => keypair
                    .clone()
                    .try_into_ed25519()?
                    .public()
                    .to_bytes()
                    .to_vec(),
                KeyType::Ecdsa => keypair.clone().try_into_ecdsa()?.public().to_bytes(),
                KeyType::Secp256k1 => keypair
                    .clone()
                    .try_into_secp256k1()?
                    .public()
                    .to_bytes()
                    .to_vec(),
                KeyType::RSA => bail!("RSA key not supported"),
            };

            let cid = Cid::new_v1(0x72, Multihash::<64>::wrap(0x00, &pk)?);

            let mut key = b"/ipns/".to_vec();
            key.extend_from_slice(&cid.hash().to_bytes());

            let record =
                rust_ipns::Record::new(keypair, data, Duration::hours(48), seq, 300_000_000_000)?
                    .encode()?;

            let query_id = swarm.behaviour_mut().kad.put_record(
                kad::Record::new(key, record),
                Quorum::N(20.try_into().unwrap()),
            )?;

            state.add_ipns_query(query_id, request.response_channel);
        }
    }

    Ok(())
}
