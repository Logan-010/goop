mod events;
mod requests;

mod types;
pub use types::*;

use crate::{consts::CACHE_TABLE, swarm::init_swarm};
use libp2p::futures::StreamExt;
use redb::{ReadableTable, TableError};
use tokio::{select, sync::mpsc};
use tokio_util::sync::CancellationToken;

pub async fn spawn(
    cancel: CancellationToken,
    mut requests: mpsc::UnboundedReceiver<Request>,
) -> color_eyre::Result<()> {
    let (keypair, blockstore, mut state, mut swarm) = init_swarm().await?;

    let mut cache_size = 0;

    {
        let read_tx = blockstore.raw_db().begin_read()?;

        match read_tx.open_table(CACHE_TABLE) {
            Ok(table) => {
                for entry in table.iter()? {
                    let (_, size) = entry?;

                    cache_size += size.value();
                }
            }
            Err(TableError::TableDoesNotExist(_)) => {
                // No actual error!
            }
            Err(e) => return Err(e.into()),
        }
    }

    state.cache_size = cache_size as usize;

    tracing::info!("initialized swarm");

    loop {
        select! {
            _ = cancel.cancelled() => break,
            req = requests.recv() => match req {
                Some(request) => if let Err(e) = requests::handle_request(&keypair, request, &blockstore, &mut state, &mut swarm, &cancel).await {
                    tracing::warn!("request error: {}", e);
                },
                None => {
                    tracing::error!("request channel closed");
                    break;
                }
            },
            event = swarm.select_next_some() => if let Err(e) = events::handle_event(event, &blockstore, &mut state, &mut swarm, &cancel).await {
                tracing::warn!("event error: {}", e);
            }
        }
    }

    Ok(())
}
