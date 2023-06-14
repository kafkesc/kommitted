use std::sync::Arc;

use konsumer_offsets::{GroupMetadata, KonsumerOffsetsData, OffsetCommit};
use tokio::sync::{broadcast, mpsc};

use crate::consumer_groups::ConsumerGroups;
use crate::partition_offsets::PartitionOffsetsRegister;

pub struct LagRegister {}

impl LagRegister {
    pub fn new(
        mut cg_rx: mpsc::Receiver<ConsumerGroups>,
        mut kod_rx: broadcast::Receiver<KonsumerOffsetsData>,
        po_reg: Arc<PartitionOffsetsRegister>,
    ) -> Self {
        let lr = LagRegister {};

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(cg) = cg_rx.recv() => {
                        debug!("Received {}", std::any::type_name::<ConsumerGroups>());

                        // TODO implement
                    },
                    Ok(kod) = kod_rx.recv() => {
                        match kod {
                            KonsumerOffsetsData::OffsetCommit(oc) => {
                                debug!("Received {}", std::any::type_name::<OffsetCommit>());

                                // TODO implement
                            },
                            KonsumerOffsetsData::GroupMetadata(gm) => {
                                debug!("Received {}", std::any::type_name::<GroupMetadata>());

                                // TODO implement
                            }
                        }
                    },
                    else => {
                        info!("Emitters stopping: breaking (internal) loop");
                        break;
                    }
                }
            }
        });

        lr
    }
}
