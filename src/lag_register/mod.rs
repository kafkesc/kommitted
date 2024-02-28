// Inner module
mod register;

use std::sync::Arc;

use konsumer_offsets::KonsumerOffsetsData;
use tokio::sync::mpsc::Receiver;

use crate::consumer_groups::ConsumerGroupsRegister;
use crate::partition_offsets::PartitionOffsetsRegister;

pub use register::{Lag, LagRegister};

pub fn init(
    kod_rx: Receiver<KonsumerOffsetsData>,
    cg_reg: Arc<ConsumerGroupsRegister>,
    po_reg: Arc<PartitionOffsetsRegister>,
) -> LagRegister {
    let l_reg = LagRegister::new(kod_rx, cg_reg, po_reg);

    debug!("Initialized");
    l_reg
}
