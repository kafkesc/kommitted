mod register;

use std::sync::Arc;

use konsumer_offsets::KonsumerOffsetsData;
use tokio::sync::mpsc::Receiver;

use crate::consumer_groups::ConsumerGroups;
use crate::partition_offsets::PartitionOffsetsRegister;

pub use register::LagRegister;

pub fn init(
    cg_rx: Receiver<ConsumerGroups>,
    kod_rx: Receiver<KonsumerOffsetsData>,
    po_reg: Arc<PartitionOffsetsRegister>,
) -> LagRegister {
    let l_reg = LagRegister::new(cg_rx, kod_rx, po_reg);

    debug!("Initialized");
    l_reg
}
