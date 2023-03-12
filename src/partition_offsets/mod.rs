// Inner modules
mod emitter;
mod errors;
mod known_offset;
mod lag_estimator;
mod register;

// Exports
pub use emitter::PartitionOffsetsEmitter;
pub use errors::PartitionOffsetsError;
pub use register::PartitionOffsetsRegister;

// Imports
use std::sync::Arc;

use rdkafka::ClientConfig;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::cluster_status::ClusterStatusRegister;
use crate::internals::{Emitter, Register};

pub fn init(
    admin_client_config: ClientConfig,
    cluster_status_register: Arc<ClusterStatusRegister>,
    shutdown_rx: broadcast::Receiver<()>,
) -> (PartitionOffsetsRegister, JoinHandle<()>) {
    let (po_rx, poe_join) = PartitionOffsetsEmitter::new(
        admin_client_config,
        cluster_status_register,
    )
    .spawn(shutdown_rx);
    let po_reg = PartitionOffsetsRegister::new(po_rx);

    debug!("Initialized");
    (po_reg, poe_join)
}
