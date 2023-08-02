// Inner modules
mod emitter;
mod errors;
mod lag_estimator;
mod register;
mod tracked_offset;

// Exports
pub use emitter::PartitionOffsetsEmitter;
pub use errors::PartitionOffsetsError;
pub use register::PartitionOffsetsRegister;
pub use tracked_offset::TrackedOffset;

// Imports
use std::sync::Arc;

use rdkafka::ClientConfig;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::cluster_status::ClusterStatusRegister;
use crate::internals::Emitter;

pub fn init(
    admin_client_config: ClientConfig,
    register_offsets_history: usize,
    ready_at: f64,
    cluster_status_register: Arc<ClusterStatusRegister>,
    shutdown_token: CancellationToken,
) -> (PartitionOffsetsRegister, JoinHandle<()>) {
    let (po_rx, poe_join) =
        PartitionOffsetsEmitter::new(admin_client_config, cluster_status_register)
            .spawn(shutdown_token);
    let po_reg = PartitionOffsetsRegister::new(po_rx, register_offsets_history, ready_at);

    debug!("Initialized");
    (po_reg, poe_join)
}
