// Inner module
mod emitter;
mod register;

// Exports
pub use emitter::ClusterStatusEmitter;
pub use register::ClusterStatusRegister;

// Imports
use rdkafka::ClientConfig;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::internals::{Emitter, Register};

pub fn init(
    admin_client_config: ClientConfig,
    shutdown_rx: broadcast::Receiver<()>,
) -> (ClusterStatusRegister, JoinHandle<()>) {
    // Cluster Status: emitter and register
    let (cs_rx, cse_join) =
        ClusterStatusEmitter::new(admin_client_config.clone())
            .spawn(shutdown_rx.resubscribe());
    let cs_reg = ClusterStatusRegister::new(cs_rx);

    debug!("Initialized");
    (cs_reg, cse_join)
}
