// Inner module
mod emitter;
mod register;

use std::sync::Arc;

// Exports
pub use emitter::ClusterStatusEmitter;
pub use register::ClusterStatusRegister;

// Imports
use prometheus::Registry;
use rdkafka::ClientConfig;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::internals::Emitter;

pub fn init(
    admin_client_config: ClientConfig,
    cluster_id_override: Option<String>,
    shutdown_token: CancellationToken,
    metrics: Arc<Registry>,
) -> (ClusterStatusRegister, JoinHandle<()>) {
    // Cluster Status: emitter and register
    let (cs_rx, cse_join) = ClusterStatusEmitter::new(admin_client_config).spawn(shutdown_token);
    let cs_reg = ClusterStatusRegister::new(cluster_id_override, cs_rx);

    debug!("Initialized");
    (cs_reg, cse_join)
}
