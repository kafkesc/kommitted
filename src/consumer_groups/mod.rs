// Inner module
mod emitter;
mod register;

use std::{sync::Arc, time::Duration};

use prometheus::Registry;
use rdkafka::ClientConfig;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::internals::Emitter;
use emitter::ConsumerGroupsEmitter;

// Exports
pub use register::ConsumerGroupsRegister;

pub fn init(
    admin_client_config: ClientConfig,
    forget_after: Duration,
    shutdown_token: CancellationToken,
    metrics: Arc<Registry>,
) -> (ConsumerGroupsRegister, JoinHandle<()>) {
    let (cg_rx, cg_join) =
        ConsumerGroupsEmitter::new(admin_client_config, metrics.clone()).spawn(shutdown_token);
    let cg_reg = ConsumerGroupsRegister::new(cg_rx, forget_after, metrics);

    debug!("Initialized");
    (cg_reg, cg_join)
}
