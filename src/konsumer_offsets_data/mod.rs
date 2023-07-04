mod emitter;

use konsumer_offsets::KonsumerOffsetsData;
use rdkafka::ClientConfig;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::internals::Emitter;

pub use emitter::KonsumerOffsetsDataEmitter;

pub fn init(
    admin_client_config: ClientConfig,
    shutdown_token: CancellationToken,
) -> (Receiver<KonsumerOffsetsData>, JoinHandle<()>) {
    let konsumer_offsets_data_emitter = KonsumerOffsetsDataEmitter::new(admin_client_config);
    let (kod_rx, kod_join) = konsumer_offsets_data_emitter.spawn(shutdown_token);

    debug!("Initialized");
    (kod_rx, kod_join)
}
