#[macro_use]
extern crate log;

mod cli;
mod cluster_status;
mod constants;
mod consumer_groups;
mod internals;
mod kafka_types;
mod konsumer_offsets_data;
mod lag_register;
mod logging;
mod partition_offsets;

use std::error::Error;
use std::sync::Arc;

use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;

use cli::Cli;
use internals::Emitter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = parse_cli_and_init_logging();
    let admin_client_config = cli.build_client_config();

    let shutdown_token = build_shutdown_token();

    // Init `cluster_status` module
    let (cs_reg, cs_join) = cluster_status::init(admin_client_config.clone(), shutdown_token.clone());

    // Init `partition_offsets` module
    let (po_reg, po_join) = partition_offsets::init(
        admin_client_config.clone(),
        cli.offsets_history,
        Arc::new(cs_reg),
        shutdown_token.clone(),
    );

    // TODO: Turn this into a "waiter" module or a functionality of the registry itself
    let mut interval = interval(Duration::from_secs(2));
    let shutdown_token_clone = shutdown_token.clone();
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let (min, max, avg, count) = po_reg.get_usage().await;
                info!(
                    "Waiting for Partition Offset Register usage to reach 0.1% avg:
                        min={min:3.3}% / max={max:3.3}% / avg={avg:3.3}%
                        count={count}"
                );
                if avg > 0.1_f64 {
                    break;
                }
            },
            _ = shutdown_token_clone.cancelled() => {
                info!("Received shutdown signal");
                return Ok(());
            },
        }
    }

    // TODO / WIP: put in `konsumer_offsets_data` module
    let konsumer_offsets_data_emitter =
        konsumer_offsets_data::KonsumerOffsetsDataEmitter::new(admin_client_config.clone());
    let (kod_rx, kod_join) = konsumer_offsets_data_emitter.spawn(shutdown_token.clone());

    // TODO / WIP: put in `consumer_groups` module
    let consumer_groups_emitter = consumer_groups::ConsumerGroupsEmitter::new(admin_client_config.clone());
    let (cg_rx, cg_join) = consumer_groups_emitter.spawn(shutdown_token.clone());

    // TODO / WIP: put in `lag_register` module
    let _l_reg = lag_register::LagRegister::new(cg_rx, kod_rx, Arc::new(po_reg));

    // Join all the async tasks, then let it terminate
    let _ = tokio::join!(cs_join, po_join, kod_join, cg_join);

    info!("Shutdown!");
    Ok(())
}

fn parse_cli_and_init_logging() -> Cli {
    // Parse command line input and initialize logging
    let cli = Cli::parse_and_validate();
    logging::init(cli.verbosity_level());

    trace!("Created:\n{:#?}", cli);

    cli
}

fn build_shutdown_token() -> CancellationToken {
    let shutdown_token = CancellationToken::new();

    // Setup shutdown signal handler:
    // when it's time to shutdown, cancels the token and all
    // other holders of a clone will be notified to being shutdown sequence.
    //
    // NOTE: This handler will be listening on its own dedicated thread.
    let shutdown_token_clone = shutdown_token.clone();
    if let Err(e) = ctrlc::set_handler(move || {
        info!("Beginning shutdown...");
        shutdown_token_clone.cancel();
    }) {
        error!("Failed to register signal handler: {e}");
    }

    // Return a CancellationToken that can notify other parts of the system.
    shutdown_token
}
