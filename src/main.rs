#[macro_use]
extern crate log;

mod cli;
mod cluster_status;
mod constants;
mod consumer_groups;
mod http;
mod internals;
mod kafka_types;
mod konsumer_offsets_data;
mod lag_register;
mod logging;
mod partition_offsets;

use std::{error::Error, sync::Arc};

use tokio_util::sync::CancellationToken;

use crate::cli::Cli;
use crate::partition_offsets::PartitionOffsetsRegister;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = parse_cli_and_init_logging();
    let admin_client_config = cli.build_client_config();

    let shutdown_token = build_shutdown_token();

    // Init `cluster_status` module
    let (cs_reg, cs_join) = cluster_status::init(admin_client_config.clone(), cli.cluster_id, shutdown_token.clone());

    // Init `partition_offsets` module
    let (po_reg, po_join) = partition_offsets::init(
        admin_client_config.clone(),
        cli.offsets_history,
        Arc::new(cs_reg),
        shutdown_token.clone(),
    );
    // Await for the Partition Offset register to be ready
    if !po_reg.await_ready(0.1, shutdown_token.clone()).await {
        warn!("Terminated before {} was ready", std::any::type_name::<PartitionOffsetsRegister>());
        std::process::exit(exit_code::SERVICE_UNAVAILABLE);
    }

    // Init `konsumer_offsets_data` module
    let (kod_rx, kod_join) = konsumer_offsets_data::init(admin_client_config.clone(), shutdown_token.clone());

    // Init `consumer_groups` module
    let (cg_rx, cg_join) = consumer_groups::init(admin_client_config.clone(), shutdown_token.clone());

    // Init `lag_register` module
    let lag_reg = lag_register::init(cg_rx, kod_rx, Arc::new(po_reg));
    let lag_reg_arc = Arc::new(lag_reg);

    // Init `http` module
    let http_fut = http::init(lag_reg_arc.clone(), shutdown_token.clone());

    // Join all the async tasks, then let it terminate
    let _ = tokio::join!(cs_join, po_join, kod_join, cg_join, http_fut);

    info!("Shutdown!");
    std::process::exit(exit_code::SUCCESS);
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
