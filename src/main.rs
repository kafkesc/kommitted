#[macro_use]
extern crate log;

mod cli;
mod cluster_status;
mod internals;
mod kafka_types;
mod logging;
mod partition_offsets;

use std::error::Error;
use std::sync::Arc;

use tokio::sync::broadcast;

use cli::Cli;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = parse_cli_and_init_logging();
    let admin_client_config = cli.build_client_config();

    let shutdown_rx = build_shutdown_channel();

    // Init `cluster_status` module
    let (cs_reg, cs_join) = cluster_status::init(
        admin_client_config.clone(),
        shutdown_rx.resubscribe(),
    );

    // Init `partition_offsets` module
    let (po_reg, po_join) = partition_offsets::init(
        admin_client_config.clone(),
        Arc::new(cs_reg),
        shutdown_rx.resubscribe(),
    );

    // Join all the async tasks, then let it terminate
    let _ = tokio::join!(cs_join, po_join);
    Ok(())
}

fn parse_cli_and_init_logging() -> Cli {
    // Parse command line input and initialize logging
    let cli = Cli::parse_and_validate();
    logging::init(cli.verbosity_level());

    trace!("Created:\n{:#?}", cli);

    cli
}

fn build_shutdown_channel() -> broadcast::Receiver<()> {
    let (sender, receiver) = broadcast::channel(1);

    // Setup shutdown signal handler:
    // when it's time to shutdown, broadcast to all receiver a unit.
    //
    // NOTE: This handler will be listening on its own dedicated thread.
    if let Err(e) = ctrlc::set_handler(move || {
        info!("Shutting down...");
        sender.send(()).unwrap();
    }) {
        error!("Failed to register signal handler: {e}");
    }

    // Return a receiver to we can notify other parts of the system.
    receiver
}
