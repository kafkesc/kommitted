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

use crate::cluster_status::ClusterStatusRegister;
use crate::partition_offsets::PartitionOffsetsEmitter;
use cli::Cli;
use cluster_status::ClusterStatusEmitter;
use internals::{Emitter, Register};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = parse_cli_and_init_logging();
    let admin_client_config = cli.build_client_config();

    let shutdown_rx = build_shutdown_channel();

    // Cluster Status: emitter and register
    let (cs_rx, cse_join) =
        ClusterStatusEmitter::new(admin_client_config.clone())
            .spawn(shutdown_rx.resubscribe());
    let cs_reg = ClusterStatusRegister::new(cs_rx);

    // Partition Offsets: emitter
    let (mut po_rx, poe_join) = PartitionOffsetsEmitter::new(
        admin_client_config.clone(),
        Arc::new(cs_reg),
    )
    .spawn(shutdown_rx.resubscribe());

    // WIP: echo
    let echo_join = tokio::spawn(async move {
        while let Some(po) = po_rx.recv().await {
            info!("{po:?}");
        }
    });

    // Join all the async tasks, then let it terminate
    let _ = tokio::join!(cse_join, poe_join, echo_join);
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
