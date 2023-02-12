use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};

pub trait BroadcastEmitter {
    type Emitted: Send;

    fn spawn(&self, shutdown_rx: broadcast::Receiver<()>) -> (broadcast::Receiver<Self::Emitted>, JoinHandle<()>);
}

pub trait Emitter {
    type Emitted: Send;

    fn spawn(&self, shutdown_rx: broadcast::Receiver<()>) -> (mpsc::Receiver<Self::Emitted>, JoinHandle<()>);
}
