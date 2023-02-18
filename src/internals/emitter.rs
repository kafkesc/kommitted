use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};

/// Type that emits an [`Send`]-able object via a [`broadcast::Receiver`].
/// Use this when you expect to have multiple receivers.
///
/// It terminates itself when it receives a unit `()` via the given `shutdown_rx` [`broadcast::Receiver`].
///
/// Awaiting for its termination should be done via the returned [`JoinHandle`].
pub trait BroadcastEmitter {
    type Emitted: Send;

    fn spawn(
        &self,
        shutdown_rx: broadcast::Receiver<()>,
    ) -> (broadcast::Receiver<Self::Emitted>, JoinHandle<()>);
}

/// Type that emits an [`Send`]-able object via a [`mpsc::Receiver`].
/// Use this when you expect to have a single receiver.
///
/// It terminates itself when it receives a unit `()` via the given `shutdown_rx` [`broadcast::Receiver`].
///
/// Awaiting for its termination should be done via the returned [`JoinHandle`].
pub trait Emitter {
    type Emitted: Send;

    fn spawn(
        &self,
        shutdown_rx: broadcast::Receiver<()>,
    ) -> (mpsc::Receiver<Self::Emitted>, JoinHandle<()>);
}
