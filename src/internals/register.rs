use tokio::sync::{broadcast, mpsc};

/// Stores the [`Send`]-able objects it receives via the given [`mpsc::Receiver`].
///
/// This is likely used as _receiver_ for [`crate::internals::Emitter`].
///
/// Objects of this type will likely have to implement additional methods,
/// to allow access to the data that it stores.
pub trait Register {
    type Registered: Send;

    fn new(rx: mpsc::Receiver<Self::Registered>) -> Self;
}

/// Stores the [`Send`]-able objects it receives via the given [`broadcast::Receiver`].
///
/// This is likely used as _receiver_ for [`crate::internals::BroadcastEmitter`].
///
/// Objects of this type will likely have to implement additional methods,
/// to allow access to the data that it stores.
pub trait BroadcastRegister {
    type Registered: Send;

    fn new(rx: broadcast::Receiver<Self::Registered>) -> Self;
}
