use tokio::{sync::mpsc, task::JoinHandle, time::Interval};
use tokio_util::sync::CancellationToken;

/// Type that emits an [`Send`]-able object via a [`mpsc::Receiver`].
/// Use this when you expect to have a single receiver.
///
/// It terminates itself when [`CancellationToken`] is cancelled (elsewhere).
///
/// Awaiting for its termination should be done via the returned [`JoinHandle`].
pub trait Emitter {
    type Emitted: Send;

    fn spawn(
        &self,
        shutdown_token: CancellationToken,
    ) -> (mpsc::Receiver<Self::Emitted>, JoinHandle<()>);

    /// Emit the `Self::Emitted`, but first wait for the next `interval` tick.
    ///
    /// # Arguments
    ///
    /// * `sender` - The [`mpsc::Sender`] side of the [`mpsc::Receiver`] returned by `spawn()`
    /// * `emitted` - The [`Self::Emitted`] that implementors of this trait emit
    /// * `interval` - For emitting, await for the next [`Interval::tick`]
    async fn emit_with_interval(
        sender: &mpsc::Sender<Self::Emitted>,
        emitted: Self::Emitted,
        interval: &mut Interval,
    ) -> Result<(), mpsc::error::SendError<Self::Emitted>> {
        // Wait for the next tick.
        // This is here so we can allow preemption inside a `select!` case
        interval.tick().await;

        Self::emit(sender, emitted).await
    }

    /// Emit the `Self::Emitted`.
    ///
    /// # Arguments
    ///
    /// * `sender` - The [`mpsc::Sender`] side of the [`mpsc::Receiver`] returned by `spawn()`
    /// * `emitted` - The [`Self::Emitted`] that implementors of this trait emit
    async fn emit(
        sender: &mpsc::Sender<Self::Emitted>,
        emitted: Self::Emitted,
    ) -> Result<(), mpsc::error::SendError<Self::Emitted>> {
        // Warn in case channel is saturated
        if sender.capacity() == 0 {
            trace!(
                "Channel to emit {} saturated: receiver too slow or service still starting?",
                std::any::type_name::<Self::Emitted>()
            );
        }

        // TODO Each `Emitter` implementation should report a metric about
        //   the current saturation of its emitting channel.
        //   See https://github.com/kafkesc/kommitted/issues/56
        //   See https://github.com/kafkesc/kommitted/issues/57

        // Send the object
        sender.send(emitted).await
    }
}
