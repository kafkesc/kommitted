use std::any::type_name;

use thiserror::Error;
use tokio::{time::interval, time::Duration};
use tokio_util::sync::CancellationToken;

const READYNESS_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// An [`Self`] is something that we can wait upon for it to be _ready_.
///
/// An [`Self`] is ready once [`Self::is_ready`] returns `true`.
/// The trait is built on _async/await_. The waiting is done by calling [`Self::await_ready`]:
/// a [`CancellationToken`] is provided in case the _awaiting loop_ has to be interrupted.
pub trait Awaitable {
    /// Returns `true` if [`Self`] is ready, `false` otherwise.
    ///
    /// This should be as fast as possible.
    async fn is_ready(&self) -> bool;

    /// Future that completes only once [`Self::is_ready()`] returns `true`.
    ///
    /// If the given [`CancellationToken`] is `cancelled`, it returns an error.
    async fn await_ready(&self, shutdown_token: CancellationToken) -> AwaitableResult<()> {
        let mut interval = interval(READYNESS_CHECK_INTERVAL);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if self.is_ready().await {
                        info!("{} is ready!", type_name::<Self>());
                        return Ok(());
                    }
                },
                _ = shutdown_token.cancelled() => {
                    warn!("{} received shutdown signal before it was ready!", type_name::<Self>());
                    return Err(AwaitableError::Cancelled);
                },
            }
        }
    }
}

#[derive(Error, Debug, Eq, PartialEq)]
pub enum AwaitableError {
    #[error("Cancelled before was ready")]
    Cancelled,
}

pub type AwaitableResult<T> = Result<T, AwaitableError>;
