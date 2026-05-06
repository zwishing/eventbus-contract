use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::{EventBusError, Subscription};

#[must_use = "subscription is idle until bound; call `.close().await` for graceful shutdown"]
pub struct StreamSubscription {
    name: String,
    closed: AtomicBool,
    close_tx: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<Result<(), EventBusError>>>>,
}

impl StreamSubscription {
    pub(crate) fn new(
        name: String,
        close_tx: watch::Sender<bool>,
        task: JoinHandle<Result<(), EventBusError>>,
    ) -> Self {
        Self {
            name,
            closed: AtomicBool::new(false),
            close_tx,
            task: Mutex::new(Some(task)),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns `true` until [`StreamSubscription::close`] has been invoked
    /// (or the subscription was dropped). Useful for control planes that
    /// need to skip already-shutdown subscriptions without racing on close.
    pub fn is_running(&self) -> bool {
        !self.closed.load(Ordering::Acquire)
    }

    fn begin_shutdown(
        &self,
    ) -> Result<Option<JoinHandle<Result<(), EventBusError>>>, EventBusError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(None);
        }

        let _ = self.close_tx.send(true);
        let mut guard = self
            .task
            .lock()
            .map_err(|_| EventBusError::Internal("subscription task mutex poisoned".into()))?;
        Ok(guard.take())
    }

    pub async fn close(&self) -> Result<(), EventBusError> {
        let Some(task) = self.begin_shutdown()? else {
            return Ok(());
        };

        task.await
            .map_err(|err| EventBusError::Internal(format!("subscription task failed: {err}")))?
    }
}

impl Subscription for StreamSubscription {
    fn name(&self) -> &str {
        self.name()
    }

    async fn close(&self) -> Result<(), EventBusError> {
        StreamSubscription::close(self).await
    }
}

/// Dropping a [`StreamSubscription`] is fire-and-forget: it signals the
/// background task to exit but does not await it, and **delivery errors
/// raised after the close signal are silently discarded**. To surface those
/// errors, call [`StreamSubscription::close`] explicitly and await the
/// returned `Result`.
impl Drop for StreamSubscription {
    fn drop(&mut self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }

        let _ = self.close_tx.send(true);
        if let Ok(mut guard) = self.task.lock() {
            let _ = guard.take();
        }
    }
}
