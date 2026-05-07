use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::observer::{ErrorObserver, ErrorScope};
use crate::{EventBusError, Subscription};

#[must_use = "subscription is idle until bound; call `.close().await` for graceful shutdown"]
pub struct StreamSubscription {
    name: String,
    closed: AtomicBool,
    close_tx: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<Result<(), EventBusError>>>>,
    observer: Option<Arc<dyn ErrorObserver>>,
}

impl StreamSubscription {
    pub(crate) fn new(
        name: String,
        close_tx: watch::Sender<bool>,
        task: JoinHandle<Result<(), EventBusError>>,
        observer: Option<Arc<dyn ErrorObserver>>,
    ) -> Self {
        Self {
            name,
            closed: AtomicBool::new(false),
            close_tx,
            task: Mutex::new(Some(task)),
            observer,
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
            .map_err(|err| EventBusError::source("subscription task failed", err))?
    }

    /// Abort the background task without waiting for graceful drain. Returns
    /// `Ok(())` if the abort was acknowledged or the task was already done;
    /// surfaces the task's last error if it had one.
    pub async fn abort(&self) -> Result<(), EventBusError> {
        let Some(task) = self.begin_shutdown()? else {
            return Ok(());
        };
        task.abort();
        match task.await {
            Ok(r) => r,
            Err(err) if err.is_cancelled() => Ok(()),
            Err(err) => Err(EventBusError::source("subscription task aborted", err)),
        }
    }
}

impl Subscription for StreamSubscription {
    fn name(&self) -> &str {
        StreamSubscription::name(self)
    }

    fn close(self: std::sync::Arc<Self>) -> crate::BoxFuture<'static, Result<(), EventBusError>> {
        Box::pin(async move {
            // Deref the Arc to call the inherent &self method, which already
            // handles the close handshake (begin_shutdown -> JoinHandle::await).
            // The Arc keeps the subscription alive until close completes.
            (*self).close().await
        })
    }
}

/// Dropping a [`StreamSubscription`] is fire-and-forget: it signals the
/// background task to exit but does not await it, and **delivery errors
/// raised after the close signal are silently discarded**. To surface those
/// errors, call [`StreamSubscription::close`] explicitly and await the
/// returned `Result`.
///
/// When the subscription is dropped without `close()` having been called,
/// the configured [`ErrorObserver`] (if any) is notified via
/// [`ErrorScope::Drop`] so leaked subscriptions are observable.
impl Drop for StreamSubscription {
    fn drop(&mut self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }

        let _ = self.close_tx.send(true);
        if let Some(obs) = self.observer.as_ref() {
            obs.on_error(
                ErrorScope::Drop,
                &EventBusError::Internal(format!(
                    "subscription `{}` dropped without close()",
                    self.name
                )),
            );
        }
        if let Ok(mut guard) = self.task.lock() {
            let _ = guard.take();
        }
    }
}
