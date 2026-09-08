use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

use super::backend::StreamBackend;
use super::observer::{ErrorObserver, ErrorScope};
use crate::{EventBusError, Subscription};

/// Capture before spawning so abort-before-first-poll also cleans up. The
/// completion notification lets close/abort await asynchronous backend cleanup.
pub(super) struct ConsumerCleanup<B: StreamBackend> {
    backend: Arc<B>,
    stream: String,
    group: String,
    consumer: String,
    completed: Option<oneshot::Sender<()>>,
}

impl<B: StreamBackend> ConsumerCleanup<B> {
    pub(super) fn new(
        backend: Arc<B>,
        stream: String,
        group: String,
        consumer: String,
    ) -> (Self, oneshot::Receiver<()>) {
        let (completed, cleaned) = oneshot::channel();
        (
            Self {
                backend,
                stream,
                group,
                consumer,
                completed: Some(completed),
            },
            cleaned,
        )
    }
}

impl<B: StreamBackend> Drop for ConsumerCleanup<B> {
    fn drop(&mut self) {
        let Some(completed) = self.completed.take() else {
            return;
        };
        let backend = Arc::clone(&self.backend);
        let stream = std::mem::take(&mut self.stream);
        let group = std::mem::take(&mut self.group);
        let consumer = std::mem::take(&mut self.consumer);
        // Async cleanup requires a live runtime. Explicit close/abort should
        // finish before shutting down that runtime.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                backend.forget_consumer(&stream, &group, &consumer).await;
                let _ = completed.send(());
            });
        }
    }
}

struct SubscriptionTask {
    handle: JoinHandle<Result<(), EventBusError>>,
    cleaned: oneshot::Receiver<()>,
}

#[must_use = "subscription is idle until bound; call `.close().await` for graceful shutdown"]
pub struct StreamSubscription {
    name: String,
    closed: AtomicBool,
    close_tx: watch::Sender<bool>,
    task: Mutex<Option<SubscriptionTask>>,
    observer: Option<Arc<dyn ErrorObserver>>,
}

impl StreamSubscription {
    pub(crate) fn new(
        name: String,
        close_tx: watch::Sender<bool>,
        task: JoinHandle<Result<(), EventBusError>>,
        cleaned: oneshot::Receiver<()>,
        observer: Option<Arc<dyn ErrorObserver>>,
    ) -> Self {
        Self {
            name,
            closed: AtomicBool::new(false),
            close_tx,
            task: Mutex::new(Some(SubscriptionTask {
                handle: task,
                cleaned,
            })),
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

    fn begin_shutdown(&self) -> Result<Option<SubscriptionTask>, EventBusError> {
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

        let result = task
            .handle
            .await
            .map_err(|err| EventBusError::source("subscription task failed", err))
            .and_then(|result| result);
        let cleanup = task
            .cleaned
            .await
            .map_err(|_| EventBusError::Internal("consumer cleanup did not complete".into()));
        result.and(cleanup)
    }

    /// Abort the background task without waiting for graceful drain. Returns
    /// `Ok(())` if the abort was acknowledged or the task was already done;
    /// surfaces the task's last error if it had one.
    pub async fn abort(&self) -> Result<(), EventBusError> {
        let Some(task) = self.begin_shutdown()? else {
            return Ok(());
        };
        task.handle.abort();
        let result = match task.handle.await {
            Ok(r) => r,
            Err(err) if err.is_cancelled() => Ok(()),
            Err(err) => Err(EventBusError::source("subscription task aborted", err)),
        };
        let cleanup = task
            .cleaned
            .await
            .map_err(|_| EventBusError::Internal("consumer cleanup did not complete".into()));
        result.and(cleanup)
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
