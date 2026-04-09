use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::{EventBusError, Subscription};

pub struct RedisStreamSubscription {
    name: String,
    closed: AtomicBool,
    close_tx: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<Result<(), EventBusError>>>>,
}

impl RedisStreamSubscription {
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

impl Subscription for RedisStreamSubscription {
    fn name(&self) -> &str {
        self.name()
    }

    async fn close(&self) -> Result<(), EventBusError> {
        RedisStreamSubscription::close(self).await
    }
}

impl Drop for RedisStreamSubscription {
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
