use std::{
    sync::{atomic::{AtomicBool, Ordering}, Mutex},
    thread::JoinHandle,
};

use tokio::sync::watch;

use crate::{EventBusError, Subscription};

pub struct RedisStreamSubscription {
    name: String,
    closed: AtomicBool,
    close_tx: watch::Sender<bool>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl RedisStreamSubscription {
    pub(crate) fn new(
        name: String,
        close_tx: watch::Sender<bool>,
        handles: Vec<JoinHandle<()>>,
    ) -> Self {
        Self {
            name,
            closed: AtomicBool::new(false),
            close_tx,
            handles: Mutex::new(handles),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn close(&self) -> Result<(), EventBusError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let _ = self.close_tx.send(true);
        let handles = {
            let mut guard = self
                .handles
                .lock()
                .map_err(|_| EventBusError::Internal("subscription handles poisoned".into()))?;
            std::mem::take(&mut *guard)
        };

        tokio::task::spawn_blocking(move || {
            for handle in handles {
                let _ = handle.join();
            }
        })
        .await
        .map_err(|_| EventBusError::Internal("subscription thread panicked".into()))?;

        Ok(())
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
