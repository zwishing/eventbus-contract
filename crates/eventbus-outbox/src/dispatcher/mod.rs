use std::future::Future;

use chrono::{DateTime, Utc};
use std::time::Duration;

use eventbus_core::EventBusError;

#[derive(Debug, Clone)]
pub struct Notification {
    pub channel: String,
    pub payload: String,
    pub received_at: DateTime<Utc>,
}

pub trait Notifier: Send + Sync {
    fn notify(
        &self,
        channel: &str,
        payload: &str,
    ) -> impl Future<Output = Result<(), EventBusError>> + Send;
}

pub trait Listener: Send + Sync {
    fn listen(&self, channel: &str) -> impl Future<Output = Result<(), EventBusError>> + Send;
    fn recv(&self) -> impl Future<Output = Result<Notification, EventBusError>> + Send;
    fn close(&self) -> impl Future<Output = Result<(), EventBusError>> + Send;
}

pub trait Dispatcher: Send + Sync {
    fn start(&self) -> impl Future<Output = Result<(), EventBusError>> + Send;
    fn stop(&self) -> impl Future<Output = Result<(), EventBusError>> + Send;
    fn dispatch_once(&self) -> impl Future<Output = Result<(), EventBusError>> + Send;
}

#[derive(Debug, Clone)]
pub struct DispatcherConfig {
    pub channel: String,
    pub poll_interval: Duration,
    pub batch_size: usize,
    pub max_in_flight: usize,
    pub max_pending_acks: usize,
    pub worker_name: String,
    pub max_retry: usize,
    pub retry_backoff: Duration,
    pub stale_lock_timeout: Duration,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            channel: "evt_outbox_notify".to_string(),
            poll_interval: Duration::from_secs(10),
            batch_size: 100,
            max_in_flight: 256,
            max_pending_acks: 512,
            worker_name: "default".to_string(),
            max_retry: 16,
            retry_backoff: Duration::from_secs(3),
            stale_lock_timeout: Duration::from_secs(60),
        }
    }
}

impl DispatcherConfig {
    pub fn with_worker(worker_name: impl Into<String>) -> Self {
        Self {
            worker_name: worker_name.into(),
            ..Self::default()
        }
    }
}

pub type Config = DispatcherConfig;
