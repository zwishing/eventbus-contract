use chrono::{DateTime, Utc};
use std::time::Duration;

use crate::error::EventBusError;

#[derive(Debug, Clone)]
pub struct Notification {
    pub channel: String,
    pub payload: String,
    pub received_at: DateTime<Utc>,
}

pub trait Notifier: Send + Sync {
    async fn notify(&self, channel: &str, payload: &str) -> Result<(), EventBusError>;
}

pub trait Listener: Send + Sync {
    async fn listen(&self, channel: &str) -> Result<(), EventBusError>;
    async fn recv(&self) -> Result<Notification, EventBusError>;
    async fn close(&self) -> Result<(), EventBusError>;
}

pub trait Dispatcher: Send + Sync {
    async fn start(&self) -> Result<(), EventBusError>;
    async fn stop(&self) -> Result<(), EventBusError>;
    async fn dispatch_once(&self) -> Result<(), EventBusError>;
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
