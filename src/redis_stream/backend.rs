use std::{
    collections::HashMap,
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use tokio::sync::{Mutex, Notify};

use crate::{DeliveryState, EventBusError, Message};

#[derive(Debug, Clone)]
pub struct ClaimedMessage {
    pub id: String,
    pub message: Message,
    pub state: DeliveryState,
}

pub trait StreamBackend: Send + Sync + 'static {
    fn create_group(
        &self,
        stream: &str,
        group: &str,
        start_id: &str,
    ) -> impl Future<Output = Result<(), EventBusError>> + Send;

    fn publish(
        &self,
        stream: &str,
        message: Message,
    ) -> impl Future<Output = Result<String, EventBusError>> + Send;

    fn reclaim_idle(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        min_idle: Duration,
        count: usize,
    ) -> impl Future<Output = Result<Vec<ClaimedMessage>, EventBusError>> + Send;

    fn read_new(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        count: usize,
    ) -> impl Future<Output = Result<Vec<ClaimedMessage>, EventBusError>> + Send;

    fn ack(
        &self,
        stream: &str,
        group: &str,
        message_id: &str,
    ) -> impl Future<Output = Result<(), EventBusError>> + Send;

    fn wait_for_messages(&self, timeout: Duration) -> impl Future<Output = ()> + Send;
}

#[derive(Debug, Default)]
pub struct MemoryStreamBackend {
    state: Mutex<MemoryState>,
    notify: Notify,
}

impl MemoryStreamBackend {
    pub async fn pending_count(&self, stream: &str, group: &str) -> usize {
        let state = self.state.lock().await;
        state
            .streams
            .get(stream)
            .and_then(|stream_state| stream_state.groups.get(group))
            .map_or(0, |group_state| group_state.pending.len())
    }

    pub async fn stream_len(&self, stream: &str) -> usize {
        let state = self.state.lock().await;
        state
            .streams
            .get(stream)
            .map_or(0, |stream_state| stream_state.entries.len())
    }
}

impl StreamBackend for MemoryStreamBackend {
    async fn create_group(
        &self,
        stream: &str,
        group: &str,
        start_id: &str,
    ) -> Result<(), EventBusError> {
        let mut state = self.state.lock().await;
        let stream_state = state.streams.entry(stream.to_string()).or_default();
        let cursor = if matches!(start_id.trim(), "0" | "0-0") {
            0
        } else {
            stream_state.entries.len()
        };
        stream_state
            .groups
            .entry(group.to_string())
            .or_insert_with(|| GroupState {
                cursor,
                pending: HashMap::new(),
            });
        Ok(())
    }

    async fn publish(
        &self,
        stream: &str,
        message: Message,
    ) -> Result<String, EventBusError> {
        let id = {
            let mut state = self.state.lock().await;
            let next = state.next_id;
            state.next_id += 1;
            let id = format!("{next}-0");
            state
                .streams
                .entry(stream.to_string())
                .or_default()
                .entries
                .push(StreamEntry {
                    id: id.clone(),
                    message,
                });
            id
        };

        self.notify.notify_waiters();
        Ok(id)
    }

    async fn reclaim_idle(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        min_idle: Duration,
        count: usize,
    ) -> Result<Vec<ClaimedMessage>, EventBusError> {
        let now = Utc::now();
        let instant = Instant::now();
        let mut state = self.state.lock().await;
        let Some(stream_state) = state.streams.get_mut(stream) else {
            return Ok(Vec::new());
        };
        let Some(group_state) = stream_state.groups.get_mut(group) else {
            return Ok(Vec::new());
        };

        let ids: Vec<String> = group_state
            .pending
            .iter()
            .filter(|(_, pending)| {
                pending.owner != consumer && pending.last_delivered_at.elapsed() >= min_idle
            })
            .map(|(id, _)| id.clone())
            .take(count)
            .collect();

        let mut claimed = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(pending) = group_state.pending.get_mut(&id) {
                pending.owner = consumer.to_string();
                pending.delivery_count += 1;
                pending.last_received = now;
                pending.last_delivered_at = instant;
                pending.redelivered = true;

                claimed.push(ClaimedMessage {
                    id,
                    message: pending.message.clone(),
                    state: DeliveryState {
                        attempt: pending.delivery_count,
                        max_attempt: pending.delivery_count,
                        first_received: pending.first_received,
                        last_received: pending.last_received,
                        redelivered: pending.redelivered,
                    },
                });
            }
        }

        Ok(claimed)
    }

    async fn read_new(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        count: usize,
    ) -> Result<Vec<ClaimedMessage>, EventBusError> {
        let now = Utc::now();
        let instant = Instant::now();
        let mut state = self.state.lock().await;
        let Some(stream_state) = state.streams.get_mut(stream) else {
            return Ok(Vec::new());
        };
        let Some(group_state) = stream_state.groups.get_mut(group) else {
            return Ok(Vec::new());
        };

        let end = (group_state.cursor + count).min(stream_state.entries.len());
        if end == group_state.cursor {
            return Ok(Vec::new());
        }

        let entries = stream_state.entries[group_state.cursor..end].to_vec();
        group_state.cursor = end;

        let mut claimed = Vec::with_capacity(entries.len());
        for entry in entries {
            let attempt = retry_attempt(&entry.message) + 1;
            group_state.pending.insert(
                entry.id.clone(),
                PendingEntry {
                    message: entry.message.clone(),
                    owner: consumer.to_string(),
                    delivery_count: attempt,
                    first_received: now,
                    last_received: now,
                    last_delivered_at: instant,
                    redelivered: false,
                },
            );

            claimed.push(ClaimedMessage {
                id: entry.id,
                message: entry.message,
                state: DeliveryState {
                    attempt,
                    max_attempt: attempt,
                    first_received: now,
                    last_received: now,
                    redelivered: false,
                },
            });
        }

        Ok(claimed)
    }

    async fn ack(
        &self,
        stream: &str,
        group: &str,
        message_id: &str,
    ) -> Result<(), EventBusError> {
        let mut state = self.state.lock().await;
        if let Some(stream_state) = state.streams.get_mut(stream) {
            if let Some(group_state) = stream_state.groups.get_mut(group) {
                group_state.pending.remove(message_id);
            }
        }

        Ok(())
    }

    async fn wait_for_messages(&self, timeout: Duration) {
        let notified = self.notify.notified();
        let _ = tokio::time::timeout(timeout, notified).await;
    }
}

#[derive(Debug, Default)]
struct MemoryState {
    next_id: u64,
    streams: HashMap<String, StreamState>,
}

#[derive(Debug, Default)]
struct StreamState {
    entries: Vec<StreamEntry>,
    groups: HashMap<String, GroupState>,
}

#[derive(Debug, Clone)]
struct StreamEntry {
    id: String,
    message: Message,
}

#[derive(Debug)]
struct GroupState {
    cursor: usize,
    pending: HashMap<String, PendingEntry>,
}

#[derive(Debug, Clone)]
struct PendingEntry {
    message: Message,
    owner: String,
    delivery_count: u32,
    first_received: DateTime<Utc>,
    last_received: DateTime<Utc>,
    last_delivered_at: Instant,
    redelivered: bool,
}

fn retry_attempt(message: &Message) -> u32 {
    message
        .headers
        .get("retry-attempt")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0)
}

pub(crate) type SharedBackend<B> = Arc<B>;
