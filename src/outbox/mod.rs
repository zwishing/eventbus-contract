pub mod dead_letter;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::time::Duration;

use crate::error::EventBusError;
use crate::eventbus::{Headers, Topic};
use crate::outbox::dead_letter::DeadLetterReason;

// ---------------------------------------------------------------------------
// Outbox status + state machine
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutboxStatus {
    Pending,
    Processing,
    Sent,
    Failed,
    Dead,
}

impl OutboxStatus {
    /// Returns the set of statuses reachable from `self`.
    pub fn allowed_transitions(self) -> &'static [OutboxStatus] {
        match self {
            Self::Pending => &[Self::Processing],
            Self::Processing => &[Self::Sent, Self::Failed, Self::Dead],
            Self::Failed => &[Self::Processing, Self::Dead],
            Self::Sent | Self::Dead => &[],
        }
    }

    pub fn can_transition_to(self, to: Self) -> bool {
        self.allowed_transitions().contains(&to)
    }

    pub fn validate_transition(self, to: Self) -> Result<(), EventBusError> {
        if self.can_transition_to(to) {
            Ok(())
        } else {
            Err(EventBusError::InvalidTransition {
                from: format!("{self:?}"),
                to: format!("{to:?}"),
            })
        }
    }
}

impl std::fmt::Display for OutboxStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Pending => "pending",
            Self::Processing => "processing",
            Self::Sent => "sent",
            Self::Failed => "failed",
            Self::Dead => "dead",
        };
        f.write_str(s)
    }
}

// ---------------------------------------------------------------------------
// Transition input
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TransitionInput {
    pub uid: String,
    pub from: OutboxStatus,
    pub to: OutboxStatus,
    pub last_error: Option<String>,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub sent_at: Option<DateTime<Utc>>,
    pub dead_letter_reason: Option<DeadLetterReason>,
}

// ---------------------------------------------------------------------------
// Append request
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct AppendRequest {
    pub message_uid: String,
    pub topic: Topic,
    pub key: String,
    pub kind: String,
    pub source: String,
    pub headers: Headers,
    pub payload: Vec<u8>,
    pub content_type: Option<String>,
    pub event_version: Option<String>,
    pub idempotency_key: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
    pub occurred_at: DateTime<Utc>,
    pub trace_uid: Option<String>,
    pub correlation_uid: Option<String>,
}

// ---------------------------------------------------------------------------
// Outbox message record
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct OutboxMessageRecord {
    pub uid: String,
    pub message_uid: String,
    pub topic: Topic,
    pub key: String,
    pub kind: String,
    pub source: String,
    pub headers: Headers,
    pub payload: Vec<u8>,
    pub content_type: Option<String>,
    pub event_version: Option<String>,
    pub idempotency_key: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
    pub occurred_at: DateTime<Utc>,
    pub trace_uid: Option<String>,
    pub correlation_uid: Option<String>,
    pub status: OutboxStatus,
    pub retry_count: u32,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub sent_at: Option<DateTime<Utc>>,
    pub locked_at: Option<DateTime<Utc>>,
    pub locked_by: Option<String>,
}

pub type OutboxRecord = OutboxMessageRecord;

// ---------------------------------------------------------------------------
// Dead letter message record
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct DeadLetterMessageRecord {
    pub uid: String,
    pub message_uid: String,
    pub topic: Topic,
    pub key: String,
    pub kind: String,
    pub source: String,
    pub headers: Headers,
    pub payload: Bytes,
    pub content_type: Option<String>,
    pub event_version: Option<String>,
    pub idempotency_key: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
    pub trace_uid: Option<String>,
    pub correlation_uid: Option<String>,
    pub failure_reason: String,
    pub failed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Repository traits
// ---------------------------------------------------------------------------

pub trait OutboxStore: Send + Sync {
    async fn append(&self, req: AppendRequest) -> Result<(), EventBusError>;

    async fn append_batch(&self, reqs: Vec<AppendRequest>) -> Result<(), EventBusError>;

    async fn lock_pending(
        &self,
        worker: &str,
        limit: usize,
        now: DateTime<Utc>,
    ) -> Result<Vec<OutboxMessageRecord>, EventBusError>;

    async fn mark_sent(&self, uids: &[String], sent_at: DateTime<Utc>)
        -> Result<(), EventBusError>;

    async fn mark_failed(
        &self,
        uid: &str,
        next_retry_at: Option<DateTime<Utc>>,
        last_error: &str,
    ) -> Result<(), EventBusError>;

    async fn mark_dead(&self, uid: &str, last_error: &str) -> Result<(), EventBusError>;

    async fn release_stale_locks(
        &self,
        timeout: Duration,
        now: DateTime<Utc>,
    ) -> Result<(), EventBusError>;
}

pub trait StateTransitionStore: Send + Sync {
    async fn transition(&self, input: TransitionInput) -> Result<(), EventBusError>;
}

pub trait DeadLetterStore: Send + Sync {
    async fn append_dead_letter(&self, msg: DeadLetterMessageRecord) -> Result<(), EventBusError>;
}

pub trait Repository: OutboxStore {}

impl<T> Repository for T where T: OutboxStore + ?Sized {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_to_processing_allowed() {
        assert!(OutboxStatus::Pending.can_transition_to(OutboxStatus::Processing));
    }

    #[test]
    fn pending_to_sent_rejected() {
        assert!(!OutboxStatus::Pending.can_transition_to(OutboxStatus::Sent));
    }

    #[test]
    fn failed_to_processing_allowed_for_retry() {
        assert!(OutboxStatus::Failed.can_transition_to(OutboxStatus::Processing));
    }

    #[test]
    fn processing_to_dead_allowed() {
        assert!(OutboxStatus::Processing
            .validate_transition(OutboxStatus::Dead)
            .is_ok());
    }

    #[test]
    fn sent_is_terminal() {
        assert!(OutboxStatus::Sent.allowed_transitions().is_empty());
    }

    #[test]
    fn dead_is_terminal() {
        assert!(OutboxStatus::Dead.allowed_transitions().is_empty());
    }
}
