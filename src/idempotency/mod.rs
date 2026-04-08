use chrono::{DateTime, Utc};

use crate::error::EventBusError;

// ---------------------------------------------------------------------------
// Idempotency store (basic dedup check)
// ---------------------------------------------------------------------------

pub trait IdempotencyStore: Send + Sync {
    async fn is_processed(
        &self,
        consumer_group: &str,
        message_uid: &str,
    ) -> Result<bool, EventBusError>;

    async fn mark_processed(
        &self,
        consumer_group: &str,
        message_uid: &str,
    ) -> Result<(), EventBusError>;
}

// ---------------------------------------------------------------------------
// Idempotency claim (lease-based dedup)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct IdempotencyClaim {
    pub consumer_group: String,
    pub message_uid: String,
    pub idempotency_key: String,
    pub claimed_at: DateTime<Utc>,
    pub lease_expire_at: Option<DateTime<Utc>>,
    pub processor_name: String,
}

pub trait IdempotencyClaimStore: Send + Sync {
    async fn claim(&self, claim: IdempotencyClaim) -> Result<bool, EventBusError>;

    async fn complete(
        &self,
        consumer_group: &str,
        message_uid: &str,
    ) -> Result<(), EventBusError>;

    async fn release(
        &self,
        consumer_group: &str,
        message_uid: &str,
    ) -> Result<(), EventBusError>;
}
