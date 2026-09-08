use std::future::Future;

use chrono::{DateTime, Utc};

use eventbus_core::EventBusError;

// ---------------------------------------------------------------------------
// Idempotency store (basic dedup check)
// ---------------------------------------------------------------------------

pub trait IdempotencyStore: Send + Sync {
    fn is_processed(
        &self,
        consumer_group: &str,
        message_uid: &str,
    ) -> impl Future<Output = Result<bool, EventBusError>> + Send;

    fn mark_processed(
        &self,
        consumer_group: &str,
        message_uid: &str,
    ) -> impl Future<Output = Result<(), EventBusError>> + Send;
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
    fn claim(
        &self,
        claim: IdempotencyClaim,
    ) -> impl Future<Output = Result<bool, EventBusError>> + Send;

    fn complete(
        &self,
        consumer_group: &str,
        message_uid: &str,
    ) -> impl Future<Output = Result<(), EventBusError>> + Send;

    fn release(
        &self,
        consumer_group: &str,
        message_uid: &str,
    ) -> impl Future<Output = Result<(), EventBusError>> + Send;
}
