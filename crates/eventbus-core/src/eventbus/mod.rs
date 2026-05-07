use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::contract::{
    AckMode, BackpressurePolicy, ConsumerBalanceMode, DeliveryGuarantee, OrderingMode,
    OverflowStrategy, PublishConfirmation,
};
use crate::delivery_contract::DeliveryInspector;
use crate::error::EventBusError;

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

pub type Topic = String;
pub type Headers = HashMap<String, String>;

// ---------------------------------------------------------------------------
// Message
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub uid: String,
    pub topic: Topic,
    pub key: String,
    pub kind: String,
    pub source: String,
    pub occurred_at: DateTime<Utc>,
    pub headers: Headers,
    #[serde(with = "crate::serde_bytes")]
    pub payload: bytes::Bytes,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_uid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_uid: Option<String>,
}

// ---------------------------------------------------------------------------
// Publish options (builder pattern)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
#[must_use = "PublishOptions is inert until passed to publish/publish_batch"]
pub struct PublishOptions {
    /// Blocks the calling task for the specified duration before publishing.
    /// For high-throughput scenarios, consider handling delays externally.
    pub delay: Option<Duration>,
    pub ordered_key: Option<String>,
    pub metadata: HashMap<String, String>,
    pub require_ordered_key: bool,
    pub guarantee: Option<DeliveryGuarantee>,
    pub confirmation: Option<PublishConfirmation>,
    pub idempotency_key: Option<String>,
    pub backpressure: Option<BackpressurePolicy>,
    pub topic_ttl: Option<Duration>,
    pub expected_content_type: Option<String>,
    pub expected_event_version: Option<String>,
}

impl PublishOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    pub fn with_ordered_key(mut self, key: impl Into<String>) -> Self {
        self.ordered_key = Some(key.into());
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn with_require_ordered_key(mut self) -> Self {
        self.require_ordered_key = true;
        self
    }

    pub fn with_guarantee(mut self, g: DeliveryGuarantee) -> Self {
        self.guarantee = Some(g);
        self
    }

    pub fn with_confirmation(mut self, c: PublishConfirmation) -> Self {
        self.confirmation = Some(c);
        self
    }

    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    pub fn with_backpressure(mut self, bp: BackpressurePolicy) -> Self {
        self.backpressure = Some(bp);
        self
    }

    pub fn with_topic_ttl(mut self, ttl: Duration) -> Self {
        self.topic_ttl = Some(ttl);
        self
    }

    pub fn with_expected_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.expected_content_type = Some(content_type.into());
        self
    }

    pub fn with_expected_event_version(mut self, version: impl Into<String>) -> Self {
        self.expected_event_version = Some(version.into());
        self
    }

    pub fn validate(&self) -> Result<(), EventBusError> {
        if self.require_ordered_key
            && self
                .ordered_key
                .as_ref()
                .is_none_or(|k| k.trim().is_empty())
        {
            return Err(EventBusError::Validation("ordered key is required".into()));
        }

        if self.guarantee == Some(DeliveryGuarantee::ExactlyOnce)
            && self.confirmation != Some(PublishConfirmation::Persisted)
        {
            return Err(EventBusError::Validation(format!(
                "exactly-once requires {:?} confirmation",
                PublishConfirmation::Persisted,
            )));
        }

        if let Some(ref bp) = self.backpressure {
            bp.validate()?;
        }

        if let Some(ttl) = self.topic_ttl {
            if ttl.is_zero() {
                return Err(EventBusError::Validation("topic ttl must be > 0".into()));
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Subscription config
// ---------------------------------------------------------------------------

/// Subscription configuration.
///
/// ## In-flight sizing precedence
///
/// During [`SubscriptionConfig::apply_defaults`] the bus reconciles three
/// related inputs:
///
/// 1. If `backpressure` is set, its `max_in_flight` / `max_pending_acks` seed
///    the matching fields when those are still `0`. Setting both
///    `backpressure.max_in_flight` and `max_in_flight` to **different**
///    non-zero values is a configuration error surfaced by `validate`.
/// 2. `max_in_flight` falls back to `1` if still unset.
/// 3. `max_pending_acks` falls back to `2 * max_in_flight`.
/// 4. If `backpressure` was unset, one is synthesized from the resolved
///    `max_in_flight` / `max_pending_acks`.
#[derive(Debug, Clone)]
pub struct SubscriptionConfig {
    pub topic: Topic,
    pub consumer_group: String,
    pub consumer_name: String,
    pub max_retry: usize,
    pub retry_backoff: Duration,
    pub dead_letter_topic: Option<Topic>,
    pub ack_mode: AckMode,
    pub ordering_mode: Option<OrderingMode>,
    pub balance_mode: Option<ConsumerBalanceMode>,
    pub guarantee: Option<DeliveryGuarantee>,
    pub max_in_flight: usize,
    pub max_pending_acks: usize,
    pub wildcard_topic: bool,
    pub backpressure: Option<BackpressurePolicy>,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            topic: String::new(),
            consumer_group: String::new(),
            consumer_name: String::new(),
            max_retry: 0,
            retry_backoff: Duration::ZERO,
            dead_letter_topic: None,
            ack_mode: AckMode::Manual,
            ordering_mode: None,
            balance_mode: None,
            guarantee: None,
            max_in_flight: 0,
            max_pending_acks: 0,
            wildcard_topic: false,
            backpressure: None,
        }
    }
}

impl SubscriptionConfig {
    /// Fill in zero-value fields with sensible defaults.
    pub fn apply_defaults(&mut self) {
        if self.ordering_mode.is_none() {
            self.ordering_mode = Some(OrderingMode::None);
        }

        if self.balance_mode.is_none() {
            self.balance_mode = Some(ConsumerBalanceMode::Competing);
        }

        if self.guarantee.is_none() {
            self.guarantee = Some(DeliveryGuarantee::AtLeastOnce);
        }

        if let Some(ref bp) = self.backpressure {
            if self.max_in_flight == 0 {
                self.max_in_flight = bp.max_in_flight;
            }
            if self.max_pending_acks == 0 {
                self.max_pending_acks = bp.max_pending_acks;
            }
        }

        if self.max_in_flight == 0 {
            self.max_in_flight = 1;
        }
        if self.max_pending_acks == 0 {
            self.max_pending_acks = self.max_in_flight * 2;
        }
        if self.retry_backoff.is_zero() {
            self.retry_backoff = Duration::from_millis(100);
        }

        if self.backpressure.is_none() {
            self.backpressure = Some(BackpressurePolicy {
                max_in_flight: self.max_in_flight,
                max_pending_acks: self.max_pending_acks,
                max_batch_size: self.max_in_flight,
                overflow_strategy: OverflowStrategy::Reject,
            });
        }
    }

    /// Check config consistency without mutating fields.
    /// Call `apply_defaults` first, or use `normalize_and_validate`.
    pub fn validate(&self) -> Result<(), EventBusError> {
        if self.ordering_mode.is_none() {
            return Err(EventBusError::Validation(
                "ordering mode is required".into(),
            ));
        }

        if self.balance_mode.is_none() {
            return Err(EventBusError::Validation("balance mode is required".into()));
        }

        if self.guarantee.is_none() {
            return Err(EventBusError::Validation(
                "delivery guarantee is required".into(),
            ));
        }

        if let Some(ref bp) = self.backpressure {
            bp.validate()?;
            if self.max_in_flight > 0 && self.max_in_flight != bp.max_in_flight {
                return Err(EventBusError::Validation(
                    "max in flight conflicts with backpressure policy".into(),
                ));
            }
            if self.max_pending_acks > 0 && self.max_pending_acks != bp.max_pending_acks {
                return Err(EventBusError::Validation(
                    "max pending acks conflicts with backpressure policy".into(),
                ));
            }
        }

        if self.max_pending_acks < self.max_in_flight {
            return Err(EventBusError::Validation(
                "max pending acks must be >= max in flight".into(),
            ));
        }

        Ok(())
    }

    /// Apply defaults then validate. Recommended single-call entrypoint.
    pub fn normalize_and_validate(&mut self) -> Result<(), EventBusError> {
        self.apply_defaults();
        self.validate()
    }
}

// ---------------------------------------------------------------------------
// Traits
// ---------------------------------------------------------------------------

pub trait Delivery: DeliveryInspector + Send + Sync {
    fn message(&self) -> &Message;
    fn ack(&self) -> crate::BoxFuture<'_, Result<(), EventBusError>>;
    fn nack(
        &self,
        reason: &(dyn std::error::Error + Send + Sync),
    ) -> crate::BoxFuture<'_, Result<(), EventBusError>>;
    fn retry(
        &self,
        reason: &(dyn std::error::Error + Send + Sync),
    ) -> crate::BoxFuture<'_, Result<(), EventBusError>>;
}

pub trait Handler: Send + Sync {
    fn handle<'a>(
        &'a self,
        delivery: &'a (dyn Delivery + Send + Sync),
    ) -> crate::BoxFuture<'a, Result<(), EventBusError>>;
}

pub trait Subscription: Send + Sync {
    fn name(&self) -> &str;
    fn close(self: Arc<Self>) -> crate::BoxFuture<'static, Result<(), EventBusError>>;
}

/// Stream / queue identifier for a published message. PR 4 will harden this
/// into a `#[repr(transparent)]` newtype with `Deref<Target = str>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(pub String);

impl MessageId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Per-message result for `publish_batch`. PR 5 will populate `results`
/// during a full re-implementation that no longer fails fast.
#[derive(Debug)]
#[non_exhaustive]
pub struct BatchOutcome {
    pub results: Vec<Result<MessageId, EventBusError>>,
}

impl BatchOutcome {
    #[must_use]
    pub fn all_ok(&self) -> bool {
        self.results.iter().all(Result::is_ok)
    }
    #[must_use]
    pub fn ok_count(&self) -> usize {
        self.results.iter().filter(|r| r.is_ok()).count()
    }
    #[must_use]
    pub fn err_count(&self) -> usize {
        self.results.len() - self.ok_count()
    }
    pub fn errors(&self) -> impl Iterator<Item = &EventBusError> {
        self.results.iter().filter_map(|r| r.as_ref().err())
    }
}

pub trait Publisher: Send + Sync {
    fn publish(
        &self,
        msg: Message,
        opts: PublishOptions,
    ) -> crate::BoxFuture<'_, Result<MessageId, EventBusError>>;

    fn publish_batch(
        &self,
        msgs: Vec<Message>,
        opts: PublishOptions,
    ) -> crate::BoxFuture<'_, Result<BatchOutcome, EventBusError>>;
}

/// Convenience layer over [`Publisher`]. Generic methods that monomorphize
/// for callers who don't need dynamic dispatch.
pub trait PublisherExt: Publisher {
    /// Publish an iterator of messages. Collects into `Vec` and delegates
    /// to [`Publisher::publish_batch`]. The collect is mandatory because the
    /// dyn-safe `publish_batch` cannot accept an opaque iterator.
    fn publish_iter<I>(
        &self,
        msgs: I,
        opts: PublishOptions,
    ) -> crate::BoxFuture<'_, Result<BatchOutcome, EventBusError>>
    where
        I: IntoIterator<Item = Message> + Send + 'static,
        I::IntoIter: Send,
    {
        let collected: Vec<Message> = msgs.into_iter().collect();
        self.publish_batch(collected, opts)
    }
}

impl<P: Publisher + ?Sized> PublisherExt for P {}

pub trait Subscriber: Send + Sync {
    fn subscribe(
        &self,
        cfg: SubscriptionConfig,
        handler: Arc<dyn Handler>,
    ) -> crate::BoxFuture<'_, Result<Arc<dyn Subscription>, EventBusError>>;
}

/// Convenience layer over [`Subscriber`]. Generic methods that monomorphize
/// for callers who don't need dynamic dispatch.
pub trait SubscriberExt: Subscriber {
    fn subscribe_with<H>(
        &self,
        cfg: SubscriptionConfig,
        handler: H,
    ) -> crate::BoxFuture<'_, Result<Arc<dyn Subscription>, EventBusError>>
    where
        H: Handler + 'static,
    {
        self.subscribe(cfg, Arc::new(handler))
    }
}

impl<S: Subscriber + ?Sized> SubscriberExt for S {}

pub trait Bus: Publisher + Subscriber + Send + Sync {}

impl<T> Bus for T where T: Publisher + Subscriber + Send + Sync {}

/// Pluggable wire-format encoder for [`Message`].
///
/// Object-safe: backends store an `Arc<dyn Codec>` and dispatch through it.
/// Implementations own the full envelope so swapping codecs is a binary
/// decision (e.g. JSON for cross-language compat vs. binary for throughput).
pub trait Codec: Send + Sync {
    fn name(&self) -> &str;
    fn encode(&self, msg: &Message) -> Result<Vec<u8>, EventBusError>;
    fn decode(&self, bytes: &[u8]) -> Result<Message, EventBusError>;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_options_accepts_valid() {
        let opts = PublishOptions::new()
            .with_ordered_key("user-1")
            .with_require_ordered_key()
            .with_guarantee(DeliveryGuarantee::AtLeastOnce)
            .with_confirmation(PublishConfirmation::Accepted)
            .with_backpressure(BackpressurePolicy {
                max_in_flight: 10,
                max_pending_acks: 20,
                max_batch_size: 10,
                overflow_strategy: OverflowStrategy::Reject,
            });
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn publish_options_rejects_missing_ordered_key() {
        let opts = PublishOptions::new().with_require_ordered_key();
        assert!(opts.validate().is_err());
    }

    #[test]
    fn publish_options_rejects_exactly_once_without_persisted() {
        let opts = PublishOptions::new()
            .with_guarantee(DeliveryGuarantee::ExactlyOnce)
            .with_confirmation(PublishConfirmation::Accepted);
        assert!(opts.validate().is_err());
    }

    #[test]
    fn publish_options_rejects_exactly_once_without_confirmation() {
        let opts = PublishOptions::new().with_guarantee(DeliveryGuarantee::ExactlyOnce);
        assert!(opts.validate().is_err());
    }

    #[test]
    fn publish_options_rejects_zero_ttl() {
        let opts = PublishOptions::new().with_topic_ttl(Duration::ZERO);
        assert!(opts.validate().is_err());
    }

    #[test]
    fn subscription_config_preserves_explicit_ack_mode() {
        let mut cfg = SubscriptionConfig {
            ack_mode: AckMode::AutoOnHandlerSuccess,
            max_in_flight: 8,
            ..Default::default()
        };
        assert!(cfg.normalize_and_validate().is_ok());
        assert_eq!(cfg.ack_mode, AckMode::AutoOnHandlerSuccess);
    }

    #[test]
    fn subscription_config_defaults_to_manual_ack() {
        let mut cfg = SubscriptionConfig {
            max_in_flight: 8,
            ..Default::default()
        };
        assert!(cfg.normalize_and_validate().is_ok());
        assert_eq!(cfg.ack_mode, AckMode::Manual);
    }

    #[test]
    fn subscription_config_rejects_conflicting_backpressure() {
        let mut cfg = SubscriptionConfig {
            ack_mode: AckMode::Manual,
            max_in_flight: 8,
            backpressure: Some(BackpressurePolicy {
                max_in_flight: 16,
                max_pending_acks: 32,
                max_batch_size: 8,
                overflow_strategy: OverflowStrategy::Reject,
            }),
            ..Default::default()
        };
        assert!(cfg.normalize_and_validate().is_err());
    }
}
