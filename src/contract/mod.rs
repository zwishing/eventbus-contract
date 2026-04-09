use serde::{Deserialize, Serialize};

use crate::error::EventBusError;

// ---------------------------------------------------------------------------
// Delivery guarantee
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeliveryGuarantee {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

// ---------------------------------------------------------------------------
// Publish confirmation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PublishConfirmation {
    FireAndForget,
    Accepted,
    Persisted,
}

// ---------------------------------------------------------------------------
// Guarantee matrix
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct GuaranteeMatrix {
    pub publish: DeliveryGuarantee,
    pub consume: DeliveryGuarantee,
    pub confirmation: PublishConfirmation,
}

impl GuaranteeMatrix {
    pub fn validate(&self) -> Result<(), EventBusError> {
        let needs_persisted = self.publish == DeliveryGuarantee::ExactlyOnce
            || self.consume == DeliveryGuarantee::ExactlyOnce;

        if needs_persisted && self.confirmation != PublishConfirmation::Persisted {
            return Err(EventBusError::Validation(format!(
                "exactly-once requires {:?} confirmation",
                PublishConfirmation::Persisted,
            )));
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Overflow strategy
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OverflowStrategy {
    Reject,
    Block,
    DropNewest,
    DropOldest,
}

// ---------------------------------------------------------------------------
// Backpressure policy
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct BackpressurePolicy {
    pub max_in_flight: usize,
    pub max_pending_acks: usize,
    pub max_batch_size: usize,
    pub overflow_strategy: OverflowStrategy,
}

impl BackpressurePolicy {
    pub fn validate(&self) -> Result<(), EventBusError> {
        if self.max_in_flight == 0 {
            return Err(EventBusError::Validation(
                "max in flight must be > 0".into(),
            ));
        }
        if self.max_pending_acks == 0 {
            return Err(EventBusError::Validation(
                "max pending acks must be > 0".into(),
            ));
        }
        if self.max_pending_acks < self.max_in_flight {
            return Err(EventBusError::Validation(
                "max pending acks must be >= max in flight".into(),
            ));
        }
        if self.max_batch_size == 0 {
            return Err(EventBusError::Validation(
                "max batch size must be > 0".into(),
            ));
        }
        if self.max_batch_size > self.max_in_flight {
            return Err(EventBusError::Validation(
                "max batch size must be <= max in flight".into(),
            ));
        }
        if self.max_batch_size > self.max_pending_acks {
            return Err(EventBusError::Validation(
                "max batch size must be <= max pending acks".into(),
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Ack mode
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AckMode {
    Manual,
    AutoOnReceive,
    AutoOnHandlerSuccess,
}

// ---------------------------------------------------------------------------
// Ordering mode
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OrderingMode {
    None,
    Key,
}

// ---------------------------------------------------------------------------
// Consumer balance mode
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConsumerBalanceMode {
    Competing,
    FanOut,
}

// ---------------------------------------------------------------------------
// Subscription semantics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SubscriptionSemantics {
    pub ack_mode: AckMode,
    pub ordering_mode: OrderingMode,
    pub balance_mode: ConsumerBalanceMode,
    pub wildcard_topic: bool,
    pub require_ordered_key: bool,
}

impl SubscriptionSemantics {
    pub fn validate(&self) -> Result<(), EventBusError> {
        if self.require_ordered_key && self.ordering_mode != OrderingMode::Key {
            return Err(EventBusError::Validation(format!(
                "ordered key can only be required when ordering mode is {:?}",
                OrderingMode::Key,
            )));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guarantee_matrix_accepts_at_least_once_with_accepted() {
        let matrix = GuaranteeMatrix {
            publish: DeliveryGuarantee::AtLeastOnce,
            consume: DeliveryGuarantee::AtLeastOnce,
            confirmation: PublishConfirmation::Accepted,
        };
        assert!(matrix.validate().is_ok());
    }

    #[test]
    fn guarantee_matrix_rejects_exactly_once_without_persisted() {
        let matrix = GuaranteeMatrix {
            publish: DeliveryGuarantee::ExactlyOnce,
            consume: DeliveryGuarantee::ExactlyOnce,
            confirmation: PublishConfirmation::Accepted,
        };
        assert!(matrix.validate().is_err());
    }

    #[test]
    fn backpressure_accepts_valid_policy() {
        let policy = BackpressurePolicy {
            max_in_flight: 128,
            max_pending_acks: 256,
            max_batch_size: 64,
            overflow_strategy: OverflowStrategy::Reject,
        };
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn backpressure_rejects_zero_in_flight() {
        let policy = BackpressurePolicy {
            max_in_flight: 0,
            max_pending_acks: 10,
            max_batch_size: 1,
            overflow_strategy: OverflowStrategy::Reject,
        };
        assert!(policy.validate().is_err());
    }

    #[test]
    fn backpressure_rejects_pending_less_than_in_flight() {
        let policy = BackpressurePolicy {
            max_in_flight: 10,
            max_pending_acks: 5,
            max_batch_size: 1,
            overflow_strategy: OverflowStrategy::Reject,
        };
        assert!(policy.validate().is_err());
    }

    #[test]
    fn backpressure_rejects_batch_larger_than_in_flight() {
        let policy = BackpressurePolicy {
            max_in_flight: 4,
            max_pending_acks: 8,
            max_batch_size: 5,
            overflow_strategy: OverflowStrategy::Reject,
        };
        assert!(policy.validate().is_err());
    }

    #[test]
    fn subscription_semantics_rejects_require_ordered_key_without_key_mode() {
        let sem = SubscriptionSemantics {
            ack_mode: AckMode::Manual,
            ordering_mode: OrderingMode::None,
            balance_mode: ConsumerBalanceMode::Competing,
            wildcard_topic: false,
            require_ordered_key: true,
        };
        assert!(sem.validate().is_err());
    }
}
