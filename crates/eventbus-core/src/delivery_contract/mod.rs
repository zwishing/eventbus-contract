use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::EventBusError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeliveryOutcome {
    Acked,
    Nacked,
    Retried,
    DeadLetter,
}

/// Backend-supplied delivery facts for an in-flight message.
///
/// Backends emit this from `read_new` / `reclaim_idle`; the bus layer pairs
/// it with the subscription's `max_retry` to construct the full
/// [`DeliveryState`] that handlers see via [`DeliveryInspector::state`].
///
/// Splitting the type makes the protocol explicit at the type level: a
/// backend can't accidentally fabricate `max_attempt`, and the bus can't
/// accidentally forget to set it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartialDeliveryState {
    pub attempt: u32,
    pub first_received: DateTime<Utc>,
    pub last_received: DateTime<Utc>,
    pub redelivered: bool,
}

impl PartialDeliveryState {
    /// Pair with the subscription-level retry budget to produce the full
    /// [`DeliveryState`] visible to handlers.
    #[must_use]
    pub fn with_max_attempt(self, max_attempt: u32) -> DeliveryState {
        DeliveryState {
            attempt: self.attempt,
            max_attempt,
            first_received: self.first_received,
            last_received: self.last_received,
            redelivered: self.redelivered,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryState {
    pub attempt: u32,
    pub max_attempt: u32,
    pub first_received: DateTime<Utc>,
    pub last_received: DateTime<Utc>,
    pub redelivered: bool,
}

pub trait DeliveryInspector: Send + Sync {
    fn state(&self) -> crate::BoxFuture<'_, Result<DeliveryState, EventBusError>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_outcome_uses_go_wire_names() {
        assert_eq!(
            serde_json::to_string(&DeliveryOutcome::Acked).unwrap(),
            "\"acked\""
        );
        assert_eq!(
            serde_json::to_string(&DeliveryOutcome::Nacked).unwrap(),
            "\"nacked\""
        );
        assert_eq!(
            serde_json::to_string(&DeliveryOutcome::Retried).unwrap(),
            "\"retried\""
        );
        assert_eq!(
            serde_json::to_string(&DeliveryOutcome::DeadLetter).unwrap(),
            "\"dead-letter\"",
        );

        assert_eq!(
            serde_json::from_str::<DeliveryOutcome>("\"dead-letter\"").unwrap(),
            DeliveryOutcome::DeadLetter,
        );
    }
}
