use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::future::Future;

use crate::error::EventBusError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeliveryOutcome {
    Acked,
    Nacked,
    Retried,
    DeadLetter,
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
    fn state(&self) -> impl Future<Output = Result<DeliveryState, EventBusError>> + Send;
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
