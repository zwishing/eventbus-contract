use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Dead letter reason
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeadLetterReason {
    MaxRetryExceeded,
    Expired,
    Terminal,
    Retriable,
}

// ---------------------------------------------------------------------------
// Dead letter decision
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterDecision {
    pub move_to_dead: bool,
    pub reason: DeadLetterReason,
}

// ---------------------------------------------------------------------------
// Dead letter policy
// ---------------------------------------------------------------------------

/// Controls when messages are moved to the dead-letter queue.
///
/// `max_retry`: positive value = max attempts before dead-letter;
/// 0 or negative = unlimited retries.
#[derive(Debug, Clone)]
pub struct DeadLetterPolicy {
    pub max_retry: i32,
    pub move_on_expired: bool,
    pub move_on_terminal: bool,
}

impl DeadLetterPolicy {
    pub fn decide(
        &self,
        retry_count: i32,
        terminal: bool,
        expires_at: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> DeadLetterDecision {
        if self.max_retry > 0 && retry_count >= self.max_retry {
            return DeadLetterDecision {
                move_to_dead: true,
                reason: DeadLetterReason::MaxRetryExceeded,
            };
        }

        if self.move_on_expired {
            if let Some(exp) = expires_at {
                if now > exp {
                    return DeadLetterDecision {
                        move_to_dead: true,
                        reason: DeadLetterReason::Expired,
                    };
                }
            }
        }

        if self.move_on_terminal && terminal {
            return DeadLetterDecision {
                move_to_dead: true,
                reason: DeadLetterReason::Terminal,
            };
        }

        DeadLetterDecision {
            move_to_dead: false,
            reason: DeadLetterReason::Retriable,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn max_retry_exceeded_routes_to_dead() {
        let policy = DeadLetterPolicy {
            max_retry: 3,
            move_on_expired: true,
            move_on_terminal: true,
        };
        let now = Utc::now();
        let decision = policy.decide(3, false, None, now);
        assert!(decision.move_to_dead);
        assert_eq!(decision.reason, DeadLetterReason::MaxRetryExceeded);
    }

    #[test]
    fn expired_message_routes_to_dead() {
        let policy = DeadLetterPolicy {
            max_retry: 3,
            move_on_expired: true,
            move_on_terminal: true,
        };
        let now = Utc::now();
        let expired = now - Duration::minutes(1);
        let decision = policy.decide(1, false, Some(expired), now);
        assert!(decision.move_to_dead);
        assert_eq!(decision.reason, DeadLetterReason::Expired);
    }

    #[test]
    fn terminal_failure_routes_to_dead() {
        let policy = DeadLetterPolicy {
            max_retry: 3,
            move_on_expired: true,
            move_on_terminal: true,
        };
        let now = Utc::now();
        let decision = policy.decide(1, true, None, now);
        assert!(decision.move_to_dead);
        assert_eq!(decision.reason, DeadLetterReason::Terminal);
    }

    #[test]
    fn retriable_message_stays() {
        let policy = DeadLetterPolicy {
            max_retry: 3,
            move_on_expired: true,
            move_on_terminal: true,
        };
        let now = Utc::now();
        let decision = policy.decide(1, false, None, now);
        assert!(!decision.move_to_dead);
        assert_eq!(decision.reason, DeadLetterReason::Retriable);
    }

    #[test]
    fn zero_max_retry_means_unlimited() {
        let policy = DeadLetterPolicy {
            max_retry: 0,
            move_on_expired: false,
            move_on_terminal: false,
        };
        let now = Utc::now();
        assert!(!policy.decide(0, false, None, now).move_to_dead);
        assert!(!policy.decide(100, false, None, now).move_to_dead);
    }
}
