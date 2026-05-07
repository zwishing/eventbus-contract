//! Transactional outbox helpers for the
//! [`eventbus`](https://docs.rs/eventbus) facade.
//!
//! Re-exports from `eventbus-core`:
//! - [`OutboxStore`] / [`OutboxStatus`] — `Pending → Processing → Sent/Failed/Dead`
//!   state machine for the transactional-outbox pattern.
//! - [`DeadLetterStore`] / `DeadLetterPolicy` — exhausted-retry routing.
//! - [`Dispatcher`] / [`Notifier`] / [`Listener`] — outbox-relay worker traits.
//!
//! Enable via `eventbus = { version = "0.2", features = ["outbox"] }`.

pub use eventbus_core::dispatcher::*;
pub use eventbus_core::outbox::*;
