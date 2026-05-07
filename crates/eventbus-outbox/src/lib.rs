#![allow(async_fn_in_trait)]
//! Transactional outbox + dispatcher + dead-letter store traits.
//!
//! This crate provides the trait surface for implementing the outbox
//! pattern on top of a database, plus a dispatcher that drives the relay
//! loop. The traits depend only on `eventbus-core` types.
//!
//! - [`OutboxStore`] / [`OutboxStatus`] — `Pending → Processing → Sent/Failed/Dead`
//!   state machine for the transactional-outbox pattern.
//! - [`DeadLetterStore`] / [`DeadLetterPolicy`] — exhausted-retry routing.
//! - [`Dispatcher`] / [`Notifier`] / [`Listener`] — outbox-relay worker traits.
//!
//! Enable via `eventbus-contract = { version = "0.2", features = ["outbox"] }`.

pub mod dispatcher;
pub mod outbox;

pub use dispatcher::{Config, Dispatcher, DispatcherConfig, Listener, Notification, Notifier};
pub use outbox::dead_letter::{DeadLetterDecision, DeadLetterPolicy, DeadLetterReason};
pub use outbox::{
    AppendRequest, DeadLetterMessageRecord, DeadLetterStore, OutboxMessageRecord, OutboxRecord,
    OutboxStatus, OutboxStore, StateTransitionStore, TransitionInput,
};
