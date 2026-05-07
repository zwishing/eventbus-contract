#![allow(async_fn_in_trait)]
//! DDD integration-event surface for the
//! [`eventbus`](https://docs.rs/eventbus) facade.
//!
//! - [`IntegrationEvent`] — domain-event trait that contributes the kind,
//!   topic, and payload bytes.
//! - [`MessageFactory`] — builds an [`eventbus_core::Message`] envelope from
//!   an `IntegrationEvent`, applying tracing/idempotency headers.
//! - [`EventPublisher`] — thin wrapper around [`eventbus_core::Publisher`]
//!   that accepts integration events directly.
//!
//! Enable via `eventbus-contract = { version = "0.2", features = ["integration"] }`.

pub mod integration;

pub use integration::{EventPublisher, IntegrationEvent, MessageFactory};
