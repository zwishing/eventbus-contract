#![allow(async_fn_in_trait)]
//! Object-safe event-bus contract: traits, value types, and the generic
//! Stream bus.
//!
//! - [`Publisher`] / [`Subscriber`] / [`Bus`] — dyn-safe core traits
//! - [`PublisherExt`] / [`SubscriberExt`] — monomorphic convenience extensions
//! - [`Handler`] receives `Box<dyn DeliveryHandle>`
//! - [`Delivery`] (read) + [`DeliveryControl`] (consume `Box<Self>`) — the
//!   compiler enforces finalize-at-most-once
//! - [`stream::StreamBus`] over [`stream::StreamBackend`]; concrete backends
//!   live in sibling crates (`eventbus-memory`, `eventbus-redis`).
//!
//! See the [`eventbus`](https://docs.rs/eventbus) facade crate for ready-to-use
//! backends behind feature flags.

use std::{future::Future, pin::Pin};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub mod contract;
pub mod error;
pub mod eventbus;
pub mod serde_bytes;
pub mod stream;

pub use contract::delivery::{
    DeliveryInspector, DeliveryOutcome, DeliveryState, PartialDeliveryState,
};
pub use contract::message::{
    SchemaDescriptor, TraceContext, HEADER_BAGGAGE, HEADER_CONTENT_TYPE, HEADER_DEAD_LETTER_REASON,
    HEADER_EVENT_VERSION, HEADER_IDEMPOTENCY_KEY, HEADER_RETRY_ATTEMPT, HEADER_RETRY_REASON,
    HEADER_TRACE_PARENT, HEADER_TRACE_STATE,
};
pub use contract::{
    AckMode, BackpressurePolicy, ConsumerBalanceMode, DeliveryGuarantee, GuaranteeMatrix,
    OrderingMode, OverflowStrategy, PublishConfirmation, SubscriptionSemantics,
};
pub use error::EventBusError;
pub use eventbus::{
    BatchOutcome, Bus, Codec, ConsumerGroup, ConsumerName, Delivery, DeliveryControl,
    DeliveryHandle, Handler, Headers, Message, MessageId, PublishOptions, Publisher, PublisherExt,
    Subscriber, SubscriberExt, Subscription, SubscriptionConfig, SubscriptionConfigBuilder, Topic,
};
pub use stream::{ErrorObserver, ErrorScope, FetchedEntry, StreamBus, StreamBusOptions};
