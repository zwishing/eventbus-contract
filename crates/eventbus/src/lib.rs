//! `eventbus` — facade crate that re-exports the contract traits, value
//! types, backends, and helpers behind feature flags.
//!
//! Use [`prelude`] for the common imports, then pick a backend via features:
//!
//! - `memory` (default): in-process [`MemoryStreamBackend`](memory) for
//!   tests, examples, and local development.
//! - `redis`: production [`RedisBackend`](redis) over Redis Streams /
//!   consumer groups (requires the `redis` server).
//! - `outbox`: transactional [`OutboxStore`](outbox) + dispatcher relay.
//! - `integration`: DDD [`IntegrationEvent`](integration) helpers.
//! - `tracing`: enables `tracing` instrumentation on hot paths.
//!
//! Recommended setup:
//!
//! ```toml
//! [dependencies]
//! eventbus = { version = "0.2", features = ["redis", "outbox"] }
//! ```
//!
//! See [`MIGRATION-0.2.md`](https://github.com/zwishing/eventbus-contract/blob/main/MIGRATION-0.2.md)
//! for upgrading from `eventbus-contract` 0.1.

pub use eventbus_core as core;

#[cfg(feature = "memory")]
pub use eventbus_memory as memory;

#[cfg(feature = "redis")]
pub use eventbus_redis as redis;

#[cfg(feature = "outbox")]
pub use eventbus_outbox as outbox;

#[cfg(feature = "integration")]
pub use eventbus_integration as integration;

pub mod prelude {
    //! Common imports for users of the event bus.
    pub use eventbus_core::{
        AckMode, BackpressurePolicy, BatchOutcome, BoxFuture, BoxedError, Bus, Codec,
        ConsumerBalanceMode, ConsumerGroup, ConsumerName, Delivery, DeliveryControl,
        DeliveryGuarantee, DeliveryHandle, DeliveryState, EventBusError, Handler, Message,
        MessageId, OrderingMode, OverflowStrategy, PublishConfirmation, PublishOptions, Publisher,
        PublisherExt, Subscriber, SubscriberExt, Subscription, SubscriptionConfig,
        SubscriptionConfigBuilder, Topic,
    };
}
