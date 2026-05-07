//! Facade crate for the EventBus contract. Choose backends + helpers via features.
//!
//! - `memory` (default): in-process backend (test/dev only)
//! - `redis`: Redis Streams backend
//! - `outbox`: transactional outbox + dispatcher
//! - `integration`: DDD integration-event helpers
//! - `tracing`: tracing instrumentation

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
        Bus, Codec, ConsumerGroup, ConsumerName, Delivery, DeliveryGuarantee, EventBusError,
        Handler, Message, PublishOptions, Publisher, Subscriber, Subscription, SubscriptionConfig,
        SubscriptionConfigBuilder, Topic,
    };
}
