#![allow(async_fn_in_trait)]

pub mod consumer;
pub mod contract;
pub mod delivery_contract;
pub mod dispatcher;
pub mod error;
pub mod eventbus;
pub mod idempotency;
pub mod integration;
pub mod message_contract;
pub mod outbox;
pub mod redis_stream;
pub mod serde_bytes;

pub mod dead_letter {
    pub use crate::outbox::dead_letter::*;
}

pub use consumer::ConsumerMessageRecord;
pub use contract::{
    AckMode, BackpressurePolicy, ConsumerBalanceMode, DeliveryGuarantee, GuaranteeMatrix,
    OrderingMode, OverflowStrategy, PublishConfirmation, SubscriptionSemantics,
};
pub use dead_letter::{DeadLetterDecision, DeadLetterPolicy, DeadLetterReason};
pub use delivery_contract::{DeliveryInspector, DeliveryOutcome, DeliveryState};
pub use dispatcher::{Config, Dispatcher, DispatcherConfig, Listener, Notification, Notifier};
pub use error::EventBusError;
pub use eventbus::{
    Bus, Codec, Delivery, EventBus, Handler, Headers, Message, PublishOptions, Publisher,
    Subscriber, Subscription, SubscriptionConfig, Topic,
};
pub use idempotency::{IdempotencyClaim, IdempotencyClaimStore, IdempotencyStore};
pub use integration::{EventPublisher, IntegrationEvent, MessageFactory};
pub use message_contract::{
    SchemaDescriptor, TraceContext, HEADER_BAGGAGE, HEADER_CONTENT_TYPE, HEADER_EVENT_VERSION,
    HEADER_IDEMPOTENCY_KEY, HEADER_TRACE_PARENT, HEADER_TRACE_STATE,
};
pub use outbox::{
    AppendRequest, DeadLetterMessageRecord, OutboxMessageRecord, OutboxRecord, OutboxStatus,
    OutboxStore, Repository,
};
pub use redis_stream::{MemoryStreamBackend, RedisStreamBus, RedisStreamBusOptions};
#[cfg(feature = "redis-backend")]
pub use redis_stream::RedisBackend;
