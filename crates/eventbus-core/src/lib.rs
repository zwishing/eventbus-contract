#![allow(async_fn_in_trait)]

use std::{future::Future, pin::Pin};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub mod codec;
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
pub mod serde_bytes;
pub mod stream;

pub use consumer::ConsumerMessageRecord;
pub use contract::{
    AckMode, BackpressurePolicy, ConsumerBalanceMode, DeliveryGuarantee, GuaranteeMatrix,
    OrderingMode, OverflowStrategy, PublishConfirmation, SubscriptionSemantics,
};
pub use delivery_contract::{
    DeliveryInspector, DeliveryOutcome, DeliveryState, PartialDeliveryState,
};
pub use dispatcher::{Config, Dispatcher, DispatcherConfig, Listener, Notification, Notifier};
pub use error::EventBusError;
pub use eventbus::{
    BatchOutcome, Bus, Codec, Delivery, Handler, Headers, Message, MessageId, PublishOptions,
    Publisher, PublisherExt, Subscriber, Subscription, SubscriptionConfig, Topic,
};
pub use idempotency::{IdempotencyClaim, IdempotencyClaimStore, IdempotencyStore};
pub use integration::{EventPublisher, IntegrationEvent, MessageFactory};
pub use message_contract::{
    SchemaDescriptor, TraceContext, HEADER_BAGGAGE, HEADER_CONTENT_TYPE, HEADER_DEAD_LETTER_REASON,
    HEADER_EVENT_VERSION, HEADER_IDEMPOTENCY_KEY, HEADER_RETRY_ATTEMPT, HEADER_RETRY_REASON,
    HEADER_TRACE_PARENT, HEADER_TRACE_STATE,
};
pub use outbox::dead_letter::{DeadLetterDecision, DeadLetterPolicy, DeadLetterReason};
pub use outbox::{
    AppendRequest, DeadLetterMessageRecord, OutboxMessageRecord, OutboxRecord, OutboxStatus,
    OutboxStore,
};
#[cfg(feature = "redis-backend")]
pub use stream::RedisBackend;
pub use stream::{
    ErrorObserver, ErrorScope, FetchedEntry, MemoryStreamBackend, StreamBus, StreamBusOptions,
};
