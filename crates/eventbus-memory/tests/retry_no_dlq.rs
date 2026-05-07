//! Spec §6 #5 — retry exhausted with NO `dead_letter_topic` configured.
//!
//! When `max_retry = 0` and the handler returns `Err`, the bus calls
//! `delivery.retry(reason)` (in `AutoOnHandlerSuccess` mode). With no DLQ
//! the retry path raises `EventBusError::Validation("retry exhausted ...")`,
//! which the consume loop captures as the first delivery error and surfaces
//! when `sub.close().await` returns.

use std::sync::Arc;
use std::time::Duration;

use eventbus_core::stream::{StreamBus, StreamBusOptions};
use eventbus_core::{
    AckMode, BoxFuture, ConsumerGroup, ConsumerName, DeliveryHandle, EventBusError, Handler,
    Headers, Message, PublishOptions, SubscriptionConfig, Topic,
};
use eventbus_memory::MemoryStreamBackend;
use tokio::time::sleep;

struct AlwaysFailHandler;

impl Handler for AlwaysFailHandler {
    fn handle(
        &self,
        _delivery: Box<dyn DeliveryHandle>,
    ) -> BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move { Err(EventBusError::Internal("handler failed".into())) })
    }
}

fn message(topic: &str, uid: &str) -> Message {
    Message {
        uid: uid.into(),
        topic: Topic::new(topic).expect("topic"),
        key: String::new(),
        kind: "test.message".into(),
        source: "test".into(),
        occurred_at: chrono::Utc::now(),
        headers: Headers::new(),
        payload: bytes::Bytes::from_static(b"{}"),
        content_type: None,
        event_version: None,
        idempotency_key: None,
        expires_at: None,
        trace_uid: None,
        correlation_uid: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_exhausted_without_dlq_surfaces_validation_error() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend, StreamBusOptions::default()).expect("bus");

    let cfg = SubscriptionConfig::builder(
        Topic::new("evt.retry-no-dlq").expect("topic"),
        ConsumerGroup::new("cg.retry-no-dlq").expect("group"),
    )
    .consumer_name(ConsumerName::new("worker-1").expect("consumer name"))
    .ack_mode(AckMode::AutoOnHandlerSuccess)
    .max_retry(0)
    .max_in_flight(1)
    .build()
    .expect("build subscription config");

    let sub = bus
        .subscribe(cfg, AlwaysFailHandler)
        .await
        .expect("subscribe");

    bus.publish(message("evt.retry-no-dlq", "uid-1"), PublishOptions::new())
        .await
        .expect("publish");

    // Give the consume loop a chance to deliver, fail, and capture the error.
    sleep(Duration::from_millis(200)).await;

    let err = sub
        .close()
        .await
        .expect_err("close should surface retry-exhausted Validation error");

    match err {
        EventBusError::Validation(msg) => {
            assert!(
                msg.contains("retry exhausted"),
                "expected 'retry exhausted' in message, got: {msg}"
            );
        }
        other => panic!("expected Validation error, got: {other:?}"),
    }
}
