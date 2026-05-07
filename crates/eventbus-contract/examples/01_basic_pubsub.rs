//! # Basic Publish / Subscribe
//!
//! Demonstrates the simplest end-to-end flow:
//!
//! - Create an in-process `MemoryStreamBackend` (no external services needed)
//! - Subscribe with `AckMode::AutoOnHandlerSuccess` so the bus acks on handler success
//! - Publish a message and wait to receive it
//!
//! Run with:
//! ```text
//! cargo run --example 01_basic_pubsub
//! ```

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use eventbus_core::stream::{StreamBus, StreamBusOptions};
use eventbus_core::{
    AckMode, DeliveryHandle, EventBusError, Handler, Headers, Message, PublishOptions,
};
use eventbus_memory::MemoryStreamBackend;
use tokio::sync::mpsc;
use tokio::time::timeout;

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

struct PrintHandler {
    tx: mpsc::Sender<Message>,
}

impl Handler for PrintHandler {
    fn handle(
        &self,
        delivery: Box<dyn DeliveryHandle>,
    ) -> eventbus_core::BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move {
            let msg = delivery.message();
            println!(
                "[handler] received topic={} uid={} kind={}",
                msg.topic, msg.uid, msg.kind
            );
            self.tx
                .send(msg.clone())
                .await
                .map_err(|e| EventBusError::Internal(e.to_string()))
        })
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend, StreamBusOptions::default())?;

    // Subscribe first — the consumer group must exist before publish.
    let (tx, mut rx) = mpsc::channel(8);
    let sub = bus
        .subscribe(
            eventbus_core::SubscriptionConfig::builder(
                eventbus_core::Topic::new("user.registered").expect("topic"),
                eventbus_core::ConsumerGroup::new("notification-service").expect("group"),
            )
            .consumer_name(eventbus_core::ConsumerName::new("worker-1").expect("consumer name"))
            .ack_mode(AckMode::AutoOnHandlerSuccess)
            .max_in_flight(1)
            .build()
            .expect("build subscription config"),
            PrintHandler { tx },
        )
        .await?;

    println!("[main] subscribed, publishing message…");

    bus.publish(
        Message {
            uid: "evt-001".to_string(),
            topic: eventbus_core::Topic::new("user.registered").expect("topic"),
            key: "user-42".to_string(),
            kind: "UserRegistered".to_string(),
            source: "auth-service".to_string(),
            occurred_at: Utc::now(),
            headers: Headers::new(),
            payload: bytes::Bytes::from_static(br#"{"user_id": 42, "email": "alice@example.com"}"#),
            content_type: Some("application/json".to_string()),
            event_version: Some("1.0".to_string()),
            idempotency_key: Some("reg-user-42".to_string()),
            expires_at: None,
            trace_uid: None,
            correlation_uid: None,
        },
        PublishOptions::new().with_metadata("source-region", "us-east-1"),
    )
    .await?;

    // Wait up to 3 seconds for the message to arrive.
    let received = timeout(Duration::from_secs(3), rx.recv())
        .await?
        .expect("channel closed");

    println!("[main] delivered: uid={}", received.uid);
    assert_eq!(received.uid, "evt-001");

    sub.close().await?;
    println!("[main] done");
    Ok(())
}
