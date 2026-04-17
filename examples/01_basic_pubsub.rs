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
use eventbus_contract::stream::{MemoryStreamBackend, StreamBus, StreamBusOptions};
use eventbus_contract::{
    AckMode, Delivery, EventBusError, Handler, Headers, Message, PublishOptions, SubscriptionConfig,
};
use tokio::sync::mpsc;
use tokio::time::timeout;

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

struct PrintHandler {
    tx: mpsc::Sender<Message>,
}

impl Handler for PrintHandler {
    async fn handle<D>(&self, delivery: &D) -> Result<(), EventBusError>
    where
        D: Delivery + Send + Sync,
    {
        let msg = delivery.message();
        println!(
            "[handler] received topic={} uid={} kind={}",
            msg.topic, msg.uid, msg.kind
        );
        self.tx
            .send(msg.clone())
            .await
            .map_err(|e| EventBusError::Internal(e.to_string()))
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
            SubscriptionConfig {
                topic: "user.registered".to_string(),
                consumer_group: "notification-service".to_string(),
                consumer_name: "worker-1".to_string(),
                ack_mode: AckMode::AutoOnHandlerSuccess,
                concurrency: 1,
                ..Default::default()
            },
            PrintHandler { tx },
        )
        .await?;

    println!("[main] subscribed, publishing message…");

    bus.publish(
        Message {
            uid: "evt-001".to_string(),
            topic: "user.registered".to_string(),
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
