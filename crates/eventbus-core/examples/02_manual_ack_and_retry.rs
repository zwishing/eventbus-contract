//! # Manual Ack and Retry
//!
//! Demonstrates explicit delivery control:
//!
//! - `delivery.ack()` — message processed successfully, remove from pending
//! - `delivery.retry(reason)` — transient failure, republish for redelivery
//! - `delivery.nack(reason)` — permanent failure, route to dead-letter topic
//!
//! A handler that fails on the first attempt and succeeds on the second.
//! After `max_retry` exhaustion, the message goes to the dead-letter stream.
//!
//! Run with:
//! ```text
//! cargo run --example 02_manual_ack_and_retry
//! ```

use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use std::time::Duration;

use chrono::Utc;
use eventbus_core::stream::{MemoryStreamBackend, StreamBus, StreamBusOptions};
use eventbus_core::{
    AckMode, DeliveryHandle, EventBusError, Handler, Headers, Message, PublishOptions,
};
use tokio::sync::mpsc;
use tokio::time::timeout;

// ---------------------------------------------------------------------------
// Handler: succeeds on attempt 2, sends attempt number to the channel
// ---------------------------------------------------------------------------

struct RetryingHandler {
    attempts: Arc<AtomicU32>,
    tx: mpsc::Sender<u32>,
}

impl Handler for RetryingHandler {
    fn handle(
        &self,
        delivery: Box<dyn DeliveryHandle>,
    ) -> eventbus_core::BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            let state = delivery.state().await?;

            println!(
                "[handler] attempt={} (delivery state attempt={})",
                attempt, state.attempt
            );

            if attempt == 1 {
                // Transient failure — ask the bus to republish for redelivery.
                delivery
                    .retry(Box::new(std::io::Error::other(
                        "temporary downstream error",
                    )))
                    .await?;
                println!("[handler] → retried");
            } else {
                delivery.ack().await?;
                println!("[handler] → acked on attempt {attempt}");
            }

            self.tx
                .send(attempt)
                .await
                .map_err(|e| EventBusError::Internal(e.to_string()))
        })
    }
}

// ---------------------------------------------------------------------------
// Handler: always fails → exhausts retries → dead letter
// ---------------------------------------------------------------------------

struct AlwaysFailHandler {
    tx: mpsc::Sender<()>,
}

impl Handler for AlwaysFailHandler {
    fn handle(
        &self,
        delivery: Box<dyn DeliveryHandle>,
    ) -> eventbus_core::BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move {
            println!("[dlq-handler] message is poison — routing to dead letter");
            delivery
                .nack(Box::new(std::io::Error::other("unrecoverable parse error")))
                .await?;
            self.tx
                .send(())
                .await
                .map_err(|e| EventBusError::Internal(e.to_string()))
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn sample_message(topic: &str, uid: &str) -> Message {
    Message {
        uid: uid.to_string(),
        topic: eventbus_core::Topic::new(topic).expect("topic"),
        key: "order-99".to_string(),
        kind: "OrderPlaced".to_string(),
        source: "order-service".to_string(),
        occurred_at: Utc::now(),
        headers: Headers::new(),
        payload: bytes::Bytes::from_static(br#"{"order_id": 99}"#),
        content_type: Some("application/json".to_string()),
        event_version: None,
        idempotency_key: None,
        expires_at: None,
        trace_uid: None,
        correlation_uid: None,
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(MemoryStreamBackend::default());

    // -----------------------------------------------------------------------
    // Part 1: retry succeeds on second attempt
    // -----------------------------------------------------------------------
    println!("=== Part 1: retry then succeed ===");
    {
        let bus = StreamBus::new(Arc::clone(&backend), StreamBusOptions::default())?;
        let attempts = Arc::new(AtomicU32::new(0));
        let (tx, mut rx) = mpsc::channel(4);

        let sub = bus
            .subscribe(
                eventbus_core::SubscriptionConfig::builder(
                    eventbus_core::Topic::new("order.placed").expect("topic"),
                    eventbus_core::ConsumerGroup::new("fulfillment").expect("group"),
                )
                .consumer_name(eventbus_core::ConsumerName::new("worker-1").expect("consumer name"))
                .ack_mode(AckMode::Manual)
                .max_retry(3)
                .max_in_flight(1)
                .build()
                .expect("build subscription config"),
                RetryingHandler {
                    attempts: Arc::clone(&attempts),
                    tx,
                },
            )
            .await?;

        bus.publish(
            sample_message("order.placed", "evt-retry-1"),
            PublishOptions::default(),
        )
        .await?;

        // Collect both signals: attempt 1 (retry) and attempt 2 (ack)
        timeout(Duration::from_secs(3), rx.recv())
            .await?
            .expect("channel closed");
        timeout(Duration::from_secs(3), rx.recv())
            .await?
            .expect("channel closed");

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            backend.pending_count("order.placed", "fulfillment").await,
            0
        );
        println!("[main] pending=0 ✓");
        sub.close().await?;
    }

    // -----------------------------------------------------------------------
    // Part 2: nack routes to dead-letter topic immediately
    // -----------------------------------------------------------------------
    println!("\n=== Part 2: nack → dead letter ===");
    {
        let bus = StreamBus::new(Arc::clone(&backend), StreamBusOptions::default())?;
        let (tx, mut rx) = mpsc::channel(1);

        let sub = bus
            .subscribe(
                eventbus_core::SubscriptionConfig::builder(
                    eventbus_core::Topic::new("payment.event").expect("topic"),
                    eventbus_core::ConsumerGroup::new("ledger").expect("group"),
                )
                .consumer_name(eventbus_core::ConsumerName::new("worker-1").expect("consumer name"))
                .ack_mode(AckMode::Manual)
                .dead_letter_topic(
                    eventbus_core::Topic::new("payment.event.dlq").expect("dlq topic"),
                )
                .max_in_flight(1)
                .build()
                .expect("build subscription config"),
                AlwaysFailHandler { tx },
            )
            .await?;

        bus.publish(
            sample_message("payment.event", "evt-nack-1"),
            PublishOptions::default(),
        )
        .await?;

        timeout(Duration::from_secs(3), rx.recv())
            .await?
            .expect("channel closed");

        assert_eq!(backend.stream_len("payment.event.dlq").await, 1);
        println!("[main] dead-letter stream length=1 ✓");
        sub.close().await?;
    }

    println!("\n[main] done");
    Ok(())
}
