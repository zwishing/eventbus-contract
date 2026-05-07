//! Type-safety tests for the PR 3 Delivery / DeliveryControl split.
//!
//! These tests verify the runtime consequences of moving from a runtime
//! `AtomicBool finalized` guard (0.1) to the consume-self `Box<Self>` design
//! (0.2): finalization happens at most once *because the type system says
//! so*, not because a flag intercepts repeated calls.
//!
//! The compile-time half — "calling `delivery.ack()` twice does not compile"
//! — is enforced by the borrow checker on every build that includes this
//! crate. Demonstrating that with a `trybuild` test costs more than it adds:
//! the consume-self signature is right there in `DeliveryControl`. The
//! sentinel below documents the guarantee for future readers.
//!
//! ```ignore
//! // does NOT compile — `delivery` is moved by the first ack() call:
//! let delivery: Box<dyn DeliveryHandle> = /* ... */;
//! delivery.ack().await?;
//! delivery.nack(reason).await?; // E0382: use of moved value
//! ```

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use eventbus_core::stream::{MemoryStreamBackend, StreamBus, StreamBusOptions};
use eventbus_core::{
    AckMode, DeliveryHandle, EventBusError, Handler, Headers, Message, PublishOptions,
    SubscriptionConfig,
};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

fn message(topic: &str, uid: &str) -> Message {
    Message {
        uid: uid.to_string(),
        topic: topic.to_string(),
        key: String::new(),
        kind: "test.message".to_string(),
        source: "test".to_string(),
        occurred_at: Utc::now(),
        headers: Headers::new(),
        payload: bytes::Bytes::from_static(br#"{"ok":true}"#),
        content_type: None,
        event_version: None,
        idempotency_key: None,
        expires_at: None,
        trace_uid: None,
        correlation_uid: None,
    }
}

/// First delivery: drop the box without finalizing. The permit attached to
/// the box drops with it, freeing the limiter slot.
/// Second delivery: ack and signal. Reaching this proves the first
/// delivery released its permit — under `max_in_flight = 1` the second
/// message can only be read after the first slot is returned.
struct DropFirstThenAckHandler {
    seen_first: tokio::sync::Mutex<bool>,
    second_tx: mpsc::Sender<String>,
}

impl Handler for DropFirstThenAckHandler {
    fn handle(
        &self,
        delivery: Box<dyn DeliveryHandle>,
    ) -> eventbus_core::BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move {
            let uid = delivery.message().uid.clone();
            let mut seen = self.seen_first.lock().await;
            if !*seen {
                *seen = true;
                // Drop the box without finalizing — permit drops with it.
                drop(delivery);
                Ok(())
            } else {
                let _ = self.second_tx.send(uid).await;
                delivery.ack().await
            }
        })
    }
}

/// Calls `ack()` exactly once. The Box is consumed; any subsequent attempt
/// would fail to compile (the sentinel doctest at the top of this file
/// documents this).
struct AckOnceHandler {
    tx: mpsc::Sender<()>,
}

impl Handler for AckOnceHandler {
    fn handle(
        &self,
        delivery: Box<dyn DeliveryHandle>,
    ) -> eventbus_core::BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move {
            delivery.ack().await?;
            self.tx
                .send(())
                .await
                .map_err(|err| EventBusError::Internal(err.to_string()))
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_box_without_finalize_releases_permit() {
    // max_in_flight = 1: the second message can only be delivered if the
    // first's permit was released. Dropping the Box (no ack/nack/retry)
    // is the only thing that does that here.
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend.clone(), StreamBusOptions::default()).expect("construct bus");

    let (second_tx, mut second_rx) = mpsc::channel(1);
    let handler = DropFirstThenAckHandler {
        seen_first: tokio::sync::Mutex::new(false),
        second_tx,
    };

    let sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "evt.drop".to_string(),
                consumer_group: "cg.drop".to_string(),
                consumer_name: "consumer-drop".to_string(),
                ack_mode: AckMode::Manual,
                max_in_flight: 1,
                ..Default::default()
            },
            handler,
        )
        .await
        .expect("subscribe");

    bus.publish(message("evt.drop", "uid-A"), PublishOptions::default())
        .await
        .expect("publish A");
    // Tiny pause so the consume loop reads + dispatches A before B lands.
    sleep(Duration::from_millis(20)).await;
    bus.publish(message("evt.drop", "uid-B"), PublishOptions::default())
        .await
        .expect("publish B");

    // If the permit was not released by the dropped box, this would block
    // until the test timeout because max_in_flight = 1.
    let observed = timeout(Duration::from_secs(2), second_rx.recv())
        .await
        .expect("second delivery within timeout")
        .expect("second signal");
    assert_eq!(observed, "uid-B", "second delivery should be B");

    sub.close().await.expect("close sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ack_consumes_box_and_clears_pending() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend.clone(), StreamBusOptions::default()).expect("construct bus");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "evt.ack".to_string(),
                consumer_group: "cg.ack".to_string(),
                ack_mode: AckMode::Manual,
                max_in_flight: 1,
                ..Default::default()
            },
            AckOnceHandler { tx },
        )
        .await
        .expect("subscribe");

    bus.publish(message("evt.ack", "uid-ack"), PublishOptions::default())
        .await
        .expect("publish");

    timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("handler signalled within timeout")
        .expect("signal");

    sleep(Duration::from_millis(50)).await;
    assert_eq!(backend.pending_count("evt.ack", "cg.ack").await, 0);

    sub.close().await.expect("close sub");
}
