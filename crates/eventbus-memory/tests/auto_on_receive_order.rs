//! Spec §6 #9 — `AckMode::AutoOnReceive` acks BEFORE the handler runs.
//!
//! In AutoOnReceive mode the bus pre-acks, then hands the delivery to the
//! handler. Even if the handler is still running (or panics), the message
//! is no longer in the consumer-group PEL. This test asserts the ordering
//! by observing `pending_count` while the handler is still asleep.

use std::sync::Arc;
use std::time::Duration;

use eventbus_core::stream::{StreamBus, StreamBusOptions};
use eventbus_core::{
    AckMode, BoxFuture, ConsumerGroup, ConsumerName, DeliveryHandle, EventBusError, Handler,
    Headers, Message, PublishOptions, SubscriptionConfig, Topic,
};
use eventbus_memory::MemoryStreamBackend;
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};

struct GatedHandler {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

impl Handler for GatedHandler {
    fn handle(
        &self,
        _delivery: Box<dyn DeliveryHandle>,
    ) -> BoxFuture<'_, Result<(), EventBusError>> {
        let started = self.started.clone();
        let release = self.release.clone();
        Box::pin(async move {
            started.notify_one();
            release.notified().await;
            Ok(())
        })
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
async fn auto_on_receive_acks_before_handler_completes() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend.clone(), StreamBusOptions::default()).expect("bus");

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());

    let cfg = SubscriptionConfig::builder(
        Topic::new("evt.auto-receive").expect("topic"),
        ConsumerGroup::new("cg.auto-receive").expect("group"),
    )
    .consumer_name(ConsumerName::new("worker-1").expect("consumer name"))
    .ack_mode(AckMode::AutoOnReceive)
    .max_in_flight(1)
    .build()
    .expect("build subscription config");

    let sub = bus
        .subscribe(
            cfg,
            GatedHandler {
                started: started.clone(),
                release: release.clone(),
            },
        )
        .await
        .expect("subscribe");

    bus.publish(message("evt.auto-receive", "uid-1"), PublishOptions::new())
        .await
        .expect("publish");

    // Wait for the handler to enter (proves delivery happened).
    timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("handler should start");

    // Handler is parked; the pre-ack must already have run. Poll briefly to
    // tolerate the small async gap between pre_ack and handler start.
    let mut acked = false;
    for _ in 0..50 {
        if backend
            .pending_count("evt.auto-receive", "cg.auto-receive")
            .await
            == 0
        {
            acked = true;
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert!(
        acked,
        "AutoOnReceive must ack before the handler completes — pending_count never dropped to 0"
    );

    // Now release the handler and clean up.
    release.notify_one();
    sub.close().await.expect("close");
}
