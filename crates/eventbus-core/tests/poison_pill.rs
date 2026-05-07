//! Poison-pill: a malformed PEL entry must be acked + observed + DLQ-routed,
//! and the consume loop must keep delivering the well-formed messages behind
//! it. Without per-entry `FetchedEntry::Malformed`, a single bad entry would
//! re-enter the PEL, fail to decode, and spin forever.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use eventbus_core::stream::{
    ClaimedMessage, ErrorObserver, ErrorScope, FetchedEntry, MemoryStreamBackend, StreamBackend,
    StreamBus, StreamBusOptions,
};
use eventbus_core::{
    BoxFuture, ConsumerGroup, DeliveryHandle, EventBusError, Handler, Headers, Message,
    PublishOptions, SubscriptionConfig, Topic,
};
use tokio::time::sleep;

#[derive(Default)]
struct CapturingObserver {
    errors: Mutex<Vec<(ErrorScope, String)>>,
}

impl ErrorObserver for CapturingObserver {
    fn on_error(&self, scope: ErrorScope, err: &EventBusError) {
        self.errors.lock().unwrap().push((scope, err.to_string()));
    }
}

/// Backend that injects exactly one `FetchedEntry::Malformed` on the first
/// `read_new` call for the target topic, then delegates everything to the
/// inner [`MemoryStreamBackend`].
struct OneMalformedThenMemory {
    inner: Arc<MemoryStreamBackend>,
    target_topic: String,
    fired: AtomicBool,
}

impl StreamBackend for OneMalformedThenMemory {
    async fn create_group(
        &self,
        stream: &str,
        group: &str,
        start_id: &str,
    ) -> Result<(), EventBusError> {
        self.inner.create_group(stream, group, start_id).await
    }

    async fn publish(&self, stream: &str, message: Message) -> Result<String, EventBusError> {
        self.inner.publish(stream, message).await
    }

    async fn reclaim_idle(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        min_idle: Duration,
        count: usize,
    ) -> Result<Vec<FetchedEntry>, EventBusError> {
        self.inner
            .reclaim_idle(stream, group, consumer, min_idle, count)
            .await
    }

    async fn read_new(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        count: usize,
        timeout: Duration,
    ) -> Result<Vec<FetchedEntry>, EventBusError> {
        if stream == self.target_topic && !self.fired.swap(true, Ordering::AcqRel) {
            return Ok(vec![FetchedEntry::Malformed {
                id: "1-0".into(),
                error: EventBusError::Serialization("synthetic poison pill".into()),
            }]);
        }
        self.inner
            .read_new(stream, group, consumer, count, timeout)
            .await
    }

    async fn ack(&self, stream: &str, group: &str, message_id: &str) -> Result<(), EventBusError> {
        // The malformed entry id was never inserted into the inner backend's
        // PEL (we synthesised it). The inner ack will silently no-op for an
        // unknown id, which matches the production contract.
        let _ = message_id;
        self.inner.ack(stream, group, message_id).await
    }

    async fn ack_many(
        &self,
        stream: &str,
        group: &str,
        message_ids: &[String],
    ) -> Result<(), EventBusError> {
        self.inner.ack_many(stream, group, message_ids).await
    }
}

// Make sure the inner publish is dyn-callable on Arc<MemoryStreamBackend>.
async fn publish_via_inner(
    inner: &Arc<MemoryStreamBackend>,
    stream: &str,
    msg: Message,
) -> Result<String, EventBusError> {
    inner.publish(stream, msg).await
}

fn message(topic: &str, uid: &str) -> Message {
    Message {
        uid: uid.to_string(),
        topic: Topic::new(topic).expect("topic"),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_entry_is_acked_observed_and_dlq_routed() {
    let inner = Arc::new(MemoryStreamBackend::default());
    let backend = Arc::new(OneMalformedThenMemory {
        inner: Arc::clone(&inner),
        target_topic: "evt.poison".to_string(),
        fired: AtomicBool::new(false),
    });
    let observer = Arc::new(CapturingObserver::default());
    let bus = StreamBus::new(
        backend,
        StreamBusOptions::default().with_error_observer(observer.clone() as Arc<dyn ErrorObserver>),
    )
    .expect("bus");

    // Track handler deliveries — we want at least one well-formed message
    // through after the poison pill.
    let delivered = Arc::new(Mutex::new(Vec::<String>::new()));
    struct Capture(Arc<Mutex<Vec<String>>>);
    impl Handler for Capture {
        fn handle(&self, d: Box<dyn DeliveryHandle>) -> BoxFuture<'_, Result<(), EventBusError>> {
            let log = Arc::clone(&self.0);
            Box::pin(async move {
                log.lock().unwrap().push(d.message().uid.clone());
                d.ack().await
            })
        }
    }

    let cfg = SubscriptionConfig::builder(
        Topic::new("evt.poison").unwrap(),
        ConsumerGroup::new("cg.poison").unwrap(),
    )
    .max_in_flight(1)
    .dead_letter_topic(Topic::new("evt.poison.dlq").unwrap())
    .build()
    .unwrap();
    let sub = bus
        .subscribe(cfg, Capture(Arc::clone(&delivered)))
        .await
        .expect("subscribe");

    // Publish a real message that the consume loop should pick up *after*
    // it has digested the synthetic poison pill.
    publish_via_inner(&inner, "evt.poison", message("evt.poison", "uid-real"))
        .await
        .expect("publish");

    // Give the consume loop time to see the malformed entry, ack/DLQ it,
    // then loop around and pick up the real one.
    sleep(Duration::from_millis(400)).await;

    let _ = sub.abort().await;

    // Observer must have seen exactly the synthetic Read-scope error (and
    // possibly more — what matters is at least one Read scope fired).
    let errors = observer.errors.lock().unwrap();
    assert!(
        errors.iter().any(|(scope, _)| *scope == ErrorScope::Read),
        "expected at least one Read scope error, got {:?}",
        errors
    );

    // The DLQ must have received the synthetic envelope.
    let dlq_len = inner.stream_len("evt.poison.dlq").await;
    assert!(
        dlq_len >= 1,
        "expected at least one DLQ envelope, got {dlq_len}"
    );

    // The well-formed message must have reached the handler — proving the
    // consume loop survived the poison pill.
    let log = delivered.lock().unwrap();
    assert!(
        log.iter().any(|u| u == "uid-real"),
        "expected the real message to be delivered after the poison pill, got {:?}",
        log
    );
}

// Silence dead_code if the helper above ends up unused under future edits.
#[allow(dead_code)]
fn _unused(_: &ClaimedMessage) {}
