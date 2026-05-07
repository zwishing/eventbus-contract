//! Spec §6 #15 — `publish_batch` honours `publish_batch_parallelism`.
//!
//! Wraps the in-memory backend with a counter that records the maximum
//! number of concurrent in-flight `publish` calls and asserts that this
//! peak never exceeds the configured `publish_batch_parallelism`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use eventbus_core::stream::{FetchedEntry, StreamBackend, StreamBus, StreamBusOptions};
use eventbus_core::{EventBusError, Headers, Message, PublishOptions, Topic};
use eventbus_memory::MemoryStreamBackend;
use tokio::time::sleep;

struct GatedBackend {
    inner: Arc<MemoryStreamBackend>,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    hold: Duration,
}

impl GatedBackend {
    fn new(inner: Arc<MemoryStreamBackend>, hold: Duration) -> Self {
        Self {
            inner,
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            hold,
        }
    }

    fn observed_max(&self) -> usize {
        self.max_in_flight.load(Ordering::SeqCst)
    }
}

impl StreamBackend for GatedBackend {
    async fn create_group(
        &self,
        stream: &str,
        group: &str,
        start_id: &str,
    ) -> Result<(), EventBusError> {
        self.inner.create_group(stream, group, start_id).await
    }

    async fn publish(&self, stream: &str, message: Message) -> Result<String, EventBusError> {
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(now, Ordering::SeqCst);
        // Hold long enough that other tasks can pile up if the bus does not
        // gate them.
        sleep(self.hold).await;
        let res = self.inner.publish(stream, message).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        res
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
        self.inner
            .read_new(stream, group, consumer, count, timeout)
            .await
    }

    async fn ack(&self, stream: &str, group: &str, message_id: &str) -> Result<(), EventBusError> {
        self.inner.ack(stream, group, message_id).await
    }
}

fn message(uid: &str) -> Message {
    Message {
        uid: uid.into(),
        topic: Topic::new("evt.parallel").expect("topic"),
        key: String::new(),
        kind: "test".into(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_batch_respects_parallelism_cap() {
    let inner = Arc::new(MemoryStreamBackend::default());
    let backend = Arc::new(GatedBackend::new(inner, Duration::from_millis(50)));

    let parallelism = 2;
    let opts = StreamBusOptions::default().with_publish_batch_parallelism(parallelism);
    let bus = StreamBus::new(backend.clone(), opts).expect("bus");

    let msgs: Vec<Message> = (0..10).map(|i| message(&format!("uid-{i}"))).collect();
    let outcome = bus
        .publish_batch(msgs, PublishOptions::new())
        .await
        .expect("publish_batch");

    assert_eq!(outcome.results.len(), 10);
    for r in &outcome.results {
        assert!(r.is_ok(), "publish should succeed: {r:?}");
    }

    let observed = backend.observed_max();
    assert!(
        observed <= parallelism,
        "max observed in-flight publishes ({observed}) exceeded cap ({parallelism})"
    );
    assert!(
        observed >= 1,
        "max observed in-flight publishes should be >= 1, got {observed}"
    );
}
