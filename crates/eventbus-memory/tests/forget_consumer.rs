//! Spec §6 #11 — `StreamBus::close()` invokes `StreamBackend::forget_consumer`
//! exactly once with the (stream, group, consumer) tuple.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use eventbus_core::stream::{FetchedEntry, StreamBackend, StreamBus, StreamBusOptions};
use eventbus_core::{
    BoxFuture, ConsumerGroup, ConsumerName, DeliveryHandle, EventBusError, Handler, Message,
    SubscriptionConfig, Topic,
};
use eventbus_memory::MemoryStreamBackend;

struct CountingBackend {
    inner: Arc<MemoryStreamBackend>,
    forget_calls: Arc<Mutex<Vec<(String, String, String)>>>,
}

impl StreamBackend for CountingBackend {
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
        self.inner
            .read_new(stream, group, consumer, count, timeout)
            .await
    }

    async fn ack(&self, stream: &str, group: &str, message_id: &str) -> Result<(), EventBusError> {
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

    async fn forget_consumer(&self, stream: &str, group: &str, consumer: &str) {
        self.forget_calls
            .lock()
            .unwrap()
            .push((stream.into(), group.into(), consumer.into()));
        self.inner.forget_consumer(stream, group, consumer).await;
    }
}

struct NoopHandler;

impl Handler for NoopHandler {
    fn handle(
        &self,
        _delivery: Box<dyn DeliveryHandle>,
    ) -> BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_invokes_forget_consumer_exactly_once() {
    let inner = Arc::new(MemoryStreamBackend::default());
    let forget_calls: Arc<Mutex<Vec<(String, String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let backend = Arc::new(CountingBackend {
        inner,
        forget_calls: forget_calls.clone(),
    });

    let bus = StreamBus::new(backend, StreamBusOptions::default()).expect("bus");

    let topic = Topic::new("evt.forget").expect("topic");
    let group = ConsumerGroup::new("cg.forget").expect("group");
    let consumer = ConsumerName::new("worker-1").expect("consumer");

    let cfg = SubscriptionConfig::builder(topic.clone(), group.clone())
        .consumer_name(consumer.clone())
        .max_in_flight(1)
        .build()
        .expect("build");

    let sub = bus.subscribe(cfg, NoopHandler).await.expect("subscribe");
    sub.close().await.expect("close");

    let calls = forget_calls.lock().unwrap();
    assert_eq!(
        calls.len(),
        1,
        "expected forget_consumer to be called exactly once, got {} calls",
        calls.len()
    );
    let (s, g, c) = &calls[0];
    assert_eq!(s, topic.as_str());
    assert_eq!(g, group.as_str());
    assert_eq!(c, consumer.as_str());
}
