use std::sync::Arc;
use std::time::Duration;

use eventbus_core::stream::{StreamBackend, StreamBus, StreamBusOptions};
use eventbus_core::{
    BoxFuture, ConsumerGroup, ConsumerName, DeliveryHandle, EventBusError, Handler, Headers,
    Message, PublishOptions, SubscriptionConfig, Topic,
};
use eventbus_memory::MemoryStreamBackend;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Instant};

fn message(topic: &str, uid: &str) -> Message {
    Message {
        uid: uid.into(),
        topic: Topic::new(topic).unwrap(),
        key: String::new(),
        kind: "regression".into(),
        source: "tests".into(),
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

struct Ack(mpsc::UnboundedSender<String>);

impl Handler for Ack {
    fn handle(
        &self,
        delivery: Box<dyn DeliveryHandle>,
    ) -> BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move {
            let uid = delivery.message().uid.clone();
            delivery.ack().await?;
            self.0.send(uid).unwrap();
            Ok(())
        })
    }
}

#[tokio::test]
async fn subscription_rejects_retry_delay_that_reclaim_would_interrupt() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let idle = Duration::from_millis(50);
    let bus = StreamBus::new(
        backend,
        StreamBusOptions::new().with_claim_idle_timeout(idle),
    )
    .unwrap();
    for delay in [idle, idle * 2] {
        let config = SubscriptionConfig::builder(
            Topic::new("invalid-delay").unwrap(),
            ConsumerGroup::new("group").unwrap(),
        )
        .max_retry(1)
        .retry_backoff(delay)
        .build()
        .unwrap();
        let (tx, _rx) = mpsc::unbounded_channel();
        match bus.subscribe(config, Ack(tx)).await {
            Err(EventBusError::Validation(_)) => {}
            Err(err) => panic!("wrong rejection: {err}"),
            Ok(sub) => {
                sub.close().await.unwrap();
                panic!("retry must finish before reclaim eligibility");
            }
        }
    }
}

#[tokio::test]
async fn reclaim_larger_than_capacity_drains_without_waiting_for_another_idle_timeout() {
    let backend = Arc::new(MemoryStreamBackend::default());
    backend.create_group("reclaim", "group", "0").await.unwrap();
    for uid in ["one", "two", "three"] {
        backend
            .publish("reclaim", message("reclaim", uid))
            .await
            .unwrap();
    }
    backend
        .read_new("reclaim", "group", "old", 3, Duration::ZERO)
        .await
        .unwrap();
    sleep(Duration::from_millis(510)).await;

    let bus = StreamBus::new(
        Arc::clone(&backend),
        StreamBusOptions::new()
            .with_block_timeout(Duration::from_secs(5))
            .with_claim_idle_timeout(Duration::from_millis(500))
            .with_reclaim_interval(Duration::from_millis(5))
            .with_claim_scan_batch_size(64),
    )
    .unwrap();
    let config = SubscriptionConfig::builder(
        Topic::new("reclaim").unwrap(),
        ConsumerGroup::new("group").unwrap(),
    )
    .consumer_name(ConsumerName::new("new").unwrap())
    .max_in_flight(1)
    .build()
    .unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let sub = bus.subscribe(config, Ack(tx)).await.unwrap();
    let received = timeout(Duration::from_millis(250), async {
        let mut ids = Vec::new();
        for _ in 0..3 {
            ids.push(rx.recv().await.unwrap());
        }
        ids.sort();
        ids
    })
    .await;
    sub.close().await.unwrap();
    assert_eq!(
        received.expect("all expired entries must drain"),
        ["one", "three", "two"]
    );
    assert_eq!(backend.pending_count("reclaim", "group").await, 0);
}

#[tokio::test]
async fn consumer_can_reclaim_its_own_abandoned_delivery() {
    let backend = MemoryStreamBackend::default();
    backend.create_group("own", "group", "0").await.unwrap();
    let id = backend.publish("own", message("own", "one")).await.unwrap();
    backend
        .read_new("own", "group", "worker", 1, Duration::ZERO)
        .await
        .unwrap();
    let reclaimed = backend
        .reclaim_idle("own", "group", "worker", Duration::ZERO, 1)
        .await
        .unwrap();
    assert_eq!(reclaimed.len(), 1);
    let eventbus_core::stream::FetchedEntry::Decoded(entry) = &reclaimed[0] else {
        panic!("valid message")
    };
    assert_eq!(entry.id, id);
    assert_eq!(entry.state.attempt, 2);
    assert!(entry.state.redelivered);
}

struct RetryOnce(mpsc::UnboundedSender<Instant>);

impl Handler for RetryOnce {
    fn handle(
        &self,
        delivery: Box<dyn DeliveryHandle>,
    ) -> BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move {
            self.0.send(Instant::now()).unwrap();
            if delivery.state().await?.attempt == 1 {
                delivery
                    .retry(Box::new(std::io::Error::other("temporary failure")))
                    .await
            } else {
                delivery.ack().await
            }
        })
    }
}

#[tokio::test]
async fn retry_waits_for_configured_backoff_before_republishing() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(Arc::clone(&backend), StreamBusOptions::default()).unwrap();
    let backoff = Duration::from_millis(80);
    let config = SubscriptionConfig::builder(
        Topic::new("retry").unwrap(),
        ConsumerGroup::new("group").unwrap(),
    )
    .max_retry(1)
    .retry_backoff(backoff)
    .build()
    .unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let sub = bus.subscribe(config, RetryOnce(tx)).await.unwrap();
    bus.publish(message("retry", "one"), PublishOptions::default())
        .await
        .unwrap();
    let first = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let second = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    sub.close().await.unwrap();
    assert!(
        second.duration_since(first) >= backoff,
        "retry ran before the configured delay"
    );
    assert_eq!(backend.pending_count("retry", "group").await, 0);
}
