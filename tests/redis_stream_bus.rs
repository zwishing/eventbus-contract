use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use chrono::Utc;
use eventbus_contract::{
    AckMode, Delivery, EventBusError, Handler, Headers, Message, PublishOptions,
    SubscriptionConfig,
};
use eventbus_contract::redis_stream::{MemoryStreamBackend, RedisStreamBus, RedisStreamBusOptions};
use tokio::sync::{mpsc, Mutex};
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
        payload: br#"{"ok":true}"#.to_vec(),
        content_type: None,
        event_version: None,
        idempotency_key: None,
        expires_at: None,
        trace_uid: None,
        correlation_uid: None,
    }
}

struct AutoAckHandler {
    tx: mpsc::Sender<Message>,
}

impl Handler for AutoAckHandler {
    async fn handle<D>(&self, delivery: &D) -> Result<(), EventBusError>
    where
        D: Delivery + Send + Sync,
    {
        self.tx
            .send(delivery.message().clone())
            .await
            .map_err(|err| EventBusError::Internal(err.to_string()))
    }
}

struct ManualAckHandler {
    tx: mpsc::Sender<()>,
}

impl Handler for ManualAckHandler {
    async fn handle<D>(&self, delivery: &D) -> Result<(), EventBusError>
    where
        D: Delivery + Send + Sync,
    {
        delivery.ack().await?;
        self.tx
            .send(())
            .await
            .map_err(|err| EventBusError::Internal(err.to_string()))
    }
}

struct RetryOnceHandler {
    attempts: Arc<AtomicUsize>,
    tx: mpsc::Sender<usize>,
}

impl Handler for RetryOnceHandler {
    async fn handle<D>(&self, delivery: &D) -> Result<(), EventBusError>
    where
        D: Delivery + Send + Sync,
    {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt == 1 {
            delivery
                .retry(&std::io::Error::other("retry-later"))
                .await?;
        } else {
            delivery.ack().await?;
        }

        self.tx
            .send(attempt)
            .await
            .map_err(|err| EventBusError::Internal(err.to_string()))
    }
}

struct ReceiveOnlyHandler {
    tx: mpsc::Sender<()>,
}

impl Handler for ReceiveOnlyHandler {
    async fn handle<D>(&self, _delivery: &D) -> Result<(), EventBusError>
    where
        D: Delivery + Send + Sync,
    {
        self.tx
            .send(())
            .await
            .map_err(|err| EventBusError::Internal(err.to_string()))
    }
}

struct AckAndSignalHandler {
    tx: mpsc::Sender<()>,
}

impl Handler for AckAndSignalHandler {
    async fn handle<D>(&self, delivery: &D) -> Result<(), EventBusError>
    where
        D: Delivery + Send + Sync,
    {
        delivery.ack().await?;
        self.tx
            .send(())
            .await
            .map_err(|err| EventBusError::Internal(err.to_string()))
    }
}

struct NackHandler {
    tx: mpsc::Sender<()>,
}

impl Handler for NackHandler {
    async fn handle<D>(&self, delivery: &D) -> Result<(), EventBusError>
    where
        D: Delivery + Send + Sync,
    {
        delivery
            .nack(&std::io::Error::other("poison-message"))
            .await?;
        self.tx
            .send(())
            .await
            .map_err(|err| EventBusError::Internal(err.to_string()))
    }
}

struct ErrorHandler {
    attempts: Arc<AtomicUsize>,
}

impl Handler for ErrorHandler {
    async fn handle<D>(&self, _delivery: &D) -> Result<(), EventBusError>
    where
        D: Delivery + Send + Sync,
    {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(EventBusError::Internal("handler failed".into()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_subscribe_auto_ack_drains_pending() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = RedisStreamBus::new(backend.clone(), RedisStreamBusOptions::default())
        .expect("construct bus");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "evt.user".to_string(),
                consumer_group: "cg.auto-ack".to_string(),
                consumer_name: "consumer-1".to_string(),
                auto_ack: true,
                concurrency: 1,
                ..Default::default()
            },
            AutoAckHandler { tx },
        )
        .await
        .expect("subscribe");

    bus.publish(
        message("evt.user", "uid-auto-ack"),
        PublishOptions::new()
            .with_idempotency_key("idem-auto-ack")
            .with_metadata("meta-key", "meta-val"),
    )
    .await
    .expect("publish");

    let got = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("receive in time")
        .expect("message delivered");
    assert_eq!(got.uid, "uid-auto-ack");
    assert_eq!(got.idempotency_key.as_deref(), Some("idem-auto-ack"));
    assert_eq!(got.headers.get("meta-key").map(String::as_str), Some("meta-val"));

    assert_eq!(backend.pending_count("evt.user", "cg.auto-ack").await, 0);
    sub.close().await.expect("close sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_ack_drains_pending() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = RedisStreamBus::new(backend.clone(), RedisStreamBusOptions::default())
        .expect("construct bus");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "evt.manual".to_string(),
                consumer_group: "cg.manual".to_string(),
                consumer_name: "consumer-1".to_string(),
                ack_mode: Some(AckMode::Manual),
                concurrency: 1,
                ..Default::default()
            },
            ManualAckHandler { tx },
        )
        .await
        .expect("subscribe");

    bus.publish(message("evt.manual", "uid-manual"), PublishOptions::default())
        .await
        .expect("publish");

    timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("ack in time")
        .expect("ack signal");
    assert_eq!(backend.pending_count("evt.manual", "cg.manual").await, 0);
    sub.close().await.expect("close sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_redelivers_message_and_then_drains_pending() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = RedisStreamBus::new(backend.clone(), RedisStreamBusOptions::default())
        .expect("construct bus");

    let attempts = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::channel(4);
    let sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "evt.retry".to_string(),
                consumer_group: "cg.retry".to_string(),
                consumer_name: "consumer-1".to_string(),
                ack_mode: Some(AckMode::Manual),
                concurrency: 1,
                max_retry: 5,
                ..Default::default()
            },
            RetryOnceHandler {
                attempts: attempts.clone(),
                tx,
            },
        )
        .await
        .expect("subscribe");

    bus.publish(message("evt.retry", "uid-retry"), PublishOptions::default())
        .await
        .expect("publish");

    timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("first attempt")
        .expect("signal");
    timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("second attempt")
        .expect("signal");

    assert!(attempts.load(Ordering::SeqCst) >= 2);
    assert_eq!(backend.pending_count("evt.retry", "cg.retry").await, 0);
    sub.close().await.expect("close sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaims_pending_from_inactive_consumer() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = RedisStreamBus::new(
        backend.clone(),
        RedisStreamBusOptions {
            claim_idle_timeout: Duration::from_millis(20),
            ..Default::default()
        },
    )
    .expect("construct bus");

    let (first_tx, mut first_rx) = mpsc::channel(1);
    let first_sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "evt.reclaim".to_string(),
                consumer_group: "cg.reclaim".to_string(),
                consumer_name: "consumer-1".to_string(),
                ack_mode: Some(AckMode::Manual),
                concurrency: 1,
                ..Default::default()
            },
            ReceiveOnlyHandler { tx: first_tx },
        )
        .await
        .expect("first subscribe");

    bus.publish(message("evt.reclaim", "uid-reclaim"), PublishOptions::default())
        .await
        .expect("publish");

    timeout(Duration::from_secs(2), first_rx.recv())
        .await
        .expect("first receive")
        .expect("signal");
    first_sub.close().await.expect("close first sub");

    sleep(Duration::from_millis(50)).await;

    let (second_tx, mut second_rx) = mpsc::channel(1);
    let second_sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "evt.reclaim".to_string(),
                consumer_group: "cg.reclaim".to_string(),
                consumer_name: "consumer-2".to_string(),
                ack_mode: Some(AckMode::Manual),
                concurrency: 1,
                ..Default::default()
            },
            AckAndSignalHandler { tx: second_tx },
        )
        .await
        .expect("second subscribe");

    timeout(Duration::from_secs(2), second_rx.recv())
        .await
        .expect("second receive")
        .expect("signal");

    assert_eq!(backend.pending_count("evt.reclaim", "cg.reclaim").await, 0);
    second_sub.close().await.expect("close second sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_routes_to_dead_letter_stream() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = RedisStreamBus::new(backend.clone(), RedisStreamBusOptions::default())
        .expect("construct bus");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "evt.nack".to_string(),
                consumer_group: "cg.nack".to_string(),
                consumer_name: "consumer-1".to_string(),
                ack_mode: Some(AckMode::Manual),
                dead_letter_topic: Some("evt.nack.dlq".to_string()),
                concurrency: 1,
                ..Default::default()
            },
            NackHandler { tx },
        )
        .await
        .expect("subscribe");

    bus.publish(message("evt.nack", "uid-nack"), PublishOptions::default())
        .await
        .expect("publish");

    timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("nack handled")
        .expect("signal");

    assert_eq!(backend.pending_count("evt.nack", "cg.nack").await, 0);
    assert_eq!(backend.stream_len("evt.nack.dlq").await, 1);
    sub.close().await.expect("close sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_max_routes_to_dead_letter_stream() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = RedisStreamBus::new(backend.clone(), RedisStreamBusOptions::default())
        .expect("construct bus");

    let attempts = Arc::new(AtomicUsize::new(0));
    let result = Arc::new(Mutex::new(Vec::new()));

    struct RetryToDeadLetterHandler {
        attempts: Arc<AtomicUsize>,
        result: Arc<Mutex<Vec<usize>>>,
    }

    impl Handler for RetryToDeadLetterHandler {
        async fn handle<D>(&self, delivery: &D) -> Result<(), EventBusError>
        where
            D: Delivery + Send + Sync,
        {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            delivery
                .retry(&std::io::Error::other("retry-limit"))
                .await?;
            self.result.lock().await.push(attempt);
            Ok(())
        }
    }

    let sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "evt.retry.max".to_string(),
                consumer_group: "cg.retry.max".to_string(),
                consumer_name: "consumer-1".to_string(),
                ack_mode: Some(AckMode::Manual),
                dead_letter_topic: Some("evt.retry.max.dlq".to_string()),
                concurrency: 1,
                max_retry: 1,
                ..Default::default()
            },
            RetryToDeadLetterHandler {
                attempts: attempts.clone(),
                result: result.clone(),
            },
        )
        .await
        .expect("subscribe");

    bus.publish(
        message("evt.retry.max", "uid-retry-max"),
        PublishOptions::default(),
    )
    .await
    .expect("publish");

    timeout(Duration::from_secs(2), async {
        loop {
            if backend.stream_len("evt.retry.max.dlq").await >= 1 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("dead letter written");

    assert_eq!(backend.pending_count("evt.retry.max", "cg.retry.max").await, 0);
    assert_eq!(backend.stream_len("evt.retry.max.dlq").await, 1);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(result.lock().await.len(), 1);
    sub.close().await.expect("close sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_ack_handler_error_does_not_auto_retry() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = RedisStreamBus::new(backend.clone(), RedisStreamBusOptions::default())
        .expect("construct bus");

    let attempts = Arc::new(AtomicUsize::new(0));
    let sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "evt.manual.error".to_string(),
                consumer_group: "cg.manual.error".to_string(),
                consumer_name: "consumer-1".to_string(),
                ack_mode: Some(AckMode::Manual),
                concurrency: 1,
                max_retry: 5,
                ..Default::default()
            },
            ErrorHandler {
                attempts: attempts.clone(),
            },
        )
        .await
        .expect("subscribe");

    bus.publish(
        message("evt.manual.error", "uid-manual-error"),
        PublishOptions::default(),
    )
    .await
    .expect("publish");

    timeout(Duration::from_secs(2), async {
        loop {
            if attempts.load(Ordering::SeqCst) >= 1 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("handler invoked");

    sleep(Duration::from_millis(100)).await;

    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(backend.pending_count("evt.manual.error", "cg.manual.error").await, 1);
    assert_eq!(backend.stream_len("evt.manual.error").await, 1);

    sub.close().await.expect("close sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_generates_consumer_name_when_empty() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = RedisStreamBus::new(backend.clone(), RedisStreamBusOptions::default())
        .expect("construct bus");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "evt.generated-consumer".to_string(),
                consumer_group: "cg.generated-consumer".to_string(),
                consumer_name: String::new(),
                auto_ack: true,
                concurrency: 1,
                ..Default::default()
            },
            AutoAckHandler { tx },
        )
        .await
        .expect("subscribe with generated consumer name");

    assert!(!sub.name().trim().is_empty());

    bus.publish(
        message("evt.generated-consumer", "uid-generated-consumer"),
        PublishOptions::default(),
    )
    .await
    .expect("publish");

    timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("message received")
        .expect("delivered");

    sub.close().await.expect("close sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_start_id_zero_reads_existing_messages() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = RedisStreamBus::new(
        backend.clone(),
        RedisStreamBusOptions {
            group_start_id: "0".to_string(),
            ..Default::default()
        },
    )
    .expect("construct bus");

    bus.publish(
        message("evt.from-zero", "uid-existing"),
        PublishOptions::default(),
    )
    .await
    .expect("publish existing before subscribe");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "evt.from-zero".to_string(),
                consumer_group: "cg.from-zero".to_string(),
                consumer_name: "consumer-1".to_string(),
                auto_ack: true,
                concurrency: 1,
                ..Default::default()
            },
            AutoAckHandler { tx },
        )
        .await
        .expect("subscribe");

    let got = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("existing message received")
        .expect("delivered");
    assert_eq!(got.uid, "uid-existing");

    sub.close().await.expect("close sub");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_batch_rejects_invalid_input_before_publishing_any_message() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = RedisStreamBus::new(backend.clone(), RedisStreamBusOptions::default())
        .expect("construct bus");

    let messages = vec![
        message("evt.batch.atomic", "uid-valid"),
        message("", "uid-invalid"),
    ];

    let err = bus
        .publish_batch(messages, PublishOptions::default())
        .await
        .expect_err("batch validation should fail");

    assert!(matches!(err, EventBusError::Validation(_)));
    assert_eq!(backend.stream_len("evt.batch.atomic").await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_allows_empty_uid_to_match_go_parity() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = RedisStreamBus::new(backend.clone(), RedisStreamBusOptions::default())
        .expect("construct bus");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "evt.empty-uid".to_string(),
                consumer_group: "cg.empty-uid".to_string(),
                consumer_name: "consumer-1".to_string(),
                auto_ack: true,
                concurrency: 1,
                ..Default::default()
            },
            AutoAckHandler { tx },
        )
        .await
        .expect("subscribe");

    bus.publish(message("evt.empty-uid", ""), PublishOptions::default())
        .await
        .expect("publish with empty uid");

    let got = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("message received")
        .expect("delivered");
    assert!(got.uid.is_empty());

    sub.close().await.expect("close sub");
}
