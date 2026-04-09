use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use std::{collections::VecDeque, future::Future, pin::Pin};

use chrono::Utc;
use eventbus_contract::redis_stream::{
    ClaimedMessage, MemoryStreamBackend, RedisStreamBus, RedisStreamBusOptions, StreamBackend,
};
use eventbus_contract::{
    AckMode, Delivery, DeliveryState, EventBusError, Handler, Headers, Message, PublishOptions,
    SubscriptionConfig,
};
use tokio::sync::{mpsc, Mutex, Notify};
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
                ack_mode: AckMode::AutoOnHandlerSuccess,
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
    assert_eq!(
        got.headers.get("meta-key").map(String::as_str),
        Some("meta-val")
    );

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
                ack_mode: AckMode::Manual,
                concurrency: 1,
                ..Default::default()
            },
            ManualAckHandler { tx },
        )
        .await
        .expect("subscribe");

    bus.publish(
        message("evt.manual", "uid-manual"),
        PublishOptions::default(),
    )
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
                ack_mode: AckMode::Manual,
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
                ack_mode: AckMode::Manual,
                concurrency: 1,
                ..Default::default()
            },
            ReceiveOnlyHandler { tx: first_tx },
        )
        .await
        .expect("first subscribe");

    bus.publish(
        message("evt.reclaim", "uid-reclaim"),
        PublishOptions::default(),
    )
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
                ack_mode: AckMode::Manual,
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
                ack_mode: AckMode::Manual,
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
                ack_mode: AckMode::Manual,
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

    assert_eq!(
        backend.pending_count("evt.retry.max", "cg.retry.max").await,
        0
    );
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
                ack_mode: AckMode::Manual,
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
    assert_eq!(
        backend
            .pending_count("evt.manual.error", "cg.manual.error")
            .await,
        1
    );
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
                ack_mode: AckMode::AutoOnHandlerSuccess,
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
async fn drop_subscription_stops_background_workers() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus =
        RedisStreamBus::new(backend, RedisStreamBusOptions::default()).expect("construct bus");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "evt.drop".to_string(),
                consumer_group: "cg.drop".to_string(),
                consumer_name: "consumer-1".to_string(),
                ack_mode: AckMode::AutoOnHandlerSuccess,
                concurrency: 1,
                ..Default::default()
            },
            AutoAckHandler { tx },
        )
        .await
        .expect("subscribe");

    drop(sub);
    sleep(Duration::from_millis(50)).await;

    bus.publish(message("evt.drop", "uid-drop"), PublishOptions::default())
        .await
        .expect("publish");

    match timeout(Duration::from_millis(200), rx.recv()).await {
        Err(_) | Ok(None) => {}
        Ok(Some(_)) => panic!("dropped subscription should not deliver new messages"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscription_respects_max_in_flight_limit() {
    struct BlockingHandler {
        started_tx: mpsc::Sender<String>,
        release: Arc<tokio::sync::Semaphore>,
    }

    impl Handler for BlockingHandler {
        async fn handle<D>(&self, delivery: &D) -> Result<(), EventBusError>
        where
            D: Delivery + Send + Sync,
        {
            self.started_tx
                .send(delivery.message().uid.clone())
                .await
                .map_err(|err| EventBusError::Internal(err.to_string()))?;
            let _permit = self
                .release
                .acquire()
                .await
                .map_err(|err| EventBusError::Internal(err.to_string()))?;
            delivery.ack().await
        }
    }

    let backend = Arc::new(MemoryStreamBackend::default());
    let bus =
        RedisStreamBus::new(backend, RedisStreamBusOptions::default()).expect("construct bus");

    let (started_tx, mut started_rx) = mpsc::channel(4);
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "evt.max-in-flight".to_string(),
                consumer_group: "cg.max-in-flight".to_string(),
                consumer_name: "consumer".to_string(),
                ack_mode: AckMode::Manual,
                concurrency: 4,
                max_in_flight: 1,
                max_pending_acks: 1,
                ..Default::default()
            },
            BlockingHandler {
                started_tx,
                release: release.clone(),
            },
        )
        .await
        .expect("subscribe");

    bus.publish(
        message("evt.max-in-flight", "uid-1"),
        PublishOptions::default(),
    )
    .await
    .expect("publish first");
    bus.publish(
        message("evt.max-in-flight", "uid-2"),
        PublishOptions::default(),
    )
    .await
    .expect("publish second");

    let first = timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .expect("first delivery")
        .expect("first uid");
    assert_eq!(first, "uid-1");
    assert!(timeout(Duration::from_millis(200), started_rx.recv())
        .await
        .is_err());

    release.add_permits(2);
    timeout(Duration::from_secs(2), started_rx.recv())
        .await
        .expect("second delivery")
        .expect("second uid");

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
                ack_mode: AckMode::AutoOnHandlerSuccess,
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
                ack_mode: AckMode::AutoOnHandlerSuccess,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaimed_messages_consume_pending_ack_budget() {
    struct ObserveOnlyHandler {
        started_tx: mpsc::Sender<String>,
    }

    impl Handler for ObserveOnlyHandler {
        async fn handle<D>(&self, delivery: &D) -> Result<(), EventBusError>
        where
            D: Delivery + Send + Sync,
        {
            self.started_tx
                .send(delivery.message().uid.clone())
                .await
                .map_err(|err| EventBusError::Internal(err.to_string()))
        }
    }

    struct AckUidHandler {
        tx: mpsc::Sender<String>,
    }

    impl Handler for AckUidHandler {
        async fn handle<D>(&self, delivery: &D) -> Result<(), EventBusError>
        where
            D: Delivery + Send + Sync,
        {
            self.tx
                .send(delivery.message().uid.clone())
                .await
                .map_err(|err| EventBusError::Internal(err.to_string()))?;
            delivery.ack().await
        }
    }

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
                topic: "evt.reclaim.pending-budget".to_string(),
                consumer_group: "cg.reclaim.pending-budget".to_string(),
                consumer_name: "consumer-1".to_string(),
                ack_mode: AckMode::Manual,
                concurrency: 1,
                ..Default::default()
            },
            ReceiveOnlyHandler { tx: first_tx },
        )
        .await
        .expect("first subscribe");

    bus.publish(
        message("evt.reclaim.pending-budget", "uid-reclaim"),
        PublishOptions::default(),
    )
    .await
    .expect("publish reclaim candidate");

    timeout(Duration::from_secs(2), first_rx.recv())
        .await
        .expect("first receive")
        .expect("signal");
    first_sub.close().await.expect("close first sub");

    sleep(Duration::from_millis(50)).await;

    let (started_tx, mut started_rx) = mpsc::channel(2);
    let second_sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "evt.reclaim.pending-budget".to_string(),
                consumer_group: "cg.reclaim.pending-budget".to_string(),
                consumer_name: "consumer-2".to_string(),
                ack_mode: AckMode::Manual,
                concurrency: 1,
                max_in_flight: 1,
                max_pending_acks: 1,
                ..Default::default()
            },
            ObserveOnlyHandler { started_tx },
        )
        .await
        .expect("second subscribe");

    bus.publish(
        message("evt.reclaim.pending-budget", "uid-new"),
        PublishOptions::default(),
    )
    .await
    .expect("publish fresh message");

    let first = timeout(Duration::from_secs(2), started_rx.recv())
        .await
        .expect("reclaimed delivery")
        .expect("uid");
    assert_eq!(first, "uid-reclaim");
    assert!(timeout(Duration::from_millis(200), started_rx.recv())
        .await
        .is_err());

    second_sub.close().await.expect("close second sub");

    sleep(Duration::from_millis(50)).await;

    let (drain_tx, mut drain_rx) = mpsc::channel(2);
    let third_sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "evt.reclaim.pending-budget".to_string(),
                consumer_group: "cg.reclaim.pending-budget".to_string(),
                consumer_name: "consumer-3".to_string(),
                ack_mode: AckMode::Manual,
                concurrency: 1,
                max_in_flight: 1,
                max_pending_acks: 1,
                ..Default::default()
            },
            AckUidHandler { tx: drain_tx },
        )
        .await
        .expect("third subscribe");

    let reclaimed = timeout(Duration::from_secs(2), drain_rx.recv())
        .await
        .expect("reclaimed delivery after reopen")
        .expect("uid");
    assert_eq!(reclaimed, "uid-reclaim");

    let fresh = timeout(Duration::from_secs(2), drain_rx.recv())
        .await
        .expect("fresh delivery after reopen")
        .expect("uid");
    assert_eq!(fresh, "uid-new");

    third_sub.close().await.expect("close third sub");
}

#[derive(Default)]
struct FailingAckBackend {
    next_id: AtomicUsize,
    queue: Mutex<VecDeque<ClaimedMessage>>,
    notify: Notify,
}

impl FailingAckBackend {
    fn read_new_impl<'a>(
        &'a self,
        count: usize,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ClaimedMessage>, EventBusError>> + Send + 'a>> {
        Box::pin(async move {
            if count == 0 {
                return Ok(Vec::new());
            }

            {
                let mut queue = self.queue.lock().await;
                if !queue.is_empty() {
                    let take = count.min(queue.len());
                    return Ok(queue.drain(..take).collect());
                }
            }

            if timeout.is_zero() {
                return Ok(Vec::new());
            }

            let notified = self.notify.notified();
            let _ = tokio::time::timeout(timeout, notified).await;
            let mut queue = self.queue.lock().await;
            let take = count.min(queue.len());
            Ok(queue.drain(..take).collect())
        })
    }
}

impl StreamBackend for FailingAckBackend {
    fn create_group(
        &self,
        _stream: &str,
        _group: &str,
        _start_id: &str,
    ) -> impl Future<Output = Result<(), EventBusError>> + Send {
        async { Ok(()) }
    }

    fn publish(
        &self,
        _stream: &str,
        message: Message,
    ) -> impl Future<Output = Result<String, EventBusError>> + Send {
        async move {
            let id = format!("{}-0", self.next_id.fetch_add(1, Ordering::SeqCst));
            {
                let mut queue = self.queue.lock().await;
                queue.push_back(ClaimedMessage {
                    id: id.clone(),
                    message,
                    state: DeliveryState {
                        attempt: 1,
                        max_attempt: 1,
                        first_received: Utc::now(),
                        last_received: Utc::now(),
                        redelivered: false,
                    },
                });
            }
            self.notify.notify_waiters();
            Ok(id)
        }
    }

    fn reclaim_idle(
        &self,
        _stream: &str,
        _group: &str,
        _consumer: &str,
        _min_idle: Duration,
        _count: usize,
    ) -> impl Future<Output = Result<Vec<ClaimedMessage>, EventBusError>> + Send {
        async { Ok(Vec::new()) }
    }

    fn read_new(
        &self,
        _stream: &str,
        _group: &str,
        _consumer: &str,
        count: usize,
        timeout: Duration,
    ) -> impl Future<Output = Result<Vec<ClaimedMessage>, EventBusError>> + Send {
        self.read_new_impl(count, timeout)
    }

    fn ack(
        &self,
        _stream: &str,
        _group: &str,
        _message_id: &str,
    ) -> impl Future<Output = Result<(), EventBusError>> + Send {
        async { Err(EventBusError::Connection("ack failed".into())) }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscription_close_surfaces_background_delivery_errors() {
    let backend = Arc::new(FailingAckBackend::default());
    let bus =
        RedisStreamBus::new(backend, RedisStreamBusOptions::default()).expect("construct bus");

    let (tx, mut rx) = mpsc::channel(1);
    let sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "evt.failing-ack".to_string(),
                consumer_group: "cg.failing-ack".to_string(),
                consumer_name: "consumer-1".to_string(),
                ack_mode: AckMode::AutoOnHandlerSuccess,
                concurrency: 1,
                ..Default::default()
            },
            AutoAckHandler { tx },
        )
        .await
        .expect("subscribe");

    bus.publish(
        message("evt.failing-ack", "uid-1"),
        PublishOptions::default(),
    )
    .await
    .expect("publish");

    timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("handler ran")
        .expect("message");

    let err = sub
        .close()
        .await
        .expect_err("close should surface task error");
    assert!(matches!(err, EventBusError::Connection(_)));
}

#[derive(Default)]
struct ParallelPublishBackend {
    next_id: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
}

impl StreamBackend for ParallelPublishBackend {
    fn create_group(
        &self,
        _stream: &str,
        _group: &str,
        _start_id: &str,
    ) -> impl Future<Output = Result<(), EventBusError>> + Send {
        async { Ok(()) }
    }

    fn publish(
        &self,
        _stream: &str,
        _message: Message,
    ) -> impl Future<Output = Result<String, EventBusError>> + Send {
        async move {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            let _ = self
                .max_active
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    Some(current.max(active))
                });
            sleep(Duration::from_millis(25)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(format!("{}-0", self.next_id.fetch_add(1, Ordering::SeqCst)))
        }
    }

    fn reclaim_idle(
        &self,
        _stream: &str,
        _group: &str,
        _consumer: &str,
        _min_idle: Duration,
        _count: usize,
    ) -> impl Future<Output = Result<Vec<ClaimedMessage>, EventBusError>> + Send {
        async { Ok(Vec::new()) }
    }

    fn read_new(
        &self,
        _stream: &str,
        _group: &str,
        _consumer: &str,
        _count: usize,
        _timeout: Duration,
    ) -> impl Future<Output = Result<Vec<ClaimedMessage>, EventBusError>> + Send {
        async { Ok(Vec::new()) }
    }

    fn ack(
        &self,
        _stream: &str,
        _group: &str,
        _message_id: &str,
    ) -> impl Future<Output = Result<(), EventBusError>> + Send {
        async { Ok(()) }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_batch_uses_parallel_publishes() {
    let backend = Arc::new(ParallelPublishBackend::default());
    let bus = RedisStreamBus::new(backend.clone(), RedisStreamBusOptions::default())
        .expect("construct bus");

    let messages = vec![
        message("evt.parallel", "uid-1"),
        message("evt.parallel", "uid-2"),
        message("evt.parallel", "uid-3"),
        message("evt.parallel", "uid-4"),
    ];

    bus.publish_batch(messages, PublishOptions::default())
        .await
        .expect("publish batch");

    assert!(backend.max_active.load(Ordering::SeqCst) > 1);
}
