//! Run against a disposable Redis: EVENTBUS_REDIS_URL=redis://127.0.0.1:6379/
//! cargo test -p eventbus-redis --test connections -- --ignored

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use eventbus_core::stream::{FetchedEntry, StreamBackend, StreamBus, StreamBusOptions};
use eventbus_core::{
    BoxFuture, ConsumerGroup, ConsumerName, DeliveryHandle, EventBusError, Handler, Headers,
    Message, SubscriptionConfig, Topic,
};
use eventbus_redis::RedisBackend;
use tokio::time::{sleep, timeout};

fn client() -> redis::Client {
    redis::Client::open(
        std::env::var("EVENTBUS_REDIS_URL").expect("set EVENTBUS_REDIS_URL to a disposable Redis"),
    )
    .unwrap()
}

fn stream() -> String {
    static NEXT_STREAM: AtomicU64 = AtomicU64::new(0);
    format!(
        "eventbus-test-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT_STREAM.fetch_add(1, Ordering::Relaxed)
    )
}

fn message(stream: &str) -> Message {
    Message {
        uid: "one".into(),
        topic: Topic::new(stream).unwrap(),
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

struct Ack;

impl Handler for Ack {
    fn handle(
        &self,
        delivery: Box<dyn DeliveryHandle>,
    ) -> BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move { delivery.ack().await })
    }
}

#[tokio::test]
#[ignore = "requires EVENTBUS_REDIS_URL"]
async fn subscription_abort_releases_consumer_state() {
    for warm_reader in [false, true] {
        let backend = Arc::new(RedisBackend::from_client(client()).await.unwrap());
        let stream = stream();
        let codec: Arc<dyn eventbus_redis::RedisStreamCodec> =
            Arc::new(eventbus_redis::EnvelopeStreamCodec::default());
        backend.set_subscription_read_codec(&stream, "group", "worker", Arc::clone(&codec));
        let bus = StreamBus::new(Arc::clone(&backend), StreamBusOptions::default()).unwrap();
        let config = SubscriptionConfig::builder(
            Topic::new(&stream).unwrap(),
            ConsumerGroup::new("group").unwrap(),
        )
        .consumer_name(ConsumerName::new("worker").unwrap())
        .build()
        .unwrap();
        let sub = bus.subscribe(config, Ack).await.unwrap();
        if warm_reader {
            sleep(Duration::from_millis(100)).await;
        }
        sub.abort().await.unwrap();
        let mut cleanup = client().get_multiplexed_async_connection().await.unwrap();
        redis::cmd("DEL")
            .arg(&stream)
            .query_async::<usize>(&mut cleanup)
            .await
            .unwrap();
        assert_eq!(
            Arc::strong_count(&codec),
            1,
            "abort retained consumer resources (warm={warm_reader})"
        );
    }
}

#[tokio::test]
#[ignore = "requires EVENTBUS_REDIS_URL"]
async fn subscription_close_releases_consumer_state() {
    let backend = Arc::new(RedisBackend::from_client(client()).await.unwrap());
    let stream = stream();
    let codec: Arc<dyn eventbus_redis::RedisStreamCodec> =
        Arc::new(eventbus_redis::EnvelopeStreamCodec::default());
    backend.set_subscription_read_codec(&stream, "group", "worker", Arc::clone(&codec));
    let bus = StreamBus::new(Arc::clone(&backend), StreamBusOptions::default()).unwrap();
    let config = SubscriptionConfig::builder(
        Topic::new(&stream).unwrap(),
        ConsumerGroup::new("group").unwrap(),
    )
    .consumer_name(ConsumerName::new("worker").unwrap())
    .build()
    .unwrap();
    let sub = bus.subscribe(config, Ack).await.unwrap();
    sleep(Duration::from_millis(100)).await;
    sub.close().await.unwrap();
    let mut cleanup = client().get_multiplexed_async_connection().await.unwrap();
    redis::cmd("DEL")
        .arg(&stream)
        .query_async::<usize>(&mut cleanup)
        .await
        .unwrap();
    assert_eq!(Arc::strong_count(&codec), 1);
}

struct PanicHandler;

impl Handler for PanicHandler {
    fn handle(
        &self,
        _delivery: Box<dyn DeliveryHandle>,
    ) -> BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async { panic!("intentional handler panic") })
    }
}

#[tokio::test]
#[ignore = "requires EVENTBUS_REDIS_URL"]
async fn consumer_error_exit_releases_state_before_close_is_called() {
    let backend = Arc::new(RedisBackend::from_client(client()).await.unwrap());
    let stream = stream();
    let codec: Arc<dyn eventbus_redis::RedisStreamCodec> =
        Arc::new(eventbus_redis::EnvelopeStreamCodec::default());
    backend.set_subscription_read_codec(&stream, "group", "worker", Arc::clone(&codec));
    let bus = StreamBus::new(
        Arc::clone(&backend),
        StreamBusOptions::new().with_block_timeout(Duration::from_millis(10)),
    )
    .unwrap();
    let config = SubscriptionConfig::builder(
        Topic::new(&stream).unwrap(),
        ConsumerGroup::new("group").unwrap(),
    )
    .consumer_name(ConsumerName::new("worker").unwrap())
    .max_in_flight(2)
    .build()
    .unwrap();
    let sub = bus.subscribe(config, PanicHandler).await.unwrap();
    backend.publish(&stream, message(&stream)).await.unwrap();
    let released = timeout(Duration::from_secs(2), async {
        while Arc::strong_count(&codec) != 1 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let result = sub.close().await;
    let mut cleanup = client().get_multiplexed_async_connection().await.unwrap();
    redis::cmd("DEL")
        .arg(&stream)
        .query_async::<usize>(&mut cleanup)
        .await
        .unwrap();
    released.expect("error exit must release resources without waiting for close");
    assert!(result.is_err(), "close must still report the handler panic");
}

async fn assert_reads_do_not_block_commands(backend: RedisBackend) {
    let backend = Arc::new(backend);
    let empty_stream = stream();
    let output_stream = stream();
    backend
        .create_group(&empty_stream, "group", "0")
        .await
        .unwrap();
    backend
        .create_group(&output_stream, "group", "0")
        .await
        .unwrap();
    let id = backend
        .publish(&output_stream, message(&output_stream))
        .await
        .unwrap();
    let initial = backend
        .read_new(&output_stream, "group", "worker", 1, Duration::ZERO)
        .await
        .unwrap();
    assert!(matches!(&initial[0], FetchedEntry::Decoded(_)));

    let reader = tokio::spawn({
        let backend = Arc::clone(&backend);
        let empty_stream = empty_stream.clone();
        async move {
            backend
                .read_new(&empty_stream, "group", "idle", 1, Duration::from_secs(3))
                .await
        }
    });
    sleep(Duration::from_millis(100)).await;
    let ack = timeout(
        Duration::from_millis(400),
        backend.ack(&output_stream, "group", &id),
    )
    .await;
    let publish = timeout(
        Duration::from_millis(400),
        backend.publish(&output_stream, message(&output_stream)),
    )
    .await;
    reader.abort();
    let _ = reader.await;
    backend
        .forget_consumer(&empty_stream, "group", "idle")
        .await;

    let mut cleanup = client().get_multiplexed_async_connection().await.unwrap();
    redis::cmd("DEL")
        .arg(&[empty_stream, output_stream])
        .query_async::<usize>(&mut cleanup)
        .await
        .unwrap();
    assert!(ack.is_ok(), "an idle reader blocked ACK");
    ack.unwrap().unwrap();
    assert!(publish.is_ok(), "an idle reader blocked publish");
    publish.unwrap().unwrap();
}

#[tokio::test]
#[ignore = "requires EVENTBUS_REDIS_URL"]
async fn shared_connection_constructor_does_not_block_publish_or_ack() {
    let connection = client().get_multiplexed_async_connection().await.unwrap();
    assert_reads_do_not_block_commands(RedisBackend::new(connection)).await;
}

#[tokio::test]
#[ignore = "requires EVENTBUS_REDIS_URL"]
async fn forgetting_consumer_releases_its_registered_codec() {
    let backend = RedisBackend::new(client().get_multiplexed_async_connection().await.unwrap());
    let codec: Arc<dyn eventbus_redis::RedisStreamCodec> =
        Arc::new(eventbus_redis::EnvelopeStreamCodec::default());
    backend.set_subscription_read_codec("stream", "group", "consumer", Arc::clone(&codec));
    assert_eq!(Arc::strong_count(&codec), 2);
    backend.forget_consumer("stream", "group", "consumer").await;
    assert_eq!(
        Arc::strong_count(&codec),
        1,
        "closed consumers must not retain codecs"
    );
}

#[tokio::test]
#[ignore = "requires EVENTBUS_REDIS_URL"]
async fn client_constructor_does_not_block_publish_or_ack() {
    assert_reads_do_not_block_commands(RedisBackend::from_client(client()).await.unwrap()).await;
}

#[tokio::test]
#[ignore = "requires EVENTBUS_REDIS_URL"]
async fn blocking_readers_are_isolated_between_consumers() {
    let backend = Arc::new(RedisBackend::from_client(client()).await.unwrap());
    let empty = stream();
    let ready = stream();
    backend.create_group(&empty, "group", "0").await.unwrap();
    backend.create_group(&ready, "group", "0").await.unwrap();
    backend.publish(&ready, message(&ready)).await.unwrap();
    let reader = tokio::spawn({
        let backend = Arc::clone(&backend);
        let empty = empty.clone();
        async move {
            backend
                .read_new(&empty, "group", "idle", 1, Duration::from_secs(3))
                .await
        }
    });
    sleep(Duration::from_millis(100)).await;
    let received = timeout(
        Duration::from_millis(400),
        backend.read_new(&ready, "group", "busy", 1, Duration::from_secs(3)),
    )
    .await;
    reader.abort();
    let _ = reader.await;
    backend.forget_consumer(&empty, "group", "idle").await;
    backend.forget_consumer(&ready, "group", "busy").await;
    let mut cleanup = client().get_multiplexed_async_connection().await.unwrap();
    redis::cmd("DEL")
        .arg(&[empty, ready])
        .query_async::<usize>(&mut cleanup)
        .await
        .unwrap();
    assert_eq!(
        received
            .expect("idle consumer blocked another reader")
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
#[ignore = "requires EVENTBUS_REDIS_URL"]
async fn zero_timeout_read_returns_immediately_on_an_empty_stream() {
    let backend = RedisBackend::from_client(client()).await.unwrap();
    let stream = stream();
    backend.create_group(&stream, "group", "0").await.unwrap();
    let result = timeout(
        Duration::from_millis(400),
        backend.read_new(&stream, "group", "worker", 1, Duration::ZERO),
    )
    .await;
    backend.forget_consumer(&stream, "group", "worker").await;
    let mut cleanup = client().get_multiplexed_async_connection().await.unwrap();
    redis::cmd("DEL")
        .arg(&stream)
        .query_async::<usize>(&mut cleanup)
        .await
        .unwrap();
    assert!(result
        .expect("zero duration must not turn into BLOCK 0")
        .unwrap()
        .is_empty());
}
