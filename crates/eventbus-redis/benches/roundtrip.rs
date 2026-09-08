//! EVENTBUS_REDIS_URL=redis://127.0.0.1:6379/ cargo bench -p eventbus-redis --bench roundtrip
//! Measures publication through completed handler ACK, with bounded stream history.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use criterion::{criterion_group, criterion_main, Criterion};
use eventbus_core::stream::{StreamBus, StreamBusOptions};
use eventbus_core::{
    BoxFuture, ConsumerGroup, DeliveryHandle, EventBusError, Handler, Headers, Message,
    PublishOptions, SubscriptionConfig, Topic,
};
use eventbus_redis::RedisBackend;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

struct Ack(mpsc::UnboundedSender<()>);

impl Handler for Ack {
    fn handle(
        &self,
        delivery: Box<dyn DeliveryHandle>,
    ) -> BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move {
            delivery.ack().await?;
            self.0.send(()).expect("benchmark receiver");
            Ok(())
        })
    }
}

fn message(topic: &Topic) -> Message {
    Message {
        uid: "bench".into(),
        topic: topic.clone(),
        key: String::new(),
        kind: "bench".into(),
        source: "bench".into(),
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

fn redis_roundtrip(c: &mut Criterion) {
    let Ok(url) = std::env::var("EVENTBUS_REDIS_URL") else {
        eprintln!("Skipping Redis roundtrip benchmark: set EVENTBUS_REDIS_URL");
        return;
    };
    let rt = Runtime::new().unwrap();
    let client = redis::Client::open(url).unwrap();
    let backend = rt
        .block_on(RedisBackend::from_client(client.clone()))
        .unwrap();
    let mut cleanup = rt
        .block_on(client.get_multiplexed_async_connection())
        .unwrap();
    let bus = StreamBus::new(Arc::new(backend), StreamBusOptions::default()).unwrap();
    let topic = Topic::new(format!(
        "eventbus-bench-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
    .unwrap();
    let config = SubscriptionConfig::builder(topic.clone(), ConsumerGroup::new("bench").unwrap())
        .build()
        .unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let subscription = rt.block_on(bus.subscribe(config, Ack(tx))).unwrap();

    c.bench_function("redis_publish_to_ack", |b| {
        b.iter_custom(|iterations| {
            rt.block_on(async {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    let msg = message(&topic);
                    let start = Instant::now();
                    let id = bus.publish(msg, PublishOptions::default()).await.unwrap();
                    tokio::time::timeout(Duration::from_secs(10), rx.recv())
                        .await
                        .expect("handler ACK timeout")
                        .expect("handler ACK");
                    elapsed += start.elapsed();
                    // Remove only this acknowledged entry, outside the timed work.
                    redis::cmd("XDEL")
                        .arg(topic.as_str())
                        .arg(id.as_str())
                        .query_async::<usize>(&mut cleanup)
                        .await
                        .unwrap();
                }
                elapsed
            })
        });
    });
    rt.block_on(async {
        subscription.close().await.unwrap();
        redis::cmd("DEL")
            .arg(topic.as_str())
            .query_async::<usize>(&mut cleanup)
            .await
            .unwrap();
    });
}

criterion_group!(benches, redis_roundtrip);
criterion_main!(benches);
