//! # Real Redis Backend
//!
//! Demonstrates using `RedisBackend` with a real Redis server.
//!
//! Requires the `redis-backend` feature and a running Redis instance:
//!
//! ```text
//! # Start Redis (Docker)
//! docker run --rm -p 6379:6379 redis:7
//!
//! # Run the example
//! cargo run --example 04_redis_backend --features redis-backend
//! ```
//!
//! The wire format is JSON-compatible with the Go `StreamBus`:
//! each entry is stored as `XADD <topic> * message <json>`.

#[cfg(not(feature = "redis-backend"))]
fn main() {
    eprintln!("This example requires the `redis-backend` feature.");
    eprintln!("Run with: cargo run --example 04_redis_backend --features redis-backend");
    std::process::exit(1);
}

#[cfg(feature = "redis-backend")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::time::Duration;

    use chrono::Utc;
    use eventbus_contract::stream::{StreamBus, StreamBusOptions};
    use eventbus_contract::{
        AckMode, Delivery, EventBusError, Handler, Headers, Message, PublishOptions,
        SubscriptionConfig,
    };
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    // -----------------------------------------------------------------------
    // Handler
    // -----------------------------------------------------------------------

    struct PrintHandler {
        tx: mpsc::Sender<Message>,
    }

    impl Handler for PrintHandler {
        async fn handle<D>(&self, delivery: &D) -> Result<(), EventBusError>
        where
            D: Delivery + Send + Sync,
        {
            let msg = delivery.message();
            println!(
                "[handler] topic={} uid={} kind={}",
                msg.topic, msg.uid, msg.kind
            );
            delivery.ack().await?;
            self.tx
                .send(msg.clone())
                .await
                .map_err(|e| EventBusError::Internal(e.to_string()))
        }
    }

    // -----------------------------------------------------------------------
    // Connect
    // -----------------------------------------------------------------------

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());

    println!("[main] connecting to {redis_url}");
    let client = redis::Client::open(redis_url.as_str())?;
    let conn = client.get_multiplexed_async_connection().await?;

    // `from_connection` wraps the connection in a RedisBackend and creates the bus.
    let bus = StreamBus::from_connection(conn, StreamBusOptions::default())?;

    // -----------------------------------------------------------------------
    // Subscribe
    // -----------------------------------------------------------------------

    let (tx, mut rx) = mpsc::channel(8);
    let sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "demo.events".to_string(),
                consumer_group: "demo-consumers".to_string(),
                consumer_name: "worker-1".to_string(),
                ack_mode: AckMode::Manual,
                max_retry: 3,
                dead_letter_topic: Some("demo.events.dlq".to_string()),
                concurrency: 1,
                ..Default::default()
            },
            PrintHandler { tx },
        )
        .await?;

    // -----------------------------------------------------------------------
    // Publish
    // -----------------------------------------------------------------------

    println!("[main] publishing…");
    bus.publish(
        Message {
            uid: uuid(),
            topic: "demo.events".to_string(),
            key: "entity-1".to_string(),
            kind: "DemoEvent".to_string(),
            source: "example".to_string(),
            occurred_at: Utc::now(),
            headers: Headers::new(),
            payload: br#"{"hello": "world"}"#.to_vec(),
            content_type: Some("application/json".to_string()),
            event_version: Some("1.0".to_string()),
            idempotency_key: Some(uuid()),
            expires_at: None,
            trace_uid: None,
            correlation_uid: None,
        },
        PublishOptions::new().with_metadata("env", "example"),
    )
    .await?;

    // -----------------------------------------------------------------------
    // Wait for delivery
    // -----------------------------------------------------------------------

    let received = timeout(Duration::from_secs(10), rx.recv())
        .await?
        .expect("channel closed");

    println!("[main] delivered uid={}", received.uid);

    sub.close().await?;
    println!("[main] done");
    Ok(())
}

#[cfg(feature = "redis-backend")]
fn uuid() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    format!("demo-{ns:010}")
}
