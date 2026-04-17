//! # Competing Consumers
//!
//! Shows horizontal scaling within a single consumer group:
//!
//! - `concurrency: N` allows up to N handler tasks to run concurrently
//! - All workers share the same consumer group — each message is processed
//!   by exactly one worker (competing/queue semantics)
//! - The order of delivery across workers is not guaranteed
//!
//! Run with:
//! ```text
//! cargo run --example 03_competing_consumers
//! ```

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use chrono::Utc;
use eventbus_contract::stream::{MemoryStreamBackend, StreamBus, StreamBusOptions};
use eventbus_contract::{
    AckMode, Delivery, EventBusError, Handler, Headers, Message, PublishOptions, SubscriptionConfig,
};
use tokio::sync::mpsc;
use tokio::time::timeout;

// ---------------------------------------------------------------------------
// Handler: records which thread handled each message
// ---------------------------------------------------------------------------

struct WorkerHandler {
    worker_id: usize,
    processed: Arc<AtomicUsize>,
    tx: mpsc::Sender<(usize, String)>, // (worker_id, message_uid)
}

impl Handler for WorkerHandler {
    async fn handle<D>(&self, delivery: &D) -> Result<(), EventBusError>
    where
        D: Delivery + Send + Sync,
    {
        let uid = delivery.message().uid.clone();
        self.processed.fetch_add(1, Ordering::SeqCst);
        println!("[worker-{}] handling uid={uid}", self.worker_id);
        delivery.ack().await?;
        self.tx
            .send((self.worker_id, uid))
            .await
            .map_err(|e| EventBusError::Internal(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const WORKER_COUNT: usize = 3;
    const MESSAGE_COUNT: usize = 9;

    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(Arc::clone(&backend), StreamBusOptions::default())?;

    let processed = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::channel(MESSAGE_COUNT);

    // Subscribe with concurrency=3: the bus may run up to three handler tasks
    // concurrently while sharing the same consumer group.
    let sub = bus
        .subscribe(
            SubscriptionConfig {
                topic: "job.created".to_string(),
                consumer_group: "job-processor".to_string(),
                // consumer_name is used as a prefix when concurrency > 1:
                // the Redis consumer identity remains stable for this subscriber
                consumer_name: "processor".to_string(),
                concurrency: WORKER_COUNT,
                ack_mode: AckMode::Manual,
                ..Default::default()
            },
            WorkerHandler {
                worker_id: 0, // all workers share the same handler type; worker_id
                // is illustrative — in practice you'd use thread-local
                // context or instrument with tracing spans
                processed: Arc::clone(&processed),
                tx,
            },
        )
        .await?;

    println!("[main] publishing {MESSAGE_COUNT} messages…");
    for i in 0..MESSAGE_COUNT {
        bus.publish(
            Message {
                uid: format!("job-{i:03}"),
                topic: "job.created".to_string(),
                key: format!("job-{i}"),
                kind: "JobCreated".to_string(),
                source: "scheduler".to_string(),
                occurred_at: Utc::now(),
                headers: Headers::new(),
                payload: bytes::Bytes::from(format!(r#"{{"job_id": {i}}}"#).into_bytes()),
                content_type: Some("application/json".to_string()),
                event_version: None,
                idempotency_key: None,
                expires_at: None,
                trace_uid: None,
                correlation_uid: None,
            },
            PublishOptions::default(),
        )
        .await?;
    }

    // Collect all MESSAGE_COUNT delivery receipts.
    let mut receipts = Vec::with_capacity(MESSAGE_COUNT);
    for _ in 0..MESSAGE_COUNT {
        let receipt = timeout(Duration::from_secs(5), rx.recv())
            .await?
            .expect("channel closed");
        receipts.push(receipt);
    }

    println!("\n[main] all {MESSAGE_COUNT} messages delivered:");
    for (worker, uid) in &receipts {
        println!("  worker={worker} uid={uid}");
    }

    assert_eq!(receipts.len(), MESSAGE_COUNT);
    assert_eq!(
        backend.pending_count("job.created", "job-processor").await,
        0
    );
    println!("\n[main] pending=0, all messages processed ✓");

    sub.close().await?;
    Ok(())
}
