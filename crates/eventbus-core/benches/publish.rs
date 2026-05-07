//! Criterion micro-benches for `StreamBus` publish paths.
//!
//! Run with `cargo bench -p eventbus-core --no-run` to verify the bench
//! compiles. The benches themselves are not part of CI; run with
//! `cargo bench -p eventbus-core` locally to collect numbers.

use std::sync::Arc;

use chrono::Utc;
use criterion::{criterion_group, criterion_main, Criterion};
use eventbus_core::stream::{MemoryStreamBackend, StreamBus, StreamBusOptions};
use eventbus_core::{Headers, Message, PublishOptions, Publisher, Topic};
use tokio::runtime::Runtime;

fn message(uid: &str) -> Message {
    Message {
        uid: uid.to_string(),
        topic: Topic::new("bench.publish").unwrap(),
        key: String::new(),
        kind: "bench".into(),
        source: "bench".into(),
        occurred_at: Utc::now(),
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

fn bench_publish_single(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend, StreamBusOptions::default()).unwrap();
    c.bench_function("publish_single", |b| {
        b.to_async(&rt).iter(|| async {
            <StreamBus<MemoryStreamBackend> as Publisher>::publish(
                &bus,
                message("u"),
                PublishOptions::default(),
            )
            .await
            .unwrap();
        });
    });
}

fn bench_publish_batch_100(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend, StreamBusOptions::default()).unwrap();
    c.bench_function("publish_batch_100", |b| {
        b.to_async(&rt).iter(|| async {
            let msgs: Vec<Message> = (0..100).map(|i| message(&i.to_string())).collect();
            <StreamBus<MemoryStreamBackend> as Publisher>::publish_batch(
                &bus,
                msgs,
                PublishOptions::default(),
            )
            .await
            .unwrap();
        });
    });
}

criterion_group!(benches, bench_publish_single, bench_publish_batch_100);
criterion_main!(benches);
