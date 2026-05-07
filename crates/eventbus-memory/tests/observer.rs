//! Observer hook tests for PR 5: Drop scope, HandlerPanic, BatchOutcome.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use eventbus_core::stream::{ErrorObserver, ErrorScope, StreamBus, StreamBusOptions};
use eventbus_core::{
    BoxFuture, ConsumerGroup, DeliveryHandle, EventBusError, Handler, Headers, Message,
    PublishOptions, SubscriptionConfig, Topic,
};
use eventbus_memory::MemoryStreamBackend;
use tokio::time::sleep;

#[derive(Default)]
struct CapturingObserver {
    errors: Mutex<Vec<(ErrorScope, String)>>,
    panics: Mutex<Vec<(ErrorScope, String)>>,
}

impl ErrorObserver for CapturingObserver {
    fn on_error(&self, scope: ErrorScope, err: &EventBusError) {
        self.errors.lock().unwrap().push((scope, err.to_string()));
    }
    fn on_panic(&self, scope: ErrorScope, payload: &str) {
        self.panics
            .lock()
            .unwrap()
            .push((scope, payload.to_string()));
    }
}

fn message(topic: &str, uid: &str) -> Message {
    Message {
        uid: uid.to_string(),
        topic: Topic::new(topic).expect("topic"),
        key: String::new(),
        kind: "test.message".to_string(),
        source: "test".to_string(),
        occurred_at: Utc::now(),
        headers: Headers::new(),
        payload: bytes::Bytes::from_static(br#"{"ok":true}"#),
        content_type: None,
        event_version: None,
        idempotency_key: None,
        expires_at: None,
        trace_uid: None,
        correlation_uid: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_without_close_fires_on_error_with_drop_scope() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let observer = Arc::new(CapturingObserver::default());
    let bus = StreamBus::new(
        backend.clone(),
        StreamBusOptions::default().with_error_observer(observer.clone() as Arc<dyn ErrorObserver>),
    )
    .expect("bus");

    struct Noop;
    impl Handler for Noop {
        fn handle(&self, _d: Box<dyn DeliveryHandle>) -> BoxFuture<'_, Result<(), EventBusError>> {
            Box::pin(async { Ok(()) })
        }
    }

    let cfg = SubscriptionConfig::builder(
        Topic::new("evt.drop").unwrap(),
        ConsumerGroup::new("cg.drop").unwrap(),
    )
    .max_in_flight(1)
    .build()
    .unwrap();
    let sub = bus.subscribe(cfg, Noop).await.expect("subscribe");
    drop(sub);

    // Give the runtime a moment to run the Drop impl.
    sleep(Duration::from_millis(20)).await;

    let errors = observer.errors.lock().unwrap();
    assert!(
        errors.iter().any(|(scope, _)| *scope == ErrorScope::Drop),
        "expected at least one Drop scope error, got {:?}",
        errors
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_panic_fires_on_panic() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let observer = Arc::new(CapturingObserver::default());
    let bus = StreamBus::new(
        backend.clone(),
        StreamBusOptions::default().with_error_observer(observer.clone() as Arc<dyn ErrorObserver>),
    )
    .expect("bus");

    struct Boom;
    impl Handler for Boom {
        fn handle(&self, _d: Box<dyn DeliveryHandle>) -> BoxFuture<'_, Result<(), EventBusError>> {
            Box::pin(async {
                panic!("boom");
            })
        }
    }

    let cfg = SubscriptionConfig::builder(
        Topic::new("evt.panic").unwrap(),
        ConsumerGroup::new("cg.panic").unwrap(),
    )
    .max_in_flight(1)
    .max_retry(0)
    .dead_letter_topic(Topic::new("evt.panic.dlq").unwrap())
    .build()
    .unwrap();
    let sub = bus.subscribe(cfg, Boom).await.expect("subscribe");

    bus.publish(message("evt.panic", "uid-panic"), PublishOptions::default())
        .await
        .expect("publish");

    // Wait for the panic to propagate and the consume loop to record it.
    sleep(Duration::from_millis(300)).await;

    let _ = sub.abort().await;

    let panics = observer.panics.lock().unwrap();
    assert!(
        !panics.is_empty(),
        "expected at least one panic event, got {:?}",
        panics
    );
    assert!(panics
        .iter()
        .all(|(scope, _)| *scope == ErrorScope::HandlerPanic));
}

#[tokio::test]
async fn publish_batch_returns_per_message_results() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend.clone(), StreamBusOptions::default()).expect("bus");

    let max = StreamBusOptions::default().max_payload_bytes;

    let good_a = message("evt.batch", "uid-A");
    let bad = Message {
        payload: bytes::Bytes::from(vec![0u8; max + 1]),
        ..message("evt.batch", "uid-B")
    };
    let good_c = message("evt.batch", "uid-C");

    let outcome = bus
        .publish_batch(vec![good_a, bad, good_c], PublishOptions::default())
        .await
        .expect("batch outcome");

    assert_eq!(outcome.results.len(), 3);
    assert!(outcome.results[0].is_ok(), "first should succeed");
    assert!(outcome.results[1].is_err(), "oversized should fail");
    assert!(outcome.results[2].is_ok(), "third should succeed");
    assert_eq!(outcome.ok_count(), 2);
    assert_eq!(outcome.err_count(), 1);
}
