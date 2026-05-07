//! Two concurrent subscribers on different consumer groups each receive
//! every published message independently. Spec §6 #12.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use eventbus_core::stream::{StreamBus, StreamBusOptions};
use eventbus_core::{
    AckMode, BoxFuture, ConsumerGroup, DeliveryHandle, EventBusError, Handler, Headers, Message,
    PublishOptions, SubscriberExt, SubscriptionConfig, Topic,
};
use eventbus_memory::MemoryStreamBackend;
use tokio::sync::mpsc;
use tokio::time::timeout;

fn message(topic: &str, uid: &str) -> Message {
    Message {
        uid: uid.to_string(),
        topic: Topic::new(topic).unwrap(),
        key: String::new(),
        kind: "test".into(),
        source: "test".into(),
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

struct Tap {
    tx: mpsc::Sender<String>,
}

impl Handler for Tap {
    fn handle(&self, d: Box<dyn DeliveryHandle>) -> BoxFuture<'_, Result<(), EventBusError>> {
        let tx = self.tx.clone();
        Box::pin(async move {
            let uid = d.message().uid.clone();
            let _ = tx.send(uid).await;
            d.ack().await
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_groups_each_receive_each_message() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend, StreamBusOptions::default()).expect("bus");

    let (tx_a, mut rx_a) = mpsc::channel(8);
    let (tx_b, mut rx_b) = mpsc::channel(8);

    let cfg_a = SubscriptionConfig::builder(
        Topic::new("evt.parallel").unwrap(),
        ConsumerGroup::new("group-a").unwrap(),
    )
    .ack_mode(AckMode::Manual)
    .max_in_flight(1)
    .build()
    .unwrap();
    let cfg_b = SubscriptionConfig::builder(
        Topic::new("evt.parallel").unwrap(),
        ConsumerGroup::new("group-b").unwrap(),
    )
    .ack_mode(AckMode::Manual)
    .max_in_flight(1)
    .build()
    .unwrap();

    let sub_a = bus
        .subscribe_with(cfg_a, Tap { tx: tx_a })
        .await
        .expect("sub a");
    let sub_b = bus
        .subscribe_with(cfg_b, Tap { tx: tx_b })
        .await
        .expect("sub b");

    bus.publish(message("evt.parallel", "uid-1"), PublishOptions::default())
        .await
        .unwrap();
    bus.publish(message("evt.parallel", "uid-2"), PublishOptions::default())
        .await
        .unwrap();

    let a1 = timeout(Duration::from_secs(2), rx_a.recv())
        .await
        .unwrap()
        .unwrap();
    let a2 = timeout(Duration::from_secs(2), rx_a.recv())
        .await
        .unwrap()
        .unwrap();
    let b1 = timeout(Duration::from_secs(2), rx_b.recv())
        .await
        .unwrap()
        .unwrap();
    let b2 = timeout(Duration::from_secs(2), rx_b.recv())
        .await
        .unwrap()
        .unwrap();

    let mut a = vec![a1, a2];
    a.sort();
    let mut b = vec![b1, b2];
    b.sort();
    assert_eq!(a, vec!["uid-1".to_string(), "uid-2".to_string()]);
    assert_eq!(b, vec!["uid-1".to_string(), "uid-2".to_string()]);

    sub_a.close().await.expect("close a");
    sub_b.close().await.expect("close b");
}
