//! Subscription-time validation: builder rejects bad configs and the bus
//! rejects unsupported balance modes.
//!
//! NOTE: a proptest-based round-trip for the AckFlusher is deferred to 0.3
//! (we have not pulled in `proptest` as a dev-dep yet).

use std::sync::Arc;

use eventbus_core::stream::{MemoryStreamBackend, StreamBus, StreamBusOptions};
use eventbus_core::{
    BackpressurePolicy, BoxFuture, ConsumerBalanceMode, ConsumerGroup, DeliveryHandle,
    EventBusError, Handler, OverflowStrategy, SubscriberExt, SubscriptionConfig, Topic,
};

#[test]
fn build_rejects_when_dependent_field_invalid() {
    // backpressure with invalid policy: max_pending_acks < max_in_flight
    let bp = BackpressurePolicy {
        max_in_flight: 4,
        max_pending_acks: 2,
        max_batch_size: 1,
        overflow_strategy: OverflowStrategy::Reject,
    };
    let res = SubscriptionConfig::builder(
        Topic::new("evt.x").unwrap(),
        ConsumerGroup::new("cg.x").unwrap(),
    )
    .backpressure(bp)
    .build();
    assert!(
        res.is_err(),
        "invalid backpressure policy must be rejected at build()"
    );
}

#[tokio::test]
async fn fan_out_subscribe_is_rejected() {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend, StreamBusOptions::default()).expect("bus");

    struct Noop;
    impl Handler for Noop {
        fn handle(&self, _d: Box<dyn DeliveryHandle>) -> BoxFuture<'_, Result<(), EventBusError>> {
            Box::pin(async { Ok(()) })
        }
    }

    let cfg = SubscriptionConfig::builder(
        Topic::new("evt.fanout").unwrap(),
        ConsumerGroup::new("cg.fanout").unwrap(),
    )
    .balance(ConsumerBalanceMode::FanOut)
    .build()
    .expect("FanOut is build-valid; bus rejects at subscribe time");

    let res = bus.subscribe_with(cfg, Noop).await;
    assert!(res.is_err(), "FanOut should be rejected at subscribe time");
}
