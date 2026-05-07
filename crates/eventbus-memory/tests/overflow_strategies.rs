//! Spec §6 #4 — `OverflowStrategy` variants.
//!
//! `BackpressurePolicy::validate()` accepts all four variants today
//! (`Reject`, `Block`, `DropNewest`, `DropOldest`); only `Reject` is wired
//! into the consume-side runtime via `max_in_flight`/`max_pending_acks`.
//! The other variants compile and validate but fall through to the same
//! semaphore-based gating until a backend opts in. This test pins that
//! observable behaviour so a future change either updates the test or
//! wires the variants intentionally.

use std::sync::Arc;

use eventbus_core::stream::{StreamBus, StreamBusOptions};
use eventbus_core::{
    BackpressurePolicy, BoxFuture, ConsumerBalanceMode, ConsumerGroup, DeliveryHandle,
    EventBusError, Handler, OverflowStrategy, SubscriptionConfig, Topic,
};
use eventbus_memory::MemoryStreamBackend;

struct NoopHandler;

impl Handler for NoopHandler {
    fn handle(
        &self,
        _delivery: Box<dyn DeliveryHandle>,
    ) -> BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async { Ok(()) })
    }
}

fn policy(strategy: OverflowStrategy) -> BackpressurePolicy {
    BackpressurePolicy {
        max_in_flight: 4,
        max_pending_acks: 8,
        max_batch_size: 2,
        overflow_strategy: strategy,
    }
}

#[test]
fn each_overflow_variant_passes_validate() {
    for strategy in [
        OverflowStrategy::Reject,
        OverflowStrategy::Block,
        OverflowStrategy::DropNewest,
        OverflowStrategy::DropOldest,
    ] {
        assert!(
            policy(strategy).validate().is_ok(),
            "policy with {strategy:?} should validate"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_overflow_variant_subscribes_via_competing_balance() {
    // Each variant must round-trip a `subscribe()` without surfacing a
    // policy-level rejection. This is the integration-side guarantee that
    // matches the unit-level `validate()` parity above.
    let strategies = [
        OverflowStrategy::Reject,
        OverflowStrategy::Block,
        OverflowStrategy::DropNewest,
        OverflowStrategy::DropOldest,
    ];

    for (i, strategy) in strategies.iter().enumerate() {
        let backend = Arc::new(MemoryStreamBackend::default());
        let bus = StreamBus::new(backend, StreamBusOptions::default()).expect("construct bus");

        let topic = Topic::new(format!("evt.overflow.{i}")).expect("topic");
        let group = ConsumerGroup::new(format!("cg.overflow.{i}")).expect("group");

        let cfg = SubscriptionConfig::builder(topic, group)
            .balance(ConsumerBalanceMode::Competing)
            .backpressure(policy(*strategy))
            .build()
            .expect("build subscription config");

        let sub = bus
            .subscribe(cfg, NoopHandler)
            .await
            .unwrap_or_else(|_| panic!("subscribe should accept {strategy:?}"));
        sub.close().await.expect("close");
    }
}
