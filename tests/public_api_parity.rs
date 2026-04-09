use chrono::Utc;
use eventbus_contract::{
    AckMode, BackpressurePolicy, ConsumerBalanceMode, DeadLetterDecision, DeadLetterPolicy,
    DeadLetterReason, Delivery, DeliveryGuarantee, DeliveryInspector, DeliveryOutcome,
    DeliveryState, DispatcherConfig, GuaranteeMatrix, Headers, IdempotencyClaim,
    IdempotencyClaimStore, Message, OrderingMode, OverflowStrategy, PublishConfirmation,
    SchemaDescriptor, SubscriptionSemantics, Topic, TraceContext, HEADER_BAGGAGE,
    HEADER_CONTENT_TYPE, HEADER_EVENT_VERSION, HEADER_IDEMPOTENCY_KEY, HEADER_TRACE_PARENT,
    HEADER_TRACE_STATE,
};

#[test]
fn root_exports_go_parity_contracts() {
    let _topic: Topic = "orders.created".to_string();
    let _headers = Headers::default();

    let matrix = GuaranteeMatrix {
        publish: DeliveryGuarantee::AtLeastOnce,
        consume: DeliveryGuarantee::AtLeastOnce,
        confirmation: PublishConfirmation::Accepted,
    };
    assert!(matrix.validate().is_ok());

    let backpressure = BackpressurePolicy {
        max_in_flight: 8,
        max_pending_acks: 16,
        max_batch_size: 4,
        overflow_strategy: OverflowStrategy::Reject,
    };
    assert!(backpressure.validate().is_ok());

    let semantics = SubscriptionSemantics {
        ack_mode: AckMode::Manual,
        ordering_mode: OrderingMode::Key,
        balance_mode: ConsumerBalanceMode::Competing,
        wildcard_topic: false,
        require_ordered_key: true,
    };
    assert!(semantics.validate().is_ok());

    let policy = DeadLetterPolicy {
        max_retry: 3,
        move_on_expired: true,
        move_on_terminal: true,
    };
    let decision = policy.decide(3, false, None, Utc::now());
    assert_eq!(decision.reason, DeadLetterReason::MaxRetryExceeded);

    let _explicit_decision = DeadLetterDecision {
        move_to_dead: true,
        reason: DeadLetterReason::Terminal,
    };

    let _schema = SchemaDescriptor {
        content_type: "application/json".into(),
        event_version: "v1".into(),
    };
    let _trace = TraceContext {
        trace_parent: Some("00-abc-def-01".into()),
        trace_state: Some("vendor=value".into()),
        baggage: Some("foo=bar".into()),
        trace_uid: Some("trace-uid".into()),
        correlation_uid: Some("corr-uid".into()),
    };

    assert_eq!(HEADER_CONTENT_TYPE, "content-type");
    assert_eq!(HEADER_EVENT_VERSION, "event-version");
    assert_eq!(HEADER_TRACE_PARENT, "traceparent");
    assert_eq!(HEADER_TRACE_STATE, "tracestate");
    assert_eq!(HEADER_BAGGAGE, "baggage");
    assert_eq!(HEADER_IDEMPOTENCY_KEY, "idempotency-key");
}

#[test]
fn delivery_contract_matches_go_shape() {
    let state = DeliveryState {
        attempt: 2,
        max_attempt: 5,
        first_received: Utc::now(),
        last_received: Utc::now(),
        redelivered: true,
    };

    assert_eq!(state.attempt, 2);
    assert_eq!(state.max_attempt, 5);
    assert!(state.redelivered);

    let outcomes = [
        DeliveryOutcome::Acked,
        DeliveryOutcome::Nacked,
        DeliveryOutcome::Retried,
        DeliveryOutcome::DeadLetter,
    ];
    assert_eq!(outcomes.len(), 4);
}

#[test]
fn root_exports_idempotency_claim_contracts() {
    fn assert_claim_store<T: IdempotencyClaimStore>() {}

    let _claim = IdempotencyClaim {
        consumer_group: "orders".into(),
        message_uid: "msg-1".into(),
        idempotency_key: "idem-1".into(),
        claimed_at: Utc::now(),
        lease_expire_at: None,
        processor_name: "worker-1".into(),
    };

    struct NoopClaimStore;

    impl IdempotencyClaimStore for NoopClaimStore {
        async fn claim(
            &self,
            _claim: IdempotencyClaim,
        ) -> Result<bool, eventbus_contract::EventBusError> {
            Ok(true)
        }

        async fn complete(
            &self,
            _consumer_group: &str,
            _message_uid: &str,
        ) -> Result<(), eventbus_contract::EventBusError> {
            Ok(())
        }

        async fn release(
            &self,
            _consumer_group: &str,
            _message_uid: &str,
        ) -> Result<(), eventbus_contract::EventBusError> {
            Ok(())
        }
    }

    assert_claim_store::<NoopClaimStore>();
}

#[test]
fn dispatcher_defaults_match_go_shared_defaults() {
    let cfg = DispatcherConfig::default();

    assert_eq!(cfg.max_in_flight, 256);
    assert_eq!(cfg.max_pending_acks, 512);
}

#[test]
fn delivery_trait_exposes_delivery_inspection() {
    fn require_delivery_inspector<T: DeliveryInspector>(_delivery: &T) {}

    fn assert_delivery_has_inspector<T: Delivery>(delivery: &T) {
        require_delivery_inspector(delivery);
    }

    struct DeliveryWithInspection;

    impl DeliveryInspector for DeliveryWithInspection {
        async fn state(&self) -> Result<DeliveryState, eventbus_contract::EventBusError> {
            Ok(DeliveryState {
                attempt: 1,
                max_attempt: 3,
                first_received: Utc::now(),
                last_received: Utc::now(),
                redelivered: false,
            })
        }
    }

    impl Delivery for DeliveryWithInspection {
        fn message(&self) -> &Message {
            panic!("test helper does not expose a message reference")
        }

        async fn ack(&self) -> Result<(), eventbus_contract::EventBusError> {
            Ok(())
        }

        async fn nack(
            &self,
            _reason: &(dyn std::error::Error + Send + Sync),
        ) -> Result<(), eventbus_contract::EventBusError> {
            Ok(())
        }

        async fn retry(
            &self,
            _reason: &(dyn std::error::Error + Send + Sync),
        ) -> Result<(), eventbus_contract::EventBusError> {
            Ok(())
        }
    }

    assert_delivery_has_inspector(&DeliveryWithInspection);
}

#[allow(dead_code)]
fn assert_delivery_inspector<T: DeliveryInspector>() {}

#[allow(dead_code)]
fn message_with_headers(headers: Headers) -> Message {
    Message {
        uid: "msg-1".into(),
        topic: "orders.created".into(),
        key: "order-1".into(),
        kind: "orders.created".into(),
        source: "tests".into(),
        occurred_at: Utc::now(),
        headers,
        payload: vec![],
        content_type: None,
        event_version: None,
        idempotency_key: None,
        expires_at: None,
        trace_uid: None,
        correlation_uid: None,
    }
}
