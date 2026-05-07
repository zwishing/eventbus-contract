# Changelog

## 0.2.0 — 2026-05-07

Breaking refactor. See [`MIGRATION-0.2.md`](./MIGRATION-0.2.md).

### Architecture

- Workspace split: `eventbus-core` + `eventbus-memory` + `eventbus-redis`
  + `eventbus-outbox` + `eventbus-integration` + `eventbus-contract` (facade).
- Single-crate users should depend on `eventbus-contract` and pick backends via
  feature flags (`memory`, `redis`, `outbox`, `integration`, `tracing`).

### Public API — breaking

- `Publisher` / `Subscriber` / `Handler` / `Subscription` / `Delivery` /
  `DeliveryInspector` are now object-safe (return `BoxFuture`).
- `PublisherExt::publish_iter` and `SubscriberExt::subscribe_with` provide
  monomorphic shortcuts for code that does not need `dyn`.
- `Delivery` is read-only; new `DeliveryControl` consumes `Box<Self>` for
  `ack` / `nack` / `retry`. `Handler::handle` now receives
  `Box<dyn DeliveryHandle>`.
- `nack` / `retry` reason changed from `&dyn Error` to `BoxedError`.
- New newtype IDs: `Topic`, `ConsumerGroup`, `ConsumerName`, `MessageId`
  — `#[repr(transparent)]`, parse-don't-validate.
- `SubscriptionConfig` fields are private; build via
  `SubscriptionConfig::builder(Topic, ConsumerGroup).build()?`.
- `publish_batch` returns `Ok(BatchOutcome)` with one `Result` per input
  — no fail-fast.
- `AppendRequest` / `OutboxMessageRecord` / `DeadLetterMessageRecord` payload
  is now `bytes::Bytes`.
- `ErrorScope` adds `Drop` and `HandlerPanic`; `ErrorObserver` adds
  `on_panic`.
- `StreamSubscription::abort()` — immediate abort without graceful drain.

### Correctness

- Limiter race fixed: `consume_loop` now acquires permits before fetching
  from the backend so the in-flight cap is never exceeded.
- `EventBusError::source(context, inner)` replaces ad-hoc
  `Internal(format!())` at sites with a causal chain.
- Subscription `Drop` fires `on_error(ErrorScope::Drop, …)` exactly once
  when the subscription was not closed gracefully.
- Per-entry `FetchedEntry::Malformed` reporting prevents poison-pill loops
  on undecodable PEL entries — the bus acks, surfaces to the observer,
  and routes a synthetic envelope to the dead-letter topic.

### Observability

- New `tracing` feature instruments `publish`, `subscribe`, `consume_loop`,
  and `process_single_message`.
- New tests: `crates/eventbus-core/tests/observer.rs`,
  `finalize_typesafe.rs`, `poison_pill.rs`, `subscribe_validation.rs`,
  plus the `dyn_safety` compile-time witness.
- New Criterion bench `crates/eventbus-core/benches/publish.rs` —
  `cargo bench -p eventbus-core --no-run` compiles clean.

## 0.1.x

See git history for `eventbus-contract` 0.1.
