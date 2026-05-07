# eventbus-core

[![Crates.io](https://img.shields.io/crates/v/eventbus-core.svg)](https://crates.io/crates/eventbus-core)
[![Docs.rs](https://docs.rs/eventbus-core/badge.svg)](https://docs.rs/eventbus-core)

Object-safe event-bus contract: traits, value types, and the generic
`StreamBus<B: StreamBackend>`. The contract layer of the
[`eventbus-contract`](https://crates.io/crates/eventbus-contract) facade.

Most users should depend on the facade crate instead — it bundles backends
and helpers behind feature flags. Depend on `eventbus-core` directly only
when you are building a custom `StreamBackend`, a custom `Handler` library,
or otherwise want the trait surface without any backend impl.

## What's here

- **Object-safe traits**: `Bus`, `Publisher`, `Subscriber`, `Handler`,
  `Subscription`, `Delivery`, `DeliveryControl`, `DeliveryHandle`,
  `StreamBackend`, `Codec`. Methods return `BoxFuture<'_, ...>` so any
  combination of `Arc<dyn Bus>` / `Box<dyn Handler>` works for runtime
  dispatch.
- **Monomorphic shortcuts**: `PublisherExt::publish_iter`,
  `SubscriberExt::subscribe_with` for callers that prefer static dispatch.
- **Value types**: `Message`, `PublishOptions`, `SubscriptionConfig` (+ builder),
  `BatchOutcome` / `BatchError`, `EventBusError`.
- **Newtype identifiers**: `Topic`, `ConsumerGroup`, `ConsumerName`,
  `MessageId` — `#[repr(transparent)]`, parse-don't-validate.
- **Contract enums**: `DeliveryGuarantee`, `AckMode`, `OrderingMode`,
  `ConsumerBalanceMode`, `OverflowStrategy`, `BackpressurePolicy`.
- **Generic `StreamBus<B>`** + the in-process bus plumbing
  (`stream::{StreamBus, StreamBusOptions, ErrorObserver, ErrorScope}`).

## Object-safe contract

```rust
use std::sync::Arc;
use eventbus_core::{
    BoxFuture, DeliveryHandle, EventBusError, Handler,
};

pub struct MyHandler;

impl Handler for MyHandler {
    fn handle(
        &self,
        delivery: Box<dyn DeliveryHandle>,
    ) -> BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move {
            // delivery.message() / delivery.state()
            delivery.ack().await
        })
    }
}

# fn dyn_dispatch(_: Arc<dyn Handler>) {}
# dyn_dispatch(Arc::new(MyHandler));
```

`DeliveryControl::ack/nack/retry` consume `Box<Self>` — the type system
enforces "finalize at most once" at compile time.

## Backends live in sibling crates

| Crate | What it provides |
|---|---|
| [`eventbus-memory`](https://crates.io/crates/eventbus-memory) | In-process `StreamBackend` for tests + dev |
| [`eventbus-redis`](https://crates.io/crates/eventbus-redis) | Redis Streams `StreamBackend` + `JsonCodec` |
| [`eventbus-outbox`](https://crates.io/crates/eventbus-outbox) | `OutboxStore` + `Dispatcher` + `DeadLetterStore` traits |
| [`eventbus-integration`](https://crates.io/crates/eventbus-integration) | DDD `IntegrationEvent` + `MessageFactory` helpers |
| [`eventbus-contract`](https://crates.io/crates/eventbus-contract) | Facade re-exporting all of the above behind feature flags |

## Design notes

- Every public future is `Send`; every public trait is `dyn`-safe.
- Handler errors don't auto-retry under `AckMode::Manual` — the handler
  owns the finalize decision. `AutoOnHandlerSuccess` is the opt-in
  auto-retry-on-error mode.
- `publish_batch` returns `BatchOutcome` with one `Result<MessageId, _>`
  per input — never fail-fast.
- See [`MIGRATION-0.2.md`](https://github.com/zwishing/eventbus-contract/blob/main/MIGRATION-0.2.md)
  if you're upgrading from `eventbus-contract` 0.1.

## License

MIT
