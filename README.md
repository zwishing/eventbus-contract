# eventbus-contract

Object-safe event-bus contract for Rust, with a Redis Streams backend and an
in-process backend for tests.

```toml
[dependencies]
eventbus-contract = { version = "0.2", features = ["redis", "outbox"] }
```

```rust
use eventbus_contract::prelude::*;
```

## Workspace layout

| Crate | What it provides |
|---|---|
| `eventbus-core` | Object-safe traits (`Publisher`, `Subscriber`, `Handler`, `Delivery`, `DeliveryControl`), value types, the generic `StreamBus`, and the in-process `MemoryStreamBackend`. |
| `eventbus-memory` | Re-exports `MemoryStreamBackend` behind the default-on `test-utils` feature. |
| `eventbus-redis` | Production `RedisBackend` over `XADD` / `XREADGROUP` / `XACK` / `XAUTOCLAIM`. |
| `eventbus-outbox` | Transactional outbox + dispatcher + dead-letter store traits. |
| `eventbus-integration` | DDD integration-event helpers (`IntegrationEvent`, `MessageFactory`, `EventPublisher`). |
| `eventbus-contract` | Facade — re-exports everything behind feature flags. |

## Features (facade)

- `memory` (default) — in-process backend for tests / dev.
- `redis` — Redis Streams backend.
- `outbox` — outbox + dispatcher helpers.
- `integration` — DDD integration-event surface.
- `tracing` — `tracing` instrumentation on hot paths.

## Quickstart

```rust
use std::sync::Arc;

use eventbus_contract::core::stream::{StreamBus, StreamBusOptions};
use eventbus_contract::memory::MemoryStreamBackend;
use eventbus_contract::prelude::*;

#[tokio::main]
async fn main() -> Result<(), EventBusError> {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend, StreamBusOptions::default())?;

    struct Echo;
    impl Handler for Echo {
        fn handle(&self, d: Box<dyn DeliveryHandle>)
            -> BoxFuture<'_, Result<(), EventBusError>>
        {
            Box::pin(async move { d.ack().await })
        }
    }

    let cfg = SubscriptionConfig::builder(
        Topic::new("orders")?,
        ConsumerGroup::new("svc-a")?,
    ).max_in_flight(8).build()?;

    let _sub = bus.subscribe(cfg, Echo).await?;
    Ok(())
}
```

## Migrating from `eventbus-contract` 0.1

See [`MIGRATION-0.2.md`](./MIGRATION-0.2.md). Highlights: crate rename,
`Handler` takes `Box<dyn DeliveryHandle>`, `Delivery`/`DeliveryControl` split,
`ack`/`nack`/`retry` consume `Box<Self>`, newtype IDs, builder-driven
`SubscriptionConfig`, `BatchOutcome` from `publish_batch`.

## Changelog

See [`CHANGELOG.md`](./CHANGELOG.md).

## License

MIT.
