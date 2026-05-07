# eventbus-contract

[![Crates.io](https://img.shields.io/crates/v/eventbus-contract.svg)](https://crates.io/crates/eventbus-contract)
[![Docs.rs](https://docs.rs/eventbus-contract/badge.svg)](https://docs.rs/eventbus-contract)

Object-safe event-bus contract for Rust, with a Redis Streams backend, an in-process backend for tests, plus optional outbox + DDD integration-event helpers — all behind feature flags.

This is the **facade crate**. It re-exports the contract traits from [`eventbus-core`](https://crates.io/crates/eventbus-core) plus the chosen backends and helpers.

## Quickstart

```toml
[dependencies]
eventbus-contract = { version = "0.2", features = ["redis", "outbox"] }
```

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
    )
    .ack_mode(AckMode::AutoOnHandlerSuccess)
    .max_in_flight(8)
    .build()?;

    let _sub = bus.subscribe_with(cfg, Echo).await?;
    Ok(())
}
```

## Features

| Feature | Default | What it pulls in |
|---|---|---|
| `memory` | yes | `eventbus-memory` (in-process backend) |
| `redis` | no | `eventbus-redis` (Redis Streams) |
| `outbox` | no | `eventbus-outbox` (transactional outbox + dispatcher) |
| `integration` | no | `eventbus-integration` (DDD integration events) |
| `tracing` | no | `tracing` instrumentation on hot paths |

## Workspace

| Crate | Purpose |
|---|---|
| [`eventbus-core`](https://crates.io/crates/eventbus-core) | Contract traits + value types + generic `StreamBus` |
| [`eventbus-memory`](https://crates.io/crates/eventbus-memory) | In-process `StreamBackend` |
| [`eventbus-redis`](https://crates.io/crates/eventbus-redis) | Redis Streams `StreamBackend` + `JsonCodec` |
| [`eventbus-outbox`](https://crates.io/crates/eventbus-outbox) | Outbox + dispatcher traits |
| [`eventbus-integration`](https://crates.io/crates/eventbus-integration) | DDD integration-event surface |
| `eventbus-contract` (this crate) | Facade re-exporting all of the above |

## Migrating from 0.1

See [MIGRATION-0.2.md](https://github.com/zwishing/eventbus-contract/blob/main/MIGRATION-0.2.md). 0.2.0 is a breaking refactor: object-safe trait surface, `Delivery + DeliveryControl` split with consume-self ack, newtype IDs, `SubscriptionConfig` builder, `BatchOutcome` per-message results, and the workspace split.

## License

MIT
