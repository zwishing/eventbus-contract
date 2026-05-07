# eventbus-integration

[![Crates.io](https://img.shields.io/crates/v/eventbus-integration.svg)](https://crates.io/crates/eventbus-integration)
[![Docs.rs](https://docs.rs/eventbus-integration/badge.svg)](https://docs.rs/eventbus-integration)

Thin DDD integration-event surface atop [eventbus-core](https://crates.io/crates/eventbus-core).

Provides three trait abstractions for translating domain integration events into bus messages:

| Trait | Purpose |
|---|---|
| `IntegrationEvent` | Domain event → topic / key / kind metadata |
| `MessageFactory` | Build a `Message` from an `IntegrationEvent` |
| `EventPublisher` | Publish one or many integration events |

## Use via the facade

```toml
[dependencies]
eventbus-contract = { version = "0.2", features = ["integration"] }
```

```rust
use eventbus_contract::integration::{IntegrationEvent, MessageFactory, EventPublisher};
```

Or directly:

```toml
[dependencies]
eventbus-core = "0.2"
eventbus-integration = "0.2"
```

## Implementing an event

```rust,ignore
use eventbus_integration::IntegrationEvent;

pub struct OrderCreated { pub order_uid: String }

impl IntegrationEvent for OrderCreated {
    fn event_topic(&self) -> &str { "orders.created.v1" }
    fn event_key(&self) -> &str { &self.order_uid }
    fn event_kind(&self) -> &str { "OrderCreated" }
}
```

Plug a domain-specific `MessageFactory` (which captures source, schema, headers) and publish through any `Publisher` impl from `eventbus-core` or its backends.

## License

MIT
