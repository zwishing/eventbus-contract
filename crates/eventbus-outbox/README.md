# eventbus-outbox

[![Crates.io](https://img.shields.io/crates/v/eventbus-outbox.svg)](https://crates.io/crates/eventbus-outbox)
[![Docs.rs](https://docs.rs/eventbus-outbox/badge.svg)](https://docs.rs/eventbus-outbox)

Transactional outbox + dispatcher + dead-letter + idempotency trait surface for the [eventbus-contract](https://crates.io/crates/eventbus-contract) workspace.

The crate provides the **trait surface** for implementing the outbox pattern on top of a database, plus a dispatcher that drives the relay loop. The traits depend only on `eventbus-core` types — no impls are bundled.

## What's in the crate

| Trait | Purpose |
|---|---|
| `OutboxStore` | Append + lock + mark-sent / failed / dead for the `Pending → Processing → Sent / Failed / Dead` state machine |
| `StateTransitionStore` | Validated state transitions with audit-friendly metadata |
| `DeadLetterStore` | Append exhausted-retry messages |
| `IdempotencyStore` / `IdempotencyClaimStore` | Cross-cutting dedup + lease-based claim |
| `Dispatcher` / `Notifier` / `Listener` | Outbox-relay worker traits |

Records: `AppendRequest`, `OutboxMessageRecord`, `DeadLetterMessageRecord`, `ConsumerMessageRecord`, `IdempotencyClaim`, `TransitionInput`. Helper enums: `OutboxStatus`, `DeadLetterDecision`, `DeadLetterPolicy`, `DeadLetterReason`.

## Use via the facade

```toml
[dependencies]
eventbus-contract = { version = "0.2", features = ["outbox"] }
```

```rust
use eventbus_contract::outbox::{OutboxStore, OutboxStatus, AppendRequest};
```

Or directly:

```toml
[dependencies]
eventbus-core = "0.2"
eventbus-outbox = "0.2"
```

## Implementing a backend

```rust,ignore
use eventbus_outbox::{OutboxStore, AppendRequest, OutboxMessageRecord};
use eventbus_core::EventBusError;

struct PostgresOutbox { /* ... */ }

impl OutboxStore for PostgresOutbox {
    async fn append(&self, req: AppendRequest) -> Result<(), EventBusError> { /* ... */ }
    async fn append_batch(&self, reqs: Vec<AppendRequest>) -> Result<(), EventBusError> { /* ... */ }
    async fn lock_pending(&self, /* ... */) -> Result<Vec<OutboxMessageRecord>, EventBusError> { /* ... */ }
    /* ... */
}
```

Payloads are `bytes::Bytes` (aligned with `Message::payload`).

## License

MIT
