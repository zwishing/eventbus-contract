# Migrating from `eventbus-contract` 0.1 to `eventbus-contract` 0.2

## Workspace split

The `eventbus-contract` crate is now a thin facade re-exporting from a
workspace of contract + backend + helper crates. The crate name is
unchanged; the version bump is `0.1 → 0.2` (breaking).

```toml
# Before (0.1)
[dependencies]
eventbus-contract = "0.1"

# After (0.2) — facade crate
[dependencies]
eventbus-contract = { version = "0.2", features = ["redis"] }
```

```rust
// Before
use eventbus_contract::{Bus, Publisher, /* ... */};

// After
use eventbus_contract::prelude::*;
```

The single `eventbus-contract` crate is now a workspace:

| Crate | Purpose |
|---|---|
| `eventbus-core` | Object-safe traits + value types + the `StreamBus` |
| `eventbus-memory` | In-process backend, default-on under the facade |
| `eventbus-redis` | Redis Streams backend |
| `eventbus-outbox` | Transactional outbox + dispatcher helpers (workspace-only in 0.2.0; awaiting reference impl, returns in 0.3.0) |
| `eventbus-integration` | DDD integration-event surface (workspace-only in 0.2.0; returns in 0.3.0) |
| `eventbus-contract` | Facade — re-exports the above behind feature flags |

## Handler signature

```rust
// Before — generic, &dyn Delivery
impl Handler for MyHandler {
    fn handle<D: Delivery + Send + Sync>(&self, d: &D) -> impl Future<Output = Result<(), EventBusError>> { /* ... */ }
}

// After — receives Box<dyn DeliveryHandle>, returns BoxFuture
impl Handler for MyHandler {
    fn handle(&self, d: Box<dyn DeliveryHandle>)
        -> BoxFuture<'_, Result<(), EventBusError>>
    {
        Box::pin(async move {
            // d.message(), d.state(), d.ack().await?, d.nack(reason).await?, ...
            Ok(())
        })
    }
}
```

## ack / nack / retry consume `Box<Self>`

```rust
// Before — could (incorrectly) call ack twice; runtime AtomicBool guard.
delivery.ack().await?;
// delivery.nack(reason).await?; // 0.1 silently ignored

// After — Box<Self> consumed; second call won't compile.
delivery.ack().await?;
// delivery.nack(reason).await?; // E0382: use of moved value
```

The compiler enforces finalize-at-most-once. The runtime [`AutoFinalizeTracker`] still
guarantees that a delivery dropped without an explicit ack/nack/retry is finalized
exactly once.

## `nack` / `retry` reason: `BoxedError`, not `&dyn Error`

```rust
// Before
delivery.nack(&my_error).await?;

// After
delivery.nack(Box::new(my_error)).await?;

// Or, with the BoxedError alias:
let reason: BoxedError = Box::new(EventBusError::Internal("explanation".into()));
delivery.retry(reason).await?;
```

## `Topic` / `ConsumerGroup` / `ConsumerName` / `MessageId` — newtypes

```rust
// Before — stringly-typed config
let cfg = SubscriptionConfig {
    topic: "orders".to_string(),
    consumer_group: "svc-a".to_string(),
    consumer_name: "node-1".to_string(),
    ..Default::default()
};

// After — parse-don't-validate at construction, builder for the rest
let cfg = SubscriptionConfig::builder(
    Topic::new("orders")?,
    ConsumerGroup::new("svc-a")?,
)
.consumer_name(ConsumerName::new("node-1")?)
.max_in_flight(8)
.build()?;
```

All four newtypes are `#[repr(transparent)]` over `String`.

## `publish_batch` returns `BatchOutcome`

```rust
// Before — fail-fast on first error.
bus.publish_batch(msgs.into_iter(), opts).await?;

// After — every input gets a slot.
let outcome = bus.publish_batch(msgs, opts).await?;
if !outcome.all_ok() {
    for (idx, result) in outcome.results.iter().enumerate() {
        if let Err(err) = result {
            tracing::warn!(idx, %err, "publish failed");
        }
    }
}
```

Iterator-friendly form via the monomorphic extension trait:

```rust
use eventbus_contract::prelude::*;
use eventbus_core::PublisherExt;
let outcome = bus.publish_iter(msgs_iter, opts).await?;
```

## Memory backend feature gate

```toml
# Default-on under the eventbus facade
eventbus-contract = { version = "0.2" }

# To exclude the in-process backend (typical production)
eventbus-contract = { version = "0.2", default-features = false, features = ["redis"] }
```

## Subscription lifecycle

`Subscription::close` consumes `Arc<Self>` (object-safe). Concrete `StreamSubscription`
also exposes a `&self` form and a new `abort()`:

```rust
// dyn Subscription
Arc::clone(&sub).close().await?;

// Concrete StreamSubscription
sub.close().await?;          // graceful drain
sub.abort().await?;          // immediate abort, no drain
```

## Error chain

`EventBusError::source(context, inner)` is the canonical way to wrap a
caused-by error. Library code (and any custom backends) should prefer this
over `EventBusError::Internal(format!("...: {err}"))` so consumers can walk
`std::error::Error::source()`.

## Observability

```rust
// New scopes
ErrorScope::Drop          // subscription dropped without close
ErrorScope::HandlerPanic  // delivery task panicked

// New observer hook (default empty)
trait ErrorObserver {
    fn on_panic(&self, scope: ErrorScope, payload: &str) {}
}
```

```toml
# Tracing instrumentation
eventbus-contract = { version = "0.2", features = ["tracing"] }
```
