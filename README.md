# eventbus-contract

A pure Rust event bus contract library for event-driven messaging. Defines
traits and types for publishers, consumers, and handlers. The transport is
pluggable — swap `MemoryStreamBackend` (tests / local dev) for `RedisBackend`
(production) with no handler code changes.

## Quick start

```toml
[dependencies]
eventbus-contract = { path = "." }

# Optional: real Redis transport
eventbus-contract = { path = ".", features = ["redis-backend"] }
```

```rust
use std::sync::Arc;
use eventbus_contract::redis_stream::{MemoryStreamBackend, RedisStreamBus, RedisStreamBusOptions};
use eventbus_contract::{AckMode, Delivery, EventBusError, Handler, Message, PublishOptions, SubscriptionConfig};

struct MyHandler;

impl Handler for MyHandler {
    async fn handle<D: Delivery + Send + Sync>(&self, delivery: &D) -> Result<(), EventBusError> {
        println!("received: {}", delivery.message().uid);
        delivery.ack().await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = RedisStreamBus::new(backend, RedisStreamBusOptions::default())?;

    let sub = bus.subscribe(
        SubscriptionConfig {
            topic: "my.topic".to_string(),
            consumer_group: "my-service".to_string(),
            consumer_name: "worker-1".to_string(),
            ack_mode: AckMode::AutoOnHandlerSuccess,
            concurrency: 1,
            ..Default::default()
        },
        MyHandler,
    ).await?;

    // publish ...

    sub.close().await?;
    Ok(())
}
```

## Delivery control

Inside a `Handler`, you control what happens to each message:

| Call | Meaning |
|------|---------|
| `delivery.ack()` | Processed successfully — remove from pending |
| `delivery.retry(reason)` | Transient failure — republish for redelivery |
| `delivery.nack(reason)` | Permanent failure — route to `dead_letter_topic` |

`ack_mode` on `SubscriptionConfig` can automate this:

| `AckMode` | Behaviour |
|-----------|-----------|
| `Manual` (default) | Handler calls `ack` / `retry` / `nack` explicitly |
| `AutoOnHandlerSuccess` | Bus calls `ack()` when handler returns `Ok(())`, `retry()` on `Err` |
| `AutoOnReceive` | Bus calls `ack()` immediately upon receiving the message |

## Retry and dead-letter

```rust
SubscriptionConfig {
    max_retry: 3,                                          // retry up to 3 times
    dead_letter_topic: Some("my.topic.dlq".to_string()),  // then route here
    ..Default::default()
}
```

- `max_retry = 0` → no retries; first call to `retry()` goes directly to DLQ.
- Without a `dead_letter_topic`, exhausted messages are silently dropped.
- Retry exhaustion is checked against `DeliveryState::max_attempt` (set from
  `SubscriptionConfig::max_retry` by the bus layer), keeping retry logic
  self-contained in the delivery.

## Error handling

Infrastructure errors (e.g. Redis `ACK` failure) do **not** kill the consumer
loop. The unacknowledged message stays in the Redis PEL and will be reclaimed
by the next idle-claim scan. Errors are **collected** and surfaced when
`Subscription::close()` is called:

```rust
let err = sub.close().await;   // Ok(()) on clean shutdown, Err(...) if any delivery failed
```

Task panics are treated differently — they propagate immediately since they
indicate programming bugs.

## Competing consumers (horizontal scaling)

Set `concurrency > 1` to allow multiple handler tasks to process messages
concurrently within the same consumer group. Each message is delivered to
exactly one handler execution.

```rust
SubscriptionConfig {
    consumer_name: "processor".to_string(), // stable consumer identity for this subscriber
    concurrency: 4,
    ..Default::default()
}
```

## Backends

### MemoryStreamBackend (default, no dependencies)

In-process stream. All integration tests use this. Zero setup required.

```rust
let backend = Arc::new(MemoryStreamBackend::default());
let bus = RedisStreamBus::new(backend, RedisStreamBusOptions::default())?;
```

### RedisBackend (`--features redis-backend`)

Backed by Redis Streams. Wire format is JSON-compatible with the Go
`RedisStreamBus`. Each entry is stored as `XADD <topic> * message <json>`.

```rust
let client = redis::Client::open("redis://127.0.0.1/")?;
let conn = client.get_multiplexed_async_connection().await?;

// Convenience constructor
let bus = RedisStreamBus::from_connection(conn, RedisStreamBusOptions::default())?;
```

`RedisStreamBusOptions` tunables:

| Field | Default | Description |
|-------|---------|-------------|
| `block_timeout` | 2 s | How long to block-poll when no new messages arrive |
| `claim_idle_timeout` | 60 s | Reclaim pending messages idle longer than this |
| `claim_scan_batch_size` | 64 | Max messages to reclaim per scan cycle |
| `group_start_id` | `"$"` | Stream offset for new consumer groups (`"0"` = from beginning) |

## Well-known headers

Standard header key constants are exported from the crate root:

| Constant | Header key | Purpose |
|----------|-----------|---------|
| `HEADER_RETRY_ATTEMPT` | `retry-attempt` | Current retry attempt number (set on retry) |
| `HEADER_RETRY_REASON` | `retry-reason` | Reason for retry (set on retry) |
| `HEADER_DEAD_LETTER_REASON` | `dead-letter-reason` | Reason message was dead-lettered |
| `HEADER_IDEMPOTENCY_KEY` | `idempotency-key` | Deduplication key |
| `HEADER_TRACE_PARENT` | `traceparent` | W3C trace parent |
| `HEADER_TRACE_STATE` | `tracestate` | W3C trace state |
| `HEADER_BAGGAGE` | `baggage` | W3C baggage |
| `HEADER_CONTENT_TYPE` | `content-type` | Payload content type |
| `HEADER_EVENT_VERSION` | `event-version` | Schema version |

## Examples

| Example | What it shows |
|---------|---------------|
| [`01_basic_pubsub`](examples/01_basic_pubsub.rs) | Minimal publish + subscribe with automatic ack-on-success |
| [`02_manual_ack_and_retry`](examples/02_manual_ack_and_retry.rs) | Explicit `ack` / `retry` / `nack`, dead-letter routing |
| [`03_competing_consumers`](examples/03_competing_consumers.rs) | Concurrent workers, competing-consumer semantics |
| [`04_redis_backend`](examples/04_redis_backend.rs) | Real Redis connection (`--features redis-backend`) |

```bash
cargo run --example 01_basic_pubsub
cargo run --example 02_manual_ack_and_retry
cargo run --example 03_competing_consumers

# Requires a running Redis and the feature flag
docker run --rm -p 6379:6379 redis:7
cargo run --example 04_redis_backend --features redis-backend
```

## Commands

```bash
cargo build
cargo build --features redis-backend

cargo test
cargo test --features redis-backend
cargo test <test_name>
cargo test -- --nocapture

cargo clippy
cargo fmt
```

## Architecture overview

```
src/
├── eventbus/         Core traits: Publisher, Subscriber, Handler, Delivery, Bus, Codec
├── contract/         Value objects: DeliveryGuarantee, AckMode, BackpressurePolicy, …
├── redis_stream/     RedisStreamBus<B: StreamBackend> + MemoryStreamBackend + RedisBackend
├── outbox/           OutboxStore trait + OutboxStatus state machine (transactional outbox)
├── idempotency/      IdempotencyStore (dedup) + IdempotencyClaimStore (lease-based dedup)
├── integration/      IntegrationEvent + MessageFactory + EventPublisher (DDD helpers)
├── dispatcher/       Dispatcher / Notifier / Listener traits (outbox-relay workers)
├── message_contract/ Header constants + TraceContext + SchemaDescriptor
├── delivery_contract/ DeliveryInspector + DeliveryOutcome + DeliveryState
└── consumer/         ConsumerMessageRecord
```

`Message` is the canonical envelope shared by all layers. `SubscriptionConfig`
drives consumer behaviour — always call `normalize_and_validate()` before use.
