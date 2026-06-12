---
name: tests
description: "Skill for the Tests area of eventbus-contract. 83 symbols across 21 files."
---

# Tests

83 symbols | 21 files | Cohesion: 86%

## When to Use

- Working with code in `crates/`
- Understanding how with_metadata, with_idempotency_key, builder work
- Modifying tests-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `crates/eventbus-memory/tests/stream_bus.rs` | message, publish_subscribe_auto_ack_drains_pending, manual_ack_drains_pending, retry_redelivers_message_and_then_drains_pending, reclaims_pending_from_inactive_consumer (+18) |
| `crates/eventbus-core/src/eventbus/mod.rs` | with_metadata, with_idempotency_key, builder, consumer_name, max_retry (+12) |
| `crates/eventbus-outbox/tests/public_api_parity.rs` | delivery_trait_exposes_delivery_inspection, require_delivery_inspector, assert_delivery_has_inspector, root_exports_idempotency_claim_contracts, assert_claim_store |
| `crates/eventbus-memory/tests/observer.rs` | drop_without_close_fires_on_error_with_drop_scope, handler_panic_fires_on_panic, message, publish_batch_returns_per_message_results |
| `crates/eventbus-memory/tests/poison_pill.rs` | publish, publish_via_inner, message, malformed_entry_is_acked_observed_and_dlq_routed |
| `crates/eventbus-memory/tests/publish_batch_parallelism.rs` | new, observed_max, message, publish_batch_respects_parallelism_cap |
| `crates/eventbus-memory/tests/finalize_typesafe.rs` | message, dropping_box_without_finalize_releases_permit, ack_consumes_box_and_clears_pending |
| `crates/eventbus-contract/examples/02_manual_ack_and_retry.rs` | sample_message, main |
| `crates/eventbus-contract/examples/04_redis_backend.rs` | main, uuid |
| `crates/eventbus-core/src/stream/bus.rs` | with_error_observer, with_publish_batch_parallelism |

## Entry Points

Start here when exploring this area:

- **`with_metadata`** (Function) — `crates/eventbus-core/src/eventbus/mod.rs:317`
- **`with_idempotency_key`** (Function) — `crates/eventbus-core/src/eventbus/mod.rs:337`
- **`builder`** (Function) — `crates/eventbus-core/src/eventbus/mod.rs:435`
- **`consumer_name`** (Function) — `crates/eventbus-core/src/eventbus/mod.rs:527`
- **`max_retry`** (Function) — `crates/eventbus-core/src/eventbus/mod.rs:531`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `with_metadata` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 317 |
| `with_idempotency_key` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 337 |
| `builder` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 435 |
| `consumer_name` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 527 |
| `max_retry` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 531 |
| `dead_letter_topic` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 539 |
| `ack_mode` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 543 |
| `max_in_flight` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 559 |
| `max_pending_acks` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 563 |
| `build` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 577 |
| `with_error_observer` | Function | `crates/eventbus-core/src/stream/bus.rs` | 200 |
| `balance` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 551 |
| `backpressure` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 567 |
| `abort` | Function | `crates/eventbus-core/src/stream/subscription.rs` | 74 |
| `stream_len` | Function | `crates/eventbus-memory/src/memory.rs` | 39 |
| `with_publish_batch_parallelism` | Function | `crates/eventbus-core/src/stream/bus.rs` | 164 |
| `pending_count` | Function | `crates/eventbus-memory/src/memory.rs` | 30 |
| `main` | Function | `crates/eventbus-contract/examples/01_basic_pubsub.rs` | 57 |
| `sample_message` | Function | `crates/eventbus-contract/examples/02_manual_ack_and_retry.rs` | 105 |
| `main` | Function | `crates/eventbus-contract/examples/02_manual_ack_and_retry.rs` | 129 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Main → New` | cross_community | 3 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Eventbus | 2 calls |
| Cluster_61 | 1 calls |

## How to Explore

1. `gitnexus_context({name: "with_metadata"})` — see callers and callees
2. `gitnexus_query({query: "tests"})` — find related execution flows
3. Read key files listed above for implementation details
