---
name: stream
description: "Skill for the Stream area of eventbus-contract. 32 symbols across 9 files."
---

# Stream

32 symbols | 9 files | Cohesion: 70%

## When to Use

- Working with code in `crates/`
- Understanding how with_max_attempt, take_remaining, pre_ack work
- Modifying stream-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `crates/eventbus-core/src/stream/bus.rs` | spawn_messages, process_single_message, publish_inner, publish_batch_impl, prepare_message (+16) |
| `crates/eventbus-core/src/stream/delivery.rs` | mark_acked, pre_ack |
| `crates/eventbus-core/src/contract/message.rs` | set_idempotency_key, idempotency_key_roundtrip |
| `crates/eventbus-core/src/stream/ack_flusher.rs` | spawn, flush_batch |
| `crates/eventbus-core/src/contract/delivery.rs` | with_max_attempt |
| `crates/eventbus-core/src/stream/auto_finalize.rs` | take_remaining |
| `crates/eventbus-core/src/eventbus/mod.rs` | validate |
| `crates/eventbus-core/src/error.rs` | source |
| `crates/eventbus-memory/tests/stream_bus.rs` | publish_batch_rejects_oversize_payload_before_publishing_any_message |

## Entry Points

Start here when exploring this area:

- **`with_max_attempt`** (Function) — `crates/eventbus-core/src/contract/delivery.rs:35`
- **`take_remaining`** (Function) — `crates/eventbus-core/src/stream/auto_finalize.rs:54`
- **`pre_ack`** (Function) — `crates/eventbus-core/src/stream/delivery.rs:94`
- **`set_idempotency_key`** (Function) — `crates/eventbus-core/src/contract/message.rs:120`
- **`validate`** (Function) — `crates/eventbus-core/src/eventbus/mod.rs:362`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `with_max_attempt` | Function | `crates/eventbus-core/src/contract/delivery.rs` | 35 |
| `take_remaining` | Function | `crates/eventbus-core/src/stream/auto_finalize.rs` | 54 |
| `pre_ack` | Function | `crates/eventbus-core/src/stream/delivery.rs` | 94 |
| `set_idempotency_key` | Function | `crates/eventbus-core/src/contract/message.rs` | 120 |
| `validate` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 362 |
| `source` | Function | `crates/eventbus-core/src/error.rs` | 57 |
| `subscribe` | Function | `crates/eventbus-core/src/stream/bus.rs` | 286 |
| `new` | Function | `crates/eventbus-core/src/stream/bus.rs` | 130 |
| `new` | Function | `crates/eventbus-core/src/stream/bus.rs` | 257 |
| `spawn` | Function | `crates/eventbus-core/src/stream/ack_flusher.rs` | 31 |
| `with_max_payload_bytes` | Function | `crates/eventbus-core/src/stream/bus.rs` | 193 |
| `spawn_messages` | Function | `crates/eventbus-core/src/stream/bus.rs` | 598 |
| `process_single_message` | Function | `crates/eventbus-core/src/stream/bus.rs` | 695 |
| `mark_acked` | Function | `crates/eventbus-core/src/stream/delivery.rs` | 60 |
| `idempotency_key_roundtrip` | Function | `crates/eventbus-core/src/contract/message.rs` | 227 |
| `publish_inner` | Function | `crates/eventbus-core/src/stream/bus.rs` | 298 |
| `publish_batch_impl` | Function | `crates/eventbus-core/src/stream/bus.rs` | 317 |
| `prepare_message` | Function | `crates/eventbus-core/src/stream/bus.rs` | 412 |
| `consume_loop` | Function | `crates/eventbus-core/src/stream/bus.rs` | 447 |
| `drain_completed_tasks` | Function | `crates/eventbus-core/src/stream/bus.rs` | 1017 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Spawn_messages → AckRequest` | intra_community | 5 |
| `Parse_autoclaim_returns_next_cursor_and_entries → Source` | cross_community | 4 |
| `Parse_autoclaim_surfaces_malformed_entry → Source` | cross_community | 4 |
| `Publish_batch_impl → Set_idempotency_key` | intra_community | 3 |
| `Subscribe → As_str` | cross_community | 3 |
| `Subscribe → Auto` | cross_community | 3 |
| `Subscribe → Normalize_and_validate` | cross_community | 3 |
| `Subscribe → As_str` | cross_community | 3 |
| `Consume_loop → Source` | intra_community | 3 |
| `Consume_loop → Sleep_or_close` | intra_community | 3 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Tests | 2 calls |
| Eventbus | 2 calls |
| Contract | 1 calls |

## How to Explore

1. `gitnexus_context({name: "with_max_attempt"})` — see callers and callees
2. `gitnexus_query({query: "stream"})` — find related execution flows
3. Read key files listed above for implementation details
