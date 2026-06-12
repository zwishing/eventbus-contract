---
name: eventbus
description: "Skill for the Eventbus area of eventbus-contract. 25 symbols across 3 files."
---

# Eventbus

25 symbols | 3 files | Cohesion: 89%

## When to Use

- Working with code in `crates/`
- Understanding how new, with_ordered_key, with_require_ordered_key work
- Modifying eventbus-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `crates/eventbus-core/src/eventbus/mod.rs` | new, with_ordered_key, with_require_ordered_key, with_guarantee, with_confirmation (+17) |
| `crates/eventbus-core/src/stream/bus.rs` | handle_malformed_entry, subscribe_inner |
| `crates/eventbus-core/src/stream/delivery.rs` | publish_dead_letter |

## Entry Points

Start here when exploring this area:

- **`new`** (Function) — `crates/eventbus-core/src/eventbus/mod.rs:303`
- **`with_ordered_key`** (Function) — `crates/eventbus-core/src/eventbus/mod.rs:312`
- **`with_require_ordered_key`** (Function) — `crates/eventbus-core/src/eventbus/mod.rs:322`
- **`with_guarantee`** (Function) — `crates/eventbus-core/src/eventbus/mod.rs:327`
- **`with_confirmation`** (Function) — `crates/eventbus-core/src/eventbus/mod.rs:332`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `new` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 303 |
| `with_ordered_key` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 312 |
| `with_require_ordered_key` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 322 |
| `with_guarantee` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 327 |
| `with_confirmation` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 332 |
| `with_backpressure` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 342 |
| `with_topic_ttl` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 347 |
| `as_str` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 56 |
| `as_str` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 126 |
| `auto` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 200 |
| `as_str` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 207 |
| `normalize_and_validate` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 670 |
| `into_result` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 811 |
| `dead_letter_topic` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 484 |
| `publish_options_accepts_valid` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 925 |
| `publish_options_rejects_missing_ordered_key` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 941 |
| `publish_options_rejects_exactly_once_without_persisted` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 947 |
| `publish_options_rejects_exactly_once_without_confirmation` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 955 |
| `publish_options_rejects_zero_ttl` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 961 |
| `consumer_name_auto_is_unique` | Function | `crates/eventbus-core/src/eventbus/mod.rs` | 1068 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Subscribe → As_str` | cross_community | 3 |
| `Subscribe → Auto` | cross_community | 3 |
| `Subscribe → Normalize_and_validate` | cross_community | 3 |
| `Subscribe → As_str` | cross_community | 3 |
| `Spawn_messages → New` | cross_community | 3 |
| `Spawn_messages → Message` | cross_community | 3 |
| `Spawn_messages → As_str` | cross_community | 3 |
| `Spawn_messages → As_str` | cross_community | 3 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Stream | 4 calls |

## How to Explore

1. `gitnexus_context({name: "new"})` — see callers and callees
2. `gitnexus_query({query: "eventbus"})` — find related execution flows
3. Read key files listed above for implementation details
