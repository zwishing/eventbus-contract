---
name: outbox
description: "Skill for the Outbox area of eventbus-contract. 6 symbols across 2 files."
---

# Outbox

6 symbols | 2 files | Cohesion: 100%

## When to Use

- Working with code in `crates/`
- Understanding how decide work
- Modifying outbox-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `crates/eventbus-outbox/src/outbox/dead_letter.rs` | decide, max_retry_exceeded_routes_to_dead, expired_message_routes_to_dead, terminal_failure_routes_to_dead, retriable_message_stays |
| `crates/eventbus-outbox/tests/public_api_parity.rs` | root_exports_go_parity_contracts |

## Entry Points

Start here when exploring this area:

- **`decide`** (Function) — `crates/eventbus-outbox/src/outbox/dead_letter.rs:42`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `decide` | Function | `crates/eventbus-outbox/src/outbox/dead_letter.rs` | 42 |
| `max_retry_exceeded_routes_to_dead` | Function | `crates/eventbus-outbox/src/outbox/dead_letter.rs` | 91 |
| `expired_message_routes_to_dead` | Function | `crates/eventbus-outbox/src/outbox/dead_letter.rs` | 104 |
| `terminal_failure_routes_to_dead` | Function | `crates/eventbus-outbox/src/outbox/dead_letter.rs` | 118 |
| `retriable_message_stays` | Function | `crates/eventbus-outbox/src/outbox/dead_letter.rs` | 131 |
| `root_exports_go_parity_contracts` | Function | `crates/eventbus-outbox/tests/public_api_parity.rs` | 14 |

## How to Explore

1. `gitnexus_context({name: "decide"})` — see callers and callees
2. `gitnexus_query({query: "outbox"})` — find related execution flows
3. Read key files listed above for implementation details
