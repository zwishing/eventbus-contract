---
name: contract
description: "Skill for the Contract area of eventbus-contract. 12 symbols across 1 files."
---

# Contract

12 symbols | 1 files | Cohesion: 81%

## When to Use

- Working with code in `crates/`
- Understanding how set_schema, schema, normalize work
- Modifying contract-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `crates/eventbus-core/src/contract/message.rs` | set_schema, schema, normalize, test_message, schema_roundtrip (+7) |

## Entry Points

Start here when exploring this area:

- **`set_schema`** (Function) — `crates/eventbus-core/src/contract/message.rs:71`
- **`schema`** (Function) — `crates/eventbus-core/src/contract/message.rs:80`
- **`normalize`** (Function) — `crates/eventbus-core/src/contract/message.rs:136`
- **`set_trace_context`** (Function) — `crates/eventbus-core/src/contract/message.rs:87`
- **`trace_context`** (Function) — `crates/eventbus-core/src/contract/message.rs:110`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `set_schema` | Function | `crates/eventbus-core/src/contract/message.rs` | 71 |
| `schema` | Function | `crates/eventbus-core/src/contract/message.rs` | 80 |
| `normalize` | Function | `crates/eventbus-core/src/contract/message.rs` | 136 |
| `set_trace_context` | Function | `crates/eventbus-core/src/contract/message.rs` | 87 |
| `trace_context` | Function | `crates/eventbus-core/src/contract/message.rs` | 110 |
| `test_message` | Function | `crates/eventbus-core/src/contract/message.rs` | 165 |
| `schema_roundtrip` | Function | `crates/eventbus-core/src/contract/message.rs` | 185 |
| `idempotency_key_reads_only_typed_field` | Function | `crates/eventbus-core/src/contract/message.rs` | 239 |
| `normalize_hoists_headers_into_typed_fields` | Function | `crates/eventbus-core/src/contract/message.rs` | 250 |
| `normalize_does_not_overwrite_explicit_typed_fields` | Function | `crates/eventbus-core/src/contract/message.rs` | 268 |
| `trace_context_roundtrip` | Function | `crates/eventbus-core/src/contract/message.rs` | 195 |
| `set_trace_context_rejects_oversized_traceparent` | Function | `crates/eventbus-core/src/contract/message.rs` | 216 |

## How to Explore

1. `gitnexus_context({name: "set_schema"})` — see callers and callees
2. `gitnexus_query({query: "contract"})` — find related execution flows
3. Read key files listed above for implementation details
