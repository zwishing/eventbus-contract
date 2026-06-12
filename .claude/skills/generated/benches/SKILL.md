---
name: benches
description: "Skill for the Benches area of eventbus-contract. 3 symbols across 1 files."
---

# Benches

3 symbols | 1 files | Cohesion: 100%

## When to Use

- Working with code in `crates/`
- Understanding how message, bench_publish_single, bench_publish_batch_100 work
- Modifying benches-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `crates/eventbus-memory/benches/publish.rs` | message, bench_publish_single, bench_publish_batch_100 |

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `message` | Function | `crates/eventbus-memory/benches/publish.rs` | 15 |
| `bench_publish_single` | Function | `crates/eventbus-memory/benches/publish.rs` | 34 |
| `bench_publish_batch_100` | Function | `crates/eventbus-memory/benches/publish.rs` | 51 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Bench_publish_single → Message` | intra_community | 3 |
| `Bench_publish_batch_100 → Message` | intra_community | 3 |

## How to Explore

1. `gitnexus_context({name: "message"})` — see callers and callees
2. `gitnexus_query({query: "benches"})` — find related execution flows
3. Read key files listed above for implementation details
