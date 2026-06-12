---
name: cluster-60
description: "Skill for the Cluster_60 area of eventbus-contract. 5 symbols across 1 files."
---

# Cluster_60

5 symbols | 1 files | Cohesion: 100%

## When to Use

- Working with code in `crates/`
- Understanding how encode, decode, sample work
- Modifying cluster_60-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `crates/eventbus-redis/src/codec.rs` | encode, decode, sample, round_trip_preserves_message, wire_format_uses_message_envelope |

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `encode` | Function | `crates/eventbus-redis/src/codec.rs` | 36 |
| `decode` | Function | `crates/eventbus-redis/src/codec.rs` | 42 |
| `sample` | Function | `crates/eventbus-redis/src/codec.rs` | 57 |
| `round_trip_preserves_message` | Function | `crates/eventbus-redis/src/codec.rs` | 77 |
| `wire_format_uses_message_envelope` | Function | `crates/eventbus-redis/src/codec.rs` | 88 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Round_trip_preserves_message → Message` | intra_community | 3 |
| `Wire_format_uses_message_envelope → Message` | intra_community | 3 |

## How to Explore

1. `gitnexus_context({name: "encode"})` — see callers and callees
2. `gitnexus_query({query: "cluster_60"})` — find related execution flows
3. Read key files listed above for implementation details
