---
name: cluster-61
description: "Skill for the Cluster_61 area of eventbus-contract. 8 symbols across 1 files."
---

# Cluster_61

8 symbols | 1 files | Cohesion: 84%

## When to Use

- Working with code in `crates/`
- Understanding how new, stream_bus_from_connection work
- Modifying cluster_61-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `crates/eventbus-redis/src/redis.rs` | new, stream_bus_from_connection, decode_entry, parse_autoclaim, retry_attempt (+3) |

## Entry Points

Start here when exploring this area:

- **`new`** (Function) — `crates/eventbus-redis/src/redis.rs:103`
- **`stream_bus_from_connection`** (Function) — `crates/eventbus-redis/src/redis.rs:128`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `new` | Function | `crates/eventbus-redis/src/redis.rs` | 103 |
| `stream_bus_from_connection` | Function | `crates/eventbus-redis/src/redis.rs` | 128 |
| `decode_entry` | Function | `crates/eventbus-redis/src/redis.rs` | 319 |
| `parse_autoclaim` | Function | `crates/eventbus-redis/src/redis.rs` | 388 |
| `retry_attempt` | Function | `crates/eventbus-redis/src/redis.rs` | 419 |
| `decode_entry_reports_invalid_payload_as_malformed` | Function | `crates/eventbus-redis/src/redis.rs` | 469 |
| `parse_autoclaim_surfaces_malformed_entry` | Function | `crates/eventbus-redis/src/redis.rs` | 485 |
| `parse_autoclaim_returns_next_cursor_and_entries` | Function | `crates/eventbus-redis/src/redis.rs` | 504 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Parse_autoclaim_returns_next_cursor_and_entries → Source` | cross_community | 4 |
| `Parse_autoclaim_returns_next_cursor_and_entries → Retry_attempt` | intra_community | 4 |
| `Parse_autoclaim_returns_next_cursor_and_entries → ClaimedMessage` | intra_community | 4 |
| `Parse_autoclaim_returns_next_cursor_and_entries → New` | intra_community | 4 |
| `Parse_autoclaim_surfaces_malformed_entry → Source` | cross_community | 4 |
| `Parse_autoclaim_surfaces_malformed_entry → Retry_attempt` | intra_community | 4 |
| `Parse_autoclaim_surfaces_malformed_entry → ClaimedMessage` | intra_community | 4 |
| `Parse_autoclaim_surfaces_malformed_entry → New` | intra_community | 4 |
| `Main → New` | cross_community | 3 |
| `Decode_entry_reports_invalid_payload_as_malformed → Source` | cross_community | 3 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Stream | 2 calls |

## How to Explore

1. `gitnexus_context({name: "new"})` — see callers and callees
2. `gitnexus_query({query: "cluster_61"})` — find related execution flows
3. Read key files listed above for implementation details
