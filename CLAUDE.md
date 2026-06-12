# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build
cargo build
cargo build --features redis-backend   # include Redis backend

# Test
cargo test                              # all tests (unit + integration with MemoryStreamBackend)
cargo test --features redis-backend     # also compile redis_backend module
cargo test <test_name>                  # single test by name
cargo test -- --nocapture               # show println! output

# Lint / format
cargo clippy
cargo fmt
```

Integration tests in `tests/` use `MemoryStreamBackend` and require no external services to run.

## Architecture

This is a pure Rust **event bus contract library** (`eventbus-contract`). It defines traits and types for event-driven messaging. The concrete transport is pluggable — only one backend (`StreamBus`) is implemented, with a `MemoryStreamBackend` for testing.

### Layer 1 — Core traits (`src/eventbus/mod.rs`)

The public API surface. Implement these traits to build publishers, consumers, and handlers:

- `Publisher` — `publish` / `publish_batch`
- `Subscriber` — `subscribe(cfg, handler) -> Subscription`
- `Handler` — `handle(delivery)` — user-supplied message processor
- `Delivery` — wraps a received `Message`; exposes `ack` / `nack` / `retry`
- `Bus = Publisher + Subscriber` (blanket impl)
- `Codec` — pluggable serialization (serialize/deserialize)

`Message` is the canonical envelope: `uid`, `topic`, `key`, `kind`, `source`, `occurred_at`, `headers`, `payload` (bytes), plus optional trace/idempotency/expiry fields.

`SubscriptionConfig` drives consumer behaviour: call `normalize_and_validate()` before use — it fills defaults then checks consistency.

### Layer 2 — Contract types (`src/contract/mod.rs`)

Value objects and policies with validation:

- `DeliveryGuarantee`: `AtMostOnce | AtLeastOnce | ExactlyOnce`
- `AckMode`: `Manual | AutoOnReceive | AutoOnHandlerSuccess`
- `OrderingMode`: `None | Key`
- `ConsumerBalanceMode`: `Competing | FanOut`
- `BackpressurePolicy`: `max_in_flight`, `max_pending_acks`, `max_batch_size`, `overflow_strategy`

### Layer 3 — Redis Stream backend (`src/stream/`)

`StreamBus<B: StreamBackend>` implements `Publisher + Subscriber`. The `StreamBackend` trait decouples the bus logic from the actual stream store:

- `MemoryStreamBackend` — in-process, used by all integration tests
- `RedisBackend` — real Redis Streams, compiled only with `--features redis-backend`

`StreamBusOptions` configures blocking poll timeouts, idle-claim timeouts, and consumer group start position.

### Supporting modules

| Module | Purpose |
|---|---|
| `outbox/` | `OutboxStore` trait + `OutboxStatus` state machine (`Pending → Processing → Sent/Failed/Dead`). Used for transactional outbox pattern. |
| `outbox/dead_letter.rs` | `DeadLetterStore`, `DeadLetterPolicy`, `DeadLetterDecision` — configurable handling of exhausted retries |
| `idempotency/` | `IdempotencyStore` (simple dedup) and `IdempotencyClaimStore` (lease-based dedup) traits |
| `integration/` | `IntegrationEvent` + `MessageFactory` + `EventPublisher` — thin DDD integration-event helpers |
| `dispatcher/` | `Dispatcher` / `Notifier` / `Listener` traits for outbox-relay workers |
| `message_contract/` | Standard header constants (`HEADER_TRACE_PARENT`, `HEADER_IDEMPOTENCY_KEY`, etc.) and `TraceContext` / `SchemaDescriptor` |
| `delivery_contract/` | `DeliveryInspector`, `DeliveryOutcome`, `DeliveryState` — delivery inspection hooks |
| `consumer/` | `ConsumerMessageRecord` — record type for consumer-side tracking |
| `serde_bytes.rs` | Custom serde module for `Vec<u8>` — serializes payload as base64 string |

## Key Constraints

- `ExactlyOnce` delivery guarantee requires `PublishConfirmation::Persisted` — enforced by `PublishOptions::validate()` and `GuaranteeMatrix::validate()`.
- `BackpressurePolicy` requires `max_pending_acks >= max_in_flight > 0`.
- `SubscriptionConfig` fields `max_in_flight` / `max_pending_acks` must agree with `backpressure` when both are set.
- Traits use `async_fn_in_trait` (allowed via `#![allow(async_fn_in_trait)]`) — no `async-trait` crate.

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **eventbus-contract** (1534 symbols, 3153 relationships, 87 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/eventbus-contract/context` | Codebase overview, check index freshness |
| `gitnexus://repo/eventbus-contract/clusters` | All functional areas |
| `gitnexus://repo/eventbus-contract/processes` | All execution flows |
| `gitnexus://repo/eventbus-contract/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
