---
title: 包与功能特性
weight: 10
---

0.2.x 是六个 workspace crate 的边界；其中只有 core、memory、Redis 和 facade 发布。
精确的公开成员、签名和 feature 元数据请查看已固定到 0.2.1 的 docs.rs API：

- [eventbus-contract 0.2.1](https://docs.rs/eventbus-contract/0.2.1/eventbus_contract/)
- [eventbus-core 0.2.1](https://docs.rs/eventbus-core/0.2.1/eventbus_core/)
- [eventbus-memory 0.2.1](https://docs.rs/eventbus-memory/0.2.1/eventbus_memory/)
- [eventbus-redis 0.2.1](https://docs.rs/eventbus-redis/0.2.1/eventbus_redis/)

| 包 | 作用 | 0.2.x 发布状态 |
|---|---|:---:|
| `eventbus-core` | 对象安全契约、值对象和泛型 `StreamBus`。 | 是 |
| `eventbus-memory` | 供测试和开发使用的进程内 `StreamBackend`。 | 是 |
| `eventbus-redis` | Redis Streams 的 `StreamBackend` 及编解码器。 | 是 |
| `eventbus-outbox` | Outbox、dispatcher、死信和幂等 trait。 | 否，计划 0.3.0 |
| `eventbus-integration` | DDD 集成事件辅助类型。 | 否，计划 0.3.0 |
| `eventbus-contract` | 面向应用的 facade，按 feature re-export 已发布 crate。 | 是 |

## 门面包功能特性

| 功能特性 | 默认启用 | 含义 |
|---|:---:|---|
| `memory` | 是 | 进程内后端，适合测试与本地开发。 |
| `redis` | 否 | Redis Streams 后端。 |
| `redis-watermill` | 否 | 启用 Redis，并转发 Watermill canonical-entry 解码支持。 |
| `tracing` | 否 | 为 hot path 启用 `tracing` instrumentation。 |

`outbox` 与 `integration` 不是 0.2.x facade feature。它们仍在 workspace 中；需要等参考实现
一同发布后，才会在 0.3.0 回到 facade。升级到 0.2 时应同时阅读
[`MIGRATION-0.2.md`](https://github.com/mapseekai/eventbus-contract/blob/main/MIGRATION-0.2.md)。
