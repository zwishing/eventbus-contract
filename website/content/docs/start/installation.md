---
title: 安装与 Feature
weight: 20
---

API 参考见 [docs.rs 上的 eventbus-contract](https://docs.rs/eventbus-contract)。

| Feature | Default | Purpose |
|---|:---:|---|
| `memory` | yes | In-process backend for tests and development. |
| `redis` | no | Redis Streams backend. |
| `redis-watermill` | no | Redis plus Watermill canonical-entry decoding. |
| `tracing` | no | Tracing instrumentation on hot paths. |

默认启用 `memory`，适合本地开发和测试：

```toml
[dependencies]
eventbus-contract = "0.2.1"
```

生产环境使用 Redis Streams：

```toml
[dependencies]
eventbus-contract = { version = "0.2.1", features = ["redis"] }
```

若要读取 Go Watermill 的 canonical Redis Stream entry，启用
`redis-watermill`：

```toml
[dependencies]
eventbus-contract = { version = "0.2.1", features = ["redis-watermill"] }
```

`outbox` 和 `integration` facade features 预留给 0.3.0；在参考实现发布前，
它们不会作为 crates.io 的 facade feature 提供。
