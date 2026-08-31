---
title: Redis Streams 后端
weight: 20
---

## 启用 Feature

Redis 后端需要 Redis 服务和 facade crate 的 `redis` Feature：

```toml
[dependencies]
eventbus-contract = { version = "0.2.1", features = ["redis"] }
redis = { version = "1", features = ["tokio-comp", "streams"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## 建立连接

使用 `redis` crate 创建 multiplexed async connection，再交给便捷构造函数。该函数以默认
JSON codec 包装 `RedisBackend` 并创建 `StreamBus`；精确签名以
[docs.rs](https://docs.rs/eventbus-redis) 为准。

```rust
use eventbus_contract::core::stream::StreamBusOptions;

let client = redis::Client::open(redis_url.as_str())?;
let connection = client.get_multiplexed_async_connection().await?;
let bus = eventbus_contract::redis::stream_bus_from_connection(
    connection,
    StreamBusOptions::default(),
)?;
```

可直接参考[`04_redis_backend.rs`](https://github.com/mapseekai/eventbus-contract/blob/main/crates/eventbus-contract/examples/04_redis_backend.rs)
的连接、订阅、发布和关闭流程。

## 订阅配置

通过 `SubscriptionConfig::builder` 提供 topic、消费者组和稳定的 consumer name；再选择
`AckMode`、`max_in_flight`、重试次数和死信 topic。手动确认时，处理器必须在成功处理后调用
`delivery.ack().await?`。示例 04 使用 `AckMode::Manual`、`max_retry(3)`、死信 topic 和
`max_in_flight(1)`，是配置这些选项的可运行起点。

后端在 Redis Streams 上使用以下命令：

Redis 为每个消费者组维护 Pending Entries List（PEL）：消息交给消费者后、收到 ACK 前都会记录为
pending。重新认领是把空闲超过阈值的 pending 消息转交给另一个消费者继续处理，避免消费者退出后
消息永久滞留。

- `XADD` 写入事件。
- `XREADGROUP` 从消费者组读取新事件。
- `XACK` 确认已处理事件，使其离开 pending entries list。
- `XAUTOCLAIM` 认领超过配置空闲时间仍未确认的事件，以便重投递。

## 失败与关闭

将 Redis 连接、超时和命令错误视为应用的运行错误：记录并按应用的恢复策略处理。应用还负责
Redis 的可用性、认证与网络配置、Stream 保留策略，以及消费者积压、失败和延迟的监控。

服务停止时，显式执行 `sub.close().await?`，让订阅按其关闭语义退出；不要只依赖进程终止。
对于需要人工处理的失败，可结合 `max_retry` 与死信 topic 保留诊断路径。
