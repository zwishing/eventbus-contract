---
title: 内存后端
weight: 10
---

## 适用范围

`MemoryStreamBackend` 是进程内后端：无需 Redis 或其他外部服务，适合本地开发、示例和
集成测试。它的状态只存在当前进程中；进程退出后消息和消费者组都会消失，因此它不是持久化
后端，也不适合跨进程投递或生产消息留存。

## 创建总线

`memory` 是 facade crate 的默认 Feature。创建一个共享后端，并把它交给 `StreamBus`：

```rust
let backend = Arc::new(MemoryStreamBackend::default());
let bus = StreamBus::new(Arc::clone(&backend), StreamBusOptions::default())?;
```

完整的发布、订阅和关闭流程见
[`01_basic_pubsub.rs`](https://github.com/mapseekai/eventbus-contract/blob/main/crates/eventbus-contract/examples/01_basic_pubsub.rs)。
具体类型和方法签名以 [docs.rs](https://docs.rs/eventbus-contract) 为准。

## 测试投递结果

示例 01 先订阅、再发布，并通过 `mpsc` 通道和超时等待处理器收到消息；测试中可断言收到的
消息 UID、处理器副作用，或 `pending_count` 是否回到零。项目的
[`stream_bus.rs`](https://github.com/mapseekai/eventbus-contract/blob/main/crates/eventbus-memory/tests/stream_bus.rs)
覆盖了内存后端的订阅、确认和重投递行为，可作为测试断言的参考。

## 限制

内存后端不提供 Redis 的跨实例消费者组、持久化或运维能力。需要在多个进程间共享消息、在
重启后保留未处理消息，或监控积压时，请改用 [Redis Streams](../redis/) 后端。
