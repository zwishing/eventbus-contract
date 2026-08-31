---
title: 竞争消费者
weight: 40
---

## 单订阅内的并发

示例 03 展示了同一个 consumer group 内的竞争消费：`max_in_flight = 3` 允许订阅同时运行
三个 handler task，而订阅仍保留一个稳定的 consumer identity。消息会由同组消费者竞争取得，
因此不同任务之间的投递顺序不保证。

```bash
cargo run -p eventbus-contract --example 03_competing_consumers
```

示例使用内存后端来观察九条消息和 `pending_count`；完整代码见
[`03_competing_consumers.rs`](https://github.com/mapseekai/eventbus-contract/blob/main/crates/eventbus-contract/examples/03_competing_consumers.rs)。

## 与多实例的区别

`max_in_flight` 是一个进程内订阅的并发上限：一个订阅、一份 handler 配置、一个稳定的
consumer name。它不会启动多个服务实例，也不会提供跨进程的故障隔离。

多实例扩展则是在同一个 consumer group 中运行多个进程或容器，并为每个实例提供自己的稳定
consumer name；Redis Streams 会在这些消费者之间分配消息。将二者结合时，先按单实例
`max_in_flight` 控制本地资源，再用实例数扩大总吞吐量，并为重复投递保持 handler 幂等。
