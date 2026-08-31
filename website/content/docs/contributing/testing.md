---
title: 测试范围
weight: 30
---

默认集成测试使用 `MemoryStreamBackend`，不需要外部服务。为每个行为先增加可观察的失败测试，然后以
最小实现让它通过；Redis 特有协议再补 feature-gated 测试。

至少覆盖以下行为：

- 单条 publish，以及 `publish_batch` 的 partial outcomes；
- 三种 ACK modes；
- NACK 路径和 retry 的成功、耗尽两种结果；
- 没有配置 DLQ 时的行为；
- 同一 consumer group 的 competing consumers；
- 配置 validation，包括 capacity 与 guarantee 约束；
- `JsonCodec` 编解码和错误路径；
- 启用 `watermill` feature 的 `WatermillStreamCodec` 兼容性。

发布前至少运行 `cargo test --workspace`；改动 Redis codec 或 feature 时再运行
`cargo test -p eventbus-redis --features watermill`。对后端改动，测试断言最终 delivery 和 pending 状态，
不要只断言某个内部 helper 被调用。
