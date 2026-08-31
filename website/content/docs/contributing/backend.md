---
title: 实现后端
weight: 20
---

新的传输实现必须符合 `StreamBackend` contract，而不只是提供“能发布”的适配器。先在独立 crate 中实现
存储操作，再将其交给 `StreamBus<B>` 管理不依赖传输的消费流程。

## Conformance checklist

- 实现 group 创建、append、读取新消息、读取/认领 idle pending 消息，以及单条和批量 ACK。
- 保持 consumer-group 语义：同一 group 的竞争消费者不应各自处理同一待投递条目。
- 正确维护 pending ACK；成功 ACK 后条目不应继续作为 pending 被 reclaim。
- 支持 reclaim，使未确认且超过 idle 阈值的消息能被再次处理，并保留必要的 delivery state。
- 将底层驱动错误用 `EventBusError::source` 包装，保留 source chain，而不是丢失类型信息的字符串。
- 为后端行为补齐与内存后端相同的 parity tests，包括错误路径和竞争场景。

不要在 backend 中复制 `StreamBus` 的 retry、handler 或容量控制逻辑；复制会让不同传输得到不同投递
语义。若存储没有某项必需能力，应明确返回可诊断错误，或不要声明支持该 contract。
