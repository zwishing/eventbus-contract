---
title: Watermill Redis Stream 兼容
weight: 30
---

## 启用兼容读取

需要读取 Go Watermill 的 canonical Redis Stream entry 时，启用 `redis-watermill`：

```toml
[dependencies]
eventbus-contract = { version = "0.2.1", features = ["redis-watermill"] }
```

这个 Feature 会启用 Redis 后端及 Watermill codec。迁移 facade crate 与 Feature 的背景见
[`MIGRATION-0.2.md`](https://github.com/mapseekai/eventbus-contract/blob/main/MIGRATION-0.2.md)。

## 编码边界

原生 eventbus 写入仍使用 eventbus JSON envelope；开启 Feature 不会把写入格式自动改成
Watermill。Watermill 支持负责读取 canonical entry 的字段（包括
`_watermill_message_uuid`、`metadata` 和 `payload`）并解码为 eventbus `Message`。

为特定 stream 选择 Watermill 读取 codec，或在已确认需要兼容读取时选择 auto-detect 读取
codec；相关实现和字段映射见
[`crates/eventbus-redis/src/codec.rs`](https://github.com/mapseekai/eventbus-contract/blob/main/crates/eventbus-redis/src/codec.rs)。
具体 API 签名以 [docs.rs](https://docs.rs/eventbus-redis) 为准。

## 自动识别只用于读取

auto-detect 的职责是读取侧在默认 JSON envelope 和 Watermill canonical entry 之间选择解码器；
它不能作为写入 codec。不要把它当作跨格式写入转换器。

## 混合 Stream

一个 stream 混入两种格式时，先用真实样本写显式兼容性测试：分别验证默认 JSON 与
Watermill entry 都能读取、字段映射符合业务约束，并验证失败 entry 的处理路径。部署前在每个
混合 stream 上运行这些测试；不要仅凭 Feature 已启用就假定生产数据可互操作。
