---
title: 示例与命令
weight: 40
---

在 workspace 根目录运行以下命令。前三个 contract 示例使用内存后端；**只有 Redis 示例需要外部 Redis**。

```bash
cargo test --workspace
cargo test -p eventbus-redis --features watermill
cargo fmt --all --check
cargo clippy -p eventbus-redis --features watermill --all-targets
cargo run -p eventbus-contract --example 01_basic_pubsub
cargo run -p eventbus-contract --example 02_manual_ack_and_retry
cargo run -p eventbus-contract --example 03_competing_consumers
cargo run -p eventbus-contract --features redis --example 04_redis_backend
```

`01_basic_pubsub` 展示先订阅再发布；`02_manual_ack_and_retry` 关注手动 ACK、NACK 和 retry；
`03_competing_consumers` 说明同一 group 中的竞争消费。运行 `04_redis_backend` 前，设置 `REDIS_URL`
（默认是 `redis://127.0.0.1:6379/`）并确保服务可访问。
