---
title: 五分钟快速开始
weight: 30
---

下面的程序只使用内存后端，无须启动 Redis。创建一个新的 Rust 二进制项目，并添加
所有代码直接引用的依赖：

```toml
[dependencies]
eventbus-contract = "0.2.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "sync"] }
chrono = "0.4"
bytes = "1"
```

将 `src/main.rs` 替换为以下完整程序：

```rust
use std::sync::Arc;

use chrono::Utc;
use eventbus_contract::{
    core::{stream::{StreamBus, StreamBusOptions}, Headers},
    memory::MemoryStreamBackend,
    prelude::*,
};
use tokio::sync::mpsc;

struct Echo {
    tx: mpsc::Sender<String>,
}

impl Handler for Echo {
    fn handle(
        &self,
        delivery: Box<dyn DeliveryHandle>,
    ) -> BoxFuture<'_, Result<(), EventBusError>> {
        Box::pin(async move {
            let uid = delivery.message().uid.clone();
            println!("收到消息：{uid}");
            delivery.ack().await?;
            self.tx
                .send(uid)
                .await
                .map_err(|error| EventBusError::Internal(error.to_string()))
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(MemoryStreamBackend::default());
    let bus = StreamBus::new(backend, StreamBusOptions::default())?;
    let (tx, mut rx) = mpsc::channel(1);

    // 先订阅，再发布：消费者组必须先存在。
    let subscription = bus
        .subscribe_with(
            SubscriptionConfig::builder(
                Topic::new("user.registered")?,
                ConsumerGroup::new("notification-service")?,
            )
            .ack_mode(AckMode::Manual)
            .max_in_flight(1)
            .build()?,
            Echo { tx },
        )
        .await?;

    bus.publish(
        Message {
            uid: "evt-001".to_string(),
            topic: Topic::new("user.registered")?,
            key: "user-42".to_string(),
            kind: "UserRegistered".to_string(),
            source: "auth-service".to_string(),
            occurred_at: Utc::now(),
            headers: Headers::new(),
            payload: bytes::Bytes::from_static(br#"{"user_id":42}"#),
            content_type: Some("application/json".to_string()),
            event_version: Some("1.0".to_string()),
            idempotency_key: Some("reg-user-42".to_string()),
            expires_at: None,
            trace_uid: None,
            correlation_uid: None,
        },
        PublishOptions::default(),
    )
    .await?;

    println!("已确认：{}", rx.recv().await.expect("处理器关闭"));
    subscription.close().await?;
    Ok(())
}
```

运行 `cargo run` 后，处理器会输出收到的消息，手动调用 `delivery.ack()` 后主函数关闭订阅。

> 消费者组需要在消息写入前建立，因此示例先订阅，再发布。完整的带超时与断言版本见
> [`01_basic_pubsub.rs`](https://github.com/mapseekai/eventbus-contract/blob/main/crates/eventbus-contract/examples/01_basic_pubsub.rs)。
