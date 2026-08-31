---
title: 配置参考
weight: 20
---

配置应在构造处完成校验，而不是把不一致的容量或投递保证带进运行时。完整字段与 builder
签名请查 docs.rs；本页说明关键字段和约束。

## `PublishOptions`

| 字段组 | 用途 | 校验或效果 |
|---|---|---|
| `delay`、`ordered_key`、`require_ordered_key` | 延迟发布和按 key 的顺序意图。 | 要求顺序 key 时，key 不能为空。 |
| `guarantee`、`confirmation` | 声明投递保证与发布确认。 | `ExactlyOnce` requires `PublishConfirmation::Persisted`. |
| `idempotency_key`、`metadata` | 业务去重键与传输元数据。 | 由应用定义业务语义。 |
| `backpressure` | 发布侧容量策略。 | 存在时调用 policy 校验。 |
| `topic_ttl`、`expected_content_type`、`expected_event_version` | 主题生命周期与信封约定。 | TTL 不能为零。 |

## `SubscriptionConfig`

| 字段组 | 用途 | 规范化与约束 |
|---|---|---|
| `topic`、`consumer_group`、`consumer_name` | 路由与消费者身份。 | 通过 `SubscriptionConfig::builder` 提供。 |
| `ack_mode`、`max_retry`、`retry_backoff`、`dead_letter_topic` | 完成、重试与死信行为。 | builder 的 `build` normalizes and validates configuration。 |
| `ordering_mode`、`balance_mode`、`guarantee` | 顺序、消费分配和投递目标。 | 未设置时使用库默认值。 |
| `max_in_flight`、`max_pending_acks`、`backpressure` | 本地并发与未确认容量。 | 顶层与嵌套 policy 同时给出时必须一致。 |

若直接持有或反序列化 contract，而不是用 builder，请调用
`normalize_and_validate`；它先补齐默认值，再执行一致性验证。

## `StreamBusOptions`

| 字段组 | 用途 |
|---|---|
| `block_timeout`、`claim_idle_timeout`、`claim_scan_batch_size`、`group_start_id` | 读取阻塞、idle reclaim 和 consumer group 起点。 |
| `publish_batch_parallelism` | 单次 `publish_batch` 的并发后端发布上限。 |
| `ack_batch_size`、`ack_flush_interval` | ACK 批量大小和强制 flush 延迟。 |
| `reclaim_interval` | 独立 reclaim 任务的检查频率。 |
| `max_payload_bytes`、`error_observer` | 负载上限与后台循环中的非阻塞观察 hook。 |

## `BackpressurePolicy`

| 字段 | 含义 |
|---|---|
| `max_in_flight` | 同时执行的 delivery 上限。 |
| `max_pending_acks` | 可以等待确认的 delivery 上限。 |
| `max_batch_size` | 单批接受的最大数量。 |
| `overflow_strategy` | 容量耗尽时的处理策略。 |

硬约束为 `max_pending_acks >= max_in_flight > 0`；`max_batch_size` 也必须大于零，且不能超过
这两个容量。顶层 concurrency 值必须与嵌套 `backpressure` 值一致，避免同一订阅出现两套容量定义。
