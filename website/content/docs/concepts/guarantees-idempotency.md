---
title: 投递保证与幂等
weight: 40
---

`DeliveryGuarantee` 表示发布和消费链路希望达到的投递语义，而不是对任意业务副作用的魔法承诺。

| Guarantee | 含义 | 设计含义 |
|---|---|---|
| `AtMostOnce` | 最多尝试一次，允许丢失。 | 适合允许丢失且避免重复比完整性更重要的场景。 |
| `AtLeastOnce` | 失败时可以再次投递，可能重复。 | handler 必须能安全处理重复。 |
| `ExactlyOnce` | 合同层请求精确一次语义。 | 仍需端到端的应用设计来保护业务副作用。 |

## 被强制校验的前提

当 `PublishOptions` 请求 `DeliveryGuarantee::ExactlyOnce` 时，必须同时请求
`PublishConfirmation::Persisted`；`GuaranteeMatrix` 在发布或消费一侧请求 exactly-once 时也执行同一
约束。否则构造和验证会返回错误。持久化确认说明后端已接受持久化的发布前提，**不等于**数据库写入、
外部 HTTP 调用和消息确认已经成为一个原子事务。

因此，即使声明 `ExactlyOnce`，应用仍要设计幂等和事务边界。例如，处理端在写入副作用前检查
`idempotency_key` 或 `uid`，并把“已处理”记录与业务写入放入同一事务（在存储能力允许时）。

## Outbox 与 `IdempotencyStore`

Outbox 把“业务事务中产生的待发布事件”保存到应用数据库；后台 dispatcher 再锁定、发布并更新记录状态。
它缩小了“数据库已提交但尚未发布”这个窗口，却不自动把外部发布变成分布式原子提交。

`IdempotencyStore` 是应用实现的去重契约：它按 consumer group 与 message UID 查询 `is_processed`，并在
成功处理后 `mark_processed`。需要并发租约时可实现 `IdempotencyClaimStore` 的 claim / complete / release。
这些都是应用层的持久化设计；eventbus-core 不会自行提供业务副作用的 exactly-once。
