---
title: 投递与完成
weight: 30
---

订阅的 `AckMode` 决定消息在 handler 前、后或由 handler 本身何时完成。选择时先问：业务失败后，
这条消息是否必须仍有机会重试？

| Mode | Finalization | Use |
|---|---|---|
| `Manual` | Handler calls `ack`, `nack`, or `retry`. | Business code controls success. |
| `AutoOnReceive` | Before handler execution. | Handler-failure loss is acceptable. |
| `AutoOnHandlerSuccess` | After handler returns `Ok`. | Common at-least-once handling. |

## 手动完成

`DeliveryControl` 的 `ack`、`nack` 和 `retry` 都接收 `self: Box<Self>`。调用一个控制方法就消费这个
handle，编译器阻止同一投递随后再走另一个完成分支。

- `ack` 确认当前投递。
- `nack` 不进入重试循环；配置了 `dead_letter_topic` 时，先发布到死信主题，发布成功后才确认原消息。没有配置 dead_letter_topic 时，nack 会确认并丢弃原消息。
- `retry` 在未耗尽预算时将消息重新发布，并写入重试 header；达到 `max_retry` 后改为发布到死信主题。两种路径都只会在发布成功后确认原消息。

如果死信发布或重试发布失败，控制方法会在确认原消息之前返回错误；原消息仍处于 pending，之后可以被
reclaim 并再次投递。

`max_retry` 表示**初次投递之外**允许的重试次数：`max_retry = 0` 时，第一次失败就已耗尽预算。若
重试耗尽而没有 `dead_letter_topic`，当前 `StreamBus` 会返回配置验证错误，而不是伪装成已经死信。

## 自动模式的边界

`AutoOnReceive` 在执行 handler 之前确认消息，因此 handler panic 或返回错误后的丢失是该模式的明确
代价。`AutoOnHandlerSuccess` 只会在 `handle` 返回 `Ok` 后自动确认；handler 已主动完成 delivery 时，
自动完成代理会避免第二次完成。

自动确认改善控制流，但不能替代业务幂等：处理结果已写入数据库、随后确认失败时，消息仍可能再次
投递。下一页说明如何把这个现实纳入保证和事务设计。
