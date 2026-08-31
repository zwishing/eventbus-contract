---
title: 消息信封
weight: 10
---

`Message` 是总线传递的标准信封：路由和可观测性信息在信封中，业务内容在 `payload` 中。后端可以为
消息分配传输层标识，但业务端用 `uid` 识别同一条业务消息。

## 必填信息

| 字段 | 含义 |
|---|---|
| `uid` | 应用生成的消息唯一标识。 |
| `topic` | 路由到的主题。 |
| `key` | 业务分组键；需要按键顺序时使用它。 |
| `kind` | 事件类型或名称。 |
| `source` | 产生消息的服务或组件。 |
| `occurred_at` | 业务事件发生时间。 |
| `headers` | 传输和跨语言互操作用的字符串元数据。 |
| `payload` | 不透明的字节序列。 |

`payload` 是不透明 bytes：核心库不会替应用解释 JSON、Protobuf 或其他格式。应用负责定义 schema、
兼容策略和版本演进；可用 `content_type` 与 `event_version` 把这两个约定写进信封。

## 可选的协议与关联字段

| 字段 | 用途 |
|---|---|
| `content_type` | payload 的媒体类型或编码约定。 |
| `event_version` | 事件 schema 版本。 |
| `idempotency_key` | 应用用来识别重复业务操作的键。 |
| `expires_at` | 消息过期时间；如何处置过期消息由应用和后端策略决定。 |
| `trace_uid` | 链路或业务追踪标识。 |
| `correlation_uid` | 将请求、事件和后续响应关联起来的标识。 |

核心库还提供标准 header 名称。接收端会把 `content-type`、`event-version` 与
`idempotency-key` header 补到相应的 typed fields（字段尚未设置时），因此应用应统一从字段读取，
并将 header 视为跨语言线上的镜像。

## 一个实用边界

信封让发布者和消费者拥有共同的最小上下文，但它不替应用规定 payload 的模型，也不自动保证业务
去重。把领域事件的 schema、兼容性规则、认证信息和敏感数据处理放在应用边界，而不是假定总线会
替你完成这些决定。
