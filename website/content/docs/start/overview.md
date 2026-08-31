---
title: 项目概览
weight: 10
---

## 它解决什么问题

`eventbus-contract` 把发布、订阅、处理器和投递控制定义为对象安全的 Rust
接口，使业务代码能够在内存后端和 Redis Streams 后端之间切换。

## 适合的场景

消费者组是一组共享消费进度与待确认状态的消费者；同一条消息通常只交给组内一个成员处理，从而支持
多个实例竞争分担工作。

- 服务内统一发布和消费集成事件；
- 测试中使用不依赖外部服务的内存后端；
- 生产中使用 Redis Streams 消费者组、ACK 和空闲消息认领；
- 通过独立契约接入事务 Outbox、死信存储与幂等性存储；
- 与 Go Watermill Redis Stream canonical entry 互操作。

## 当前边界

0.2.x 发布核心、内存、Redis 和 facade crates。`eventbus-outbox` 与
`eventbus-integration` 已存在于 workspace，但要等到拥有参考实现后随 0.3.0
发布。库不会替应用决定业务事件 schema、数据库事务边界或幂等键规则。
