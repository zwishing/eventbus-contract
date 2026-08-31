---
title: 错误参考
weight: 30
---

`EventBusError` 是 `#[non_exhaustive]` 枚举；应用匹配时应保留 `_` 分支，以便将来的兼容新增。

| Variant | 用途 |
|---|---|
| `Internal` | 库内部不变量、任务或通道等无法归入更具体类别的问题。 |
| `Validation` | 配置、消息或 option 不满足 contract。 |
| `Serialization` | JSON、Redis field 或其它 wire-format 编解码失败。 |
| `InvalidTransition` | 状态机不允许的 `from -> to` 转换。 |
| `Connection` | 连接建立或连接状态错误。 |
| `Timeout` | 操作超过约定的时间界限。 |
| `Source` | 后端、codec 或依赖返回的底层错误，并带有可读 context。 |

## 保留因果链

包装底层错误时使用 `EventBusError::source(context, error)`，不要把错误塞进
`Internal(format!(...))`。`Source` 保留 `std::error::Error::source()` 链，因此日志、tracing 和
观察系统可以同时显示外层操作语境和原始原因。调用方也可以沿 source chain 判断连接、协议或 I/O
失败，而不必从字符串反推原因。
