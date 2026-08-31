---
title: 发布流程
weight: 40
---

发布由 [`.github/workflows/publish.yml`](https://github.com/mapseekai/eventbus-contract/blob/main/.github/workflows/publish.yml)
在 GitHub Release 发布后触发。工作流 checkout 对应 tag，并要求 release tag 与 workspace version 严格相等：
例如版本 `0.2.1` 必须使用 `v0.2.1`。

## 发布清单

1. 更新并审核 [`CHANGELOG.md`](https://github.com/mapseekai/eventbus-contract/blob/main/CHANGELOG.md)，破坏性变更同时更新 [`MIGRATION-0.2.md`](https://github.com/mapseekai/eventbus-contract/blob/main/MIGRATION-0.2.md)。
2. 在 tag 对应提交上运行 workspace 测试和 Redis Watermill feature 测试。
3. 确认 crates.io token 可用；工作流先 dry-run 再真正发布。
4. 按内部 path dependency 顺序发布：`eventbus-core` → `eventbus-memory` → `eventbus-redis` → `eventbus-contract`。
5. 发布后检查 crates.io 与 docs.rs 的版本页面，再发布文档站点。

文档部署独立于 crate publish：网站可以独立构建和部署，但对已发布 API 的链接、版本说明和迁移内容必须
与 release tag 对齐。不要把 workspace-only 的 outbox 或 integration crate 当作已经发布的 facade feature。
