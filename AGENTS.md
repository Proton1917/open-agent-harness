# agent-harness engineering guide

## Scope

- 本项目只实现开放、提供方无关的 Rust coding-agent harness。
- `reference/` 只读，仅用于核对通用执行逻辑；不得修改或打包进成品。
- 不引入厂商身份、账号、订阅、遥测、专属 UI 或品牌提示词。
- 工程指令入口统一为 `AGENTS.md`；运行时不得发现或生成其他命名的指令文件。

## Invariants

- 保持 JSON Schema 工具注册、权限先于执行、完整读取后才能覆盖现有文件、陈旧内容拒绝、原子写入，以及 `tool_use/tool_result` 的严格顺序。
- Endpoint 通过 `HARNESS_BASE_URL`、`HARNESS_MESSAGES_PATH` 和可选 `HARNESS_API_KEY` 配置。
- 新功能必须同时覆盖成功路径和失败/边界路径；网络协议使用本地 mock server 测试。
- 禁止在源码、测试、日志或配置中写入真实 secret。

## Required checks

```bash
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo build --release
scripts/audit-harness.sh
```

工程日志应直接整合到本文件，保持简短；当前核心 harness、8 个工具、SSE 流、权限、会话和分层 `AGENTS.md` 已迁移。
