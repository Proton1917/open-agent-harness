# Harness logic audit

## 成品范围

目标是一个纯 Rust、开放且提供方无关的 coding-agent harness。完成范围限定为：

1. CLI 输入输出与开放系统提示。
2. 通用 messages endpoint、SSE/JSON 消息重建和错误重试。
3. 模型工具注册、权限判断、执行与结果回传循环。
4. 本地文件、搜索、shell 和后台任务工具。
5. 会话持久化、恢复和工程指令发现。
6. 行为测试、静态检查和 release 产物。

不迁移厂商账号、订阅、遥测、品牌 UI、远程专属服务、语音角色或营销功能。

## 覆盖矩阵

| Harness 子系统 | Rust 对应 | 状态 | 证据 |
|---|---|---:|---|
| CLI 与输出格式 | `src/main.rs`, `src/cli.rs` | 已实现 | help/version smoke |
| messages endpoint | `src/api.rs` | 已实现 | mock SSE query-loop |
| query/tool loop | `src/query.rs` | 已实现 | 分块 tool input 与 result round trip |
| 工具 registry | `src/tools/mod.rs` | 已实现 | JSON Schema 与真实执行测试 |
| 文件工具 | `src/tools/read.rs`, `edit.rs`, `write.rs` | 已实现 | 陈旧拒绝、partial-read 拒绝、原子写入 |
| 搜索工具 | `src/tools/glob.rs`, `grep.rs` | 已实现 | 临时目录真实匹配 |
| shell/background | `src/tools/bash.rs`, `tasks.rs` | 已实现 | stdout/stderr/exit 测试 |
| 权限 | `src/permissions.rs` | 已实现 | deny precedence、非交互拒绝 |
| settings | `src/config.rs` | 已实现 | 递归 merge 测试 |
| session | `src/session.rs` | 已实现 | JSONL resume 路径 |
| `AGENTS.md` 指令 | `src/context.rs` | 已实现 | 分层顺序与 bare 模式测试 |

## 完成门槛

```bash
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo build --release
scripts/audit-harness.sh
target/release/agent-harness --help
target/release/agent-harness --version
```

审计脚本必须证明 Rust 成品和工程文档中不存在被移除的厂商品牌字符串；原始 `reference/` 快照只读且不属于成品扫描范围。
