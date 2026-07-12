# agent-harness

一个开放、提供方无关的 Rust coding-agent harness。项目只保留通用运行逻辑：模型消息循环、SSE、工具调用、权限、会话、工程指令和可验证测试；不包含任何厂商身份、账号体系、专属 UI 或品牌提示词。

## 已实现的 harness

- 交互模式与 `--print`，支持 text/json/stream-json 输出。
- 可配置 messages endpoint，支持 SSE content blocks 与普通 JSON fallback。
- `tool_use → execute → tool_result` 多轮循环和 usage 累计。
- 工具：`Read`、`Write`、`Edit`、`Glob`、`Grep`、`Bash`、`TaskOutput`、`TaskStop`。
- 完整读取前置条件、陈旧写入检测、唯一替换和原子写入。
- `default`、`accept-edits`、`plan`、`bypass-permissions` 权限模式。
- JSONL 会话、`--continue`、`--resume`。
- 从宽到窄分层加载 `AGENTS.md`；`--bare` 可关闭自动发现。

## Endpoint 契约

环境变量：

```bash
export HARNESS_BASE_URL='http://127.0.0.1:8080'
export HARNESS_MESSAGES_PATH='/v1/messages'
export HARNESS_API_KEY='optional-token'
```

Harness 向 endpoint 发送以下通用字段：

```json
{
  "model": "default",
  "max_tokens": 16384,
  "system": "...",
  "messages": [],
  "tools": [],
  "stream": true
}
```

认证 token（如有）使用 `Authorization: Bearer ...`。Endpoint 可返回 `text/event-stream`，也可返回完整 JSON 消息。

## 构建与验证

```bash
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo build --release
scripts/audit-harness.sh
```

产物：

```text
target/release/agent-harness
```

运行：

```bash
target/release/agent-harness
target/release/agent-harness -p '检查当前项目并概括结构'
target/release/agent-harness -p --permission-mode accept-edits '完成并验证修复'
```

全局工程指令可放在 `~/.agent-harness/AGENTS.md`，项目指令使用各目录下的 `AGENTS.md`。越接近当前工作目录的文件优先级越高。

## License

MIT，见 [LICENSE](LICENSE)。
