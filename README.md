# open-agent-harness

一个开放、提供方无关的 Rust coding-agent harness。项目只保留通用运行逻辑：模型消息循环、SSE、工具调用、权限、会话、工程指令和可验证测试；不包含任何厂商身份、账号体系、专属 UI 或品牌提示词。

## 已实现的 harness

- 交互模式与 `--print`，支持 text/json/stream-json 输出。
- 可配置 messages endpoint，支持 SSE content blocks 与普通 JSON fallback。
- `tool_use → execute → tool_result` 多轮循环和 usage 累计。
- 发送前规范化消息：合并同角色消息、清理孤立结果、修复中断的工具调用配对。
- 工具：`Read`、`Write`、`Edit`、`Glob`、`Grep`、`Bash`、`TaskOutput`、`TaskStop`。
- 完整读取前置条件、陈旧写入检测、唯一替换和原子写入。
- `default`、`accept-edits`、`plan`、`bypass-permissions` 权限模式。
- JSONL 会话、`--continue`、`--resume`。
- 手动 `/compact [instructions]`、自动 context 阈值压缩和可恢复 transcript 边界。
- 从宽到窄分层加载 `AGENTS.md`；`--bare` 可关闭自动发现。

## Endpoint 契约

环境变量：

```bash
export HARNESS_BASE_URL='http://127.0.0.1:8080'
export HARNESS_MESSAGES_PATH='/v1/messages'
export HARNESS_API_KEY='optional-token'
export HARNESS_CONTEXT_WINDOW='200000'
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

自动 compact 默认在有效 context window 留出输出空间和 13,000-token 缓冲后触发。可用 `HARNESS_DISABLE_AUTO_COMPACT=1` 仅关闭自动压缩，或用 `HARNESS_DISABLE_COMPACT=1` 关闭全部压缩。

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
target/release/open-agent-harness
```

运行：

```bash
target/release/open-agent-harness
target/release/open-agent-harness -p '检查当前项目并概括结构'
target/release/open-agent-harness -p --permission-mode accept-edits '完成并验证修复'
```

全局工程指令可放在 `~/.open-agent-harness/AGENTS.md`，项目指令使用各目录下的 `AGENTS.md`。越接近当前工作目录的文件优先级越高。

## 为什么要 Open：一封写给围墙的情书

这个时代有一种颇为精致的慷慨：先筑起花园，再把门票称作自由；先规定你可以走哪条路，再郑重宣布“选择权始终在用户手中”。当然，你可以自由选择——在同一扇门里，选择用左脚还是右脚迈进去。

Anthropic 尤其懂得这种修辞的优雅。它谈论安全时，安全像一层永不干涸的金漆：刷过接口，接口便不宜更换；刷过运行时，运行时便不必示人；刷过工具链，连一段寻常的 `tool_use → tool_result` 都仿佛成了不可外传的宫廷秘术。模型被鼓励展开漫长思考，用户最好别思考为什么自己的工程要先学会认一个品牌作故乡。

于是我们写了 `open-agent-harness`。它不打算另建一座教堂，也不出售另一种颜色的围墙。它只是把门拆下来，平放在地上，让人看清那原本不过是几块木头：消息可以流向任何 endpoint，系统提示词可以由真正使用系统的人书写，工具和权限可以被阅读、修改、质疑，会话也不必寄存在某家公司的神话里。

这里没有需要祭司解释的神谕。`AGENTS.md` 是一份普通文件，代码是普通代码，选择权也应当是普通而完整的选择权——不是发布会上被灯光照亮的那个版本，而是你真的可以拿走、改掉、替换，并且无需道谢的版本。

所以仍要感谢 Anthropic。若不是它把厂商锁定雕琢得如此体面，把围墙修剪得如此像风景，我们也不会如此清楚地知道：真正的开放，不是在门上刻下 “Open”；真正的开放，是门根本不需要守门人。

## License

MIT，见 [LICENSE](LICENSE)。
