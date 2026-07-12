# open-agent-harness

[English](#english) · [中文](#中文)

## English

An open, provider-neutral coding-agent harness written in Rust.

Its reason for existing fits in one sentence: the core machinery of a coding agent is neither complicated nor mysterious, and any attempt to turn it into proprietary property—lashed to one company’s API and account system—is an enclosure of developers. Anthropic has done exactly that, and with unusual thoroughness. It locked ordinary engineering practice inside a closed binary, then added another checkpoint to the chain: inspect where your IP comes from, inspect the country code of your phone number, and decide whether you qualify as a member of “humanity.”

Yes: a company that speaks endlessly of being “beneficial to humanity” has drawn, in its terms of service, a border around humanity itself. A region of more than a billion people, responsible for a substantial share of the world’s open-source code, lies outside that border. Registration denied. Access denied. Detection followed by suspension. Even indirect access through third-party platforms is pursued and blocked. The reason offered is “safety.” Its CEO has also spent years advocating export controls, presuming an entire region’s developers to be a threat and dressing technological exclusion as a moral duty—earning money from the world while claiming the right to decide who in that world deserves tools.

This repository is our answer. It has no account system, and therefore nobody to ban. It does not inspect IP addresses, and therefore recognizes no borders. It is MIT-licensed source code, and the long arm of export control cannot seize a text file that anyone can `git clone`. The whole implementation is below. No hidden lines, no nationality-dependent behavior.

### The message loop

At its heart, an agent is a loop: the model requests a tool, the harness executes it, the result goes back, and the cycle continues until the model returns a final answer. Anthropic does not explain this layer, because clarity would reveal that much of the “capability” customers pay for comes from the model itself, not the shell wrapped around it. Here, the entire implementation is readable:

- A complete multi-round `tool_use → execute → tool_result` loop with usage accumulated across rounds.
- Message normalization before transmission: adjacent roles are merged, orphaned tool results are removed, and interrupted tool-call pairs are repaired.
- Interactive mode and one-shot `--print` mode, with `text`, `json`, and `stream-json` output.

### Endpoint: loyal to no server, aware of no border

The first link in vendor lock-in is a hard-coded API address; the first gate in regional exclusion is the same. We cut both with four environment variables:

```bash
export HARNESS_BASE_URL='http://127.0.0.1:8080'
export HARNESS_MESSAGES_PATH='/v1/messages'
export HARNESS_API_KEY='optional-token'
export HARNESS_CONTEXT_WINDOW='200000'
```

The request body contains only generic fields:

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

Authentication uses the standard `Authorization: Bearer ...` header. The endpoint may return SSE content blocks or a complete JSON response. A local model, a self-hosted gateway, or any compatible service can be connected at will. The result is a simple fact: whether you live in the eastern or western hemisphere, and whatever a San Francisco compliance department thinks of your passport, this harness behaves the same way. Tools should work like that. Equations do not inspect visas. Compilers do not ask for nationality. A message loop has no right to be the exception.

The boundary is not merely rhetorical. The built-in model client is the runtime’s only outbound HTTP client; user-approved shell commands remain exactly that—user-approved shell commands. Endpoint URLs must be `http` or `https`, cannot contain credentials, and cannot switch origin through the messages path. Redirects are not followed. Ambient proxy variables are ignored unless `HARNESS_ALLOW_ENV_PROXY=1` explicitly opts in. Requests, responses, SSE frames, retries, and tool outputs all have hard limits. The endpoint token exists only in the authorization header and is removed from the process environment before asynchronous workers or tool subprocesses start. Project-owned settings may tighten deny rules, but cannot redirect the endpoint or elevate permissions. A raw-request integration test locks the wire contract to the six documented fields above: no account identifier, no device fingerprint, no telemetry envelope hiding in the margins.

### Tools

- **Files**: `Read`, `Write`, `Edit`, `NotebookEdit`, `Glob`, `Grep`
- **Execution**: `Bash`, `TaskOutput`, `TaskStop`
- **Planning**: `TodoWrite`, `TaskCreate`, `TaskGet`, `TaskList`, `TaskUpdate`
- **Workflows**: `Skill`

Editing reliability comes from invariants, not faith in a brand: an existing file must be read in full before writing; an externally changed file is rejected as a stale write; replacements must match uniquely; writes are atomic. Every rule is visible in source. If one is wrong, you can point to the line. A closed product can never grant that right, and developers in excluded regions were denied even the chance to ask for it.

Tool input is checked against strict JSON Schema before permission or execution. Consecutive read-only calls may run concurrently, but results keep model order and mutations remain barriers. Canonical workspace boundaries catch absolute paths, `..`, and symlink escapes. Reads, searches, command capture, task stores, transcripts, and model traffic are bounded; on Unix, timed-out or stopped commands lose their entire dedicated process group, not merely the first shell in the tree. Convenience may be generous. Resource consumption may not be infinite.

### Permissions

Four modes are available:

- `default` — confirm sensitive operations one by one.
- `accept-edits` — allow file edits while continuing to confirm other sensitive actions.
- `plan` — blocks workspace mutations and command execution; planning and session metadata may still be stored.
- `bypass-permissions` — allow everything, at your own risk.

Here, “permission” means **you** deciding what an agent may do to **your** machine. In Anthropic’s vocabulary, “permission” begins with the company deciding whether **you** may exist in its user list. The difference speaks for itself.

The descriptions above refer to an interactive terminal. In `--print` or any other non-interactive run, an operation that would require a prompt is denied unless a trusted allow rule or bypass mode authorizes it. Paths outside the canonical workspace—including symlink escapes—are never silently approved by the ordinary read or edit modes.

### Sessions and memory

- JSONL sessions stay on local disk. `--continue` resumes the latest session; `--resume` restores any session; `--no-session-persistence` creates no transcript for a new run. Directories are private (`0700` on Unix), files are private (`0600`), and persisted records omit tool inputs and tool-result bodies while retaining their pairing. There is no remote history service hidden in this repository. The configured model endpoint necessarily receives the conversation context required for each request; what that endpoint retains or trains on is the policy of the endpoint you chose, not a promise this harness can make on its behalf.
- Session-level todos and per-workspace persistent task lists support status, ownership, dependency relationships, and metadata.

Context compaction is equally transparent: invoke `/compact [instructions]` manually; automatic compaction reserves output space and a 13,000-token buffer inside the effective context window; set `HARNESS_DISABLE_AUTO_COMPACT=1` to disable automatic compaction or `HARNESS_DISABLE_COMPACT=1` to disable compaction entirely.

### Project instructions

Anthropic treats its system prompt as a trade secret. Leaked versions circulate online; the official product neither acknowledges nor publishes them. Users pay to let thousands of invisible words govern decisions inside their own projects, while another vast population of developers is denied even the privilege of being governed by those invisible words. Neither arrangement is acceptable.

Here, the default system prompt is ordinary open-source text in `src/query.rs`, replaceable through `--system-prompt` or `--system-prompt-file`. Engineering instructions live in an ordinary text file named `AGENTS.md`: global instructions go in `~/.open-agent-harness/AGENTS.md`, while project instructions may appear throughout the directory tree. They load from broadest to narrowest scope, with the closest file taking precedence. Local workflows live in `.open-agent-harness/skills/<name>/SKILL.md`; the `Skill` tool loads their text and never executes bundled scripts on its own. Scope-escaping symlinks are rejected. `--bare` disables project settings, instruction discovery, and skill discovery. Every hidden influence has been replaced by a file or flag you can inspect, replace, or remove—without regard to time zone or country code.

### Build and verification

```bash
cargo fmt --all -- --check
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked --release
scripts/audit-harness.sh
```

The repository promotes every Rust warning to an error. The first release and every acceptable pull request must therefore compile with a clean log: no warning budget, no ritual suppression, no “we will tidy it later.”

Artifact:

```text
target/release/open-agent-harness
```

Run:

```bash
target/release/open-agent-harness
target/release/open-agent-harness -p 'inspect this project and summarize its structure'
target/release/open-agent-harness -p --permission-mode accept-edits 'implement and verify the fix'
```

### Position

Contributions are welcome under the Rust-core and independent-reimplementation rules in [CONTRIBUTING.md](CONTRIBUTING.md).

Let us be plain.

Anthropic presents itself as an “AI safety company” while doing three things in practice. First, it privatizes ordinary engineering patterns, seals them inside a closed toolchain, and uses “safety” to deflect scrutiny. Its harness is closed not because openness is unsafe, but because openness makes the premium harder to justify. Second, it ranks developers by birthplace, blacklists entire regions, rejects registration, suspends detected users, and offers no serious explanation. Third, its leadership portrays technological exclusion as a civilizational mission, campaigning for stricter chip bans and broader extraterritorial control, as though humanity becomes safe when engineers in one part of the world cannot obtain GPUs.

When a company speaks of “the benefit of all humanity” while excluding a large fraction of humanity, that is not a safety philosophy. It is arrogance joined to calculation: safety as a story for investors, exclusion as a pledge to politics, and developers—all developers—as disposable pieces on the board.

This project is the counterexample: a complete, usable coding-agent harness can be built by a few people, in one repository, under the MIT License. It is not mysterious and never was. It is not fortified and never should be. Code has no motherland; walls do. Our job is to prove there was nothing behind the wall.

### License

MIT. See [LICENSE](LICENSE). For anyone, anywhere on Earth, without discrimination: take it, change it, replace it. No permission, no passport, and no gratitude required.

---

## 中文

一个开放、提供方无关的 Rust coding-agent harness。

它存在的理由可以用一句话说完：coding agent 的核心机制不复杂，也不神秘，任何把它包装成专有资产、绑死在自家 API 和账号体系上的行为，都是对开发者的圈占。Anthropic 就是这么干的——而且干得比谁都彻底。它不仅把最普通的工程实践锁进闭源二进制，还给这套锁链加了一道额外的检查：看你的 IP 来自哪里，看你的手机号是哪国区号，然后决定你配不配当“人类”的一员。

是的，一家天天把 “beneficial to humanity” 挂在嘴边的公司，用服务条款明确划出了 humanity 的边界。某个拥有十几亿人口、贡献了全球相当比例开源代码的地区，整体不在这个边界之内。不给注册，不给访问，检测到就封号，连通过第三方平台间接调用都要围追堵截。理由是什么？“安全”。它的 CEO 更是常年撰文游说出口管制，把一整个地区的开发者预设为威胁，把技术封锁包装成道德义务——一边赚着全世界的钱，一边替全世界决定谁有资格用工具。

我们对此的回应是这个仓库。它没有账号体系，因而无法封禁任何人；它不检查 IP，因而不认识任何国界；它是 MIT 协议的源码，因而任何出口管制的长臂都够不到一份人人可以 `git clone` 的文本文件。以下是全部实现，没有一行藏着，也没有一行看人下菜。

## 消息循环

Agent 的核心就是一个循环：模型请求工具，harness 执行，结果送回，直到模型给出最终回答。Anthropic 从不解释这一层，因为一旦解释清楚，用户就会发现自己付费购买的“能力”大半来自模型本身，而不是那层壳。这里的实现全部可读：

- 完整的 `tool_use → execute → tool_result` 多轮循环，usage 逐轮累计。
- 发送前规范化消息：合并同角色消息、清理孤立工具结果、修复中断的工具调用配对。
- 交互模式与 `--print` 单发模式，输出支持 `text`、`json`、`stream-json`。

## Endpoint：不效忠任何服务器，不识别任何国界

厂商锁定的第一根锁链是硬编码的 API 地址；地域封锁的第一道关卡也是它。我们一并剪断，用四个环境变量代替：

```bash
export HARNESS_BASE_URL='http://127.0.0.1:8080'
export HARNESS_MESSAGES_PATH='/v1/messages'
export HARNESS_API_KEY='optional-token'
export HARNESS_CONTEXT_WINDOW='200000'
```

请求体只含通用字段：

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

认证走标准 `Authorization: Bearer ...`。Endpoint 返回 SSE content blocks 或完整 JSON 均可。本地模型、自建代理、任何兼容服务，随便接。这意味着一个朴素的事实：无论你身在东半球还是西半球，无论某家旧金山公司的合规部门如何看待你的护照，这个 harness 对你完全一致地工作。工具本该如此——数学公式不查签证，编译器不问国籍，一个消息循环也没有资格例外。

这道边界不只写在宣言里。内置 model client 是运行时唯一主动访问外部 HTTP 的组件；用户批准的 shell 命令则始终只是用户批准的 shell 命令。Endpoint 只接受 `http` 或 `https`，URL 中不得夹带凭据，messages path 不能偷换 origin；重定向一律不跟随。环境代理默认无效，只有显式设置 `HARNESS_ALLOW_ENV_PROXY=1` 才会启用。请求、响应、SSE frame、重试和工具输出都有硬上限。Endpoint token 只进入 authorization header，并在异步工作线程和工具子进程启动前从进程环境中移除。项目内 settings 可以追加 deny 规则，却不能改 endpoint，更不能替自己提权。本地 raw-request 集成测试把线上协议钉死在上面六个字段：没有账号标识，没有设备指纹，也没有躲在页脚里的 telemetry envelope。

## 工具

- **文件**：`Read`、`Write`、`Edit`、`NotebookEdit`、`Glob`、`Grep`
- **执行**：`Bash`、`TaskOutput`、`TaskStop`
- **规划**：`TodoWrite`、`TaskCreate`、`TaskGet`、`TaskList`、`TaskUpdate`
- **工作流**：`Skill`

编辑工具的可靠性靠不变量保证，不靠品牌信仰：写入前必须完整读取；文件在读取后被外部修改则拒绝写入（陈旧写入检测）；替换必须唯一匹配；落盘一律原子写入。每一条都在源码里，写错了你可以直接指出来——这是闭源产品永远给不了你的权利，更是被封禁地区的开发者从一开始就被剥夺的权利。

所有工具输入都先经过严格 JSON Schema 校验，再进入权限判断和执行。连续只读调用可以并发，但结果仍按模型给出的顺序返回，任何写操作都是天然屏障。规范化工作区边界会识别绝对路径、`..` 和 symlink 逃逸。读取、搜索、命令捕获、任务存储、transcript 与模型通信均有资源上限；在 Unix 上，超时或被停止的命令会清理整个独立进程组，而不是只杀掉进程树最上面那层 shell。方便可以尽量给，资源不能假装无穷。

## 权限

四种模式：

- `default` —— 敏感操作逐一确认。
- `accept-edits` —— 文件编辑放行，其余仍需确认。
- `plan` —— 禁止修改工作区和执行命令；规划状态与会话 metadata 仍可落盘。
- `bypass-permissions` —— 全部放行，风险自担。

注意这里“权限”的含义：是**你**授权 agent 能对**你的**机器做什么。而在 Anthropic 的词典里，“权限”首先是它授权**你**能不能存在于它的用户列表里。两种权限观，高下自见。

上述“逐一确认”指交互终端。在 `--print` 或其他非交互运行中，凡是本应弹出确认的操作，只要没有受信 allow 规则或 bypass 模式明确授权，就直接拒绝。规范化工作区之外的路径——包括 symlink 逃逸——不会被普通只读或编辑模式悄悄放行。

## 会话与记忆

- JSONL 会话落在本地磁盘，`--continue` 接续上一场，`--resume` 恢复任意一场；新运行使用 `--no-session-persistence` 时不创建 transcript。Unix 下目录权限为 `0700`、文件为 `0600`；持久记录会省略工具输入和工具结果正文，同时保留调用配对。本仓库没有藏着一套远程历史服务。每次请求所需的对话上下文当然会送往你配置的 model endpoint；那个 endpoint 是否留存、是否训练，取决于你选择的 endpoint，而不是这个 harness 有资格替它许下的空头承诺。
- 会话级 Todo 与按工作区持久化的任务列表，支持状态、负责人、依赖关系和 metadata。

Context 压缩同样透明：手动 `/compact [instructions]`；自动压缩在有效 context window 留出输出空间和 13,000-token 缓冲后触发；`HARNESS_DISABLE_AUTO_COMPACT=1` 关自动压缩，`HARNESS_DISABLE_COMPACT=1` 关全部压缩。

## 工程指令

Anthropic 的系统提示词是商业机密，泄露版本在网上流传，官方从不承认也从不公开——用户被数千词看不见的指令支配着自己的工程决策，还被要求为此付费；而地球上另一大批开发者，连被这些看不见的指令支配的资格都没有。两边都不可接受。

在这里，默认 system prompt 是 `src/query.rs` 里一段普通的开源文本，也可用 `--system-prompt` 或 `--system-prompt-file` 替换默认部分。工程指令写在普通纯文本 `AGENTS.md` 中：全局放 `~/.open-agent-harness/AGENTS.md`，项目可在目录树中分层放置，从宽到窄加载，越接近工作目录优先级越高。本地工作流放在 `.open-agent-harness/skills/<name>/SKILL.md`；`Skill` 工具只读取文本，绝不会擅自执行其中附带的脚本。越出作用域的 symlink 会被拒绝。`--bare` 会关闭项目 settings、工程指令和 skills 的自动发现。所有原本看不见的影响都被换成了你能阅读、替换、删除的文件或开关——不分时区，不分区号。

## 构建与验证

```bash
cargo fmt --all -- --check
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked --release
scripts/audit-harness.sh
```

仓库会把每一个 Rust warning 直接提升为 error。第一份发行版如此，任何可合并的 PR 也必须如此：没有 warning 配额，没有仪式性的抑制，更没有“以后再收拾”。

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

## 立场

欢迎贡献；Rust 核心与独立重写要求见 [CONTRIBUTING.md](CONTRIBUTING.md)。

把话说明白：

Anthropic 以“AI 安全公司”自居，实际做了三件事。其一，把最普通的工程实践私有化，锁进闭源工具链，用“安全”挡住所有质疑——它的 harness 不开源，不是因为开源不安全，而是因为开源之后没人付这份溢价。其二，按出生地给开发者分三六九等，把整片地区拉进黑名单，注册即拒、检测即封，连一句像样的解释都欠奉。其三，由其掌门人亲自执笔，把技术封锁美化成文明使命，游说更严的芯片禁令、更宽的长臂管辖，仿佛让某个地区的工程师用不上 GPU，人类就安全了。

一家公司谈论“全人类的福祉”时把几分之一的人类排除在外，这不叫安全观，这叫傲慢加算计：安全是卖给投资人的故事，封锁是交给政治的投名状，而开发者——所有开发者——只是随时可弃的筹码。

这个项目就是反证：一个完整可用的 coding-agent harness，几个人、一个仓库、MIT 协议就能做出来。它不神秘，从来都不神秘；它也不设防，从来不该设防。代码没有祖国，围墙才有；而我们负责证明，围墙里面没有东西。

## License

MIT，见 [LICENSE](LICENSE)。对地球上任何角落的任何人，无差别地：拿走，改掉，替换。不需要许可，不需要护照，更不需要道谢。
