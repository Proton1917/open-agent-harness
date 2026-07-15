# open-agent-harness

[English](#english) · [中文](#中文)

## English

An open, provider-neutral coding-agent harness written in Rust.

The compatibility target is the provider-neutral general behavior visible in
the static 2026-03-31 comparison snapshot, plus selected provider-neutral
extensions identified in a later checksummed archive review. This is an
independent behavioral implementation: it does not claim identical source
structure, prompts, wording, assets, account behavior, or vendor-specific
features.

Its reason for existing fits in one sentence: the core machinery of a coding agent is neither complicated nor mysterious, and any attempt to turn it into proprietary property—lashed to one company’s API and account system—is an enclosure of developers. Anthropic has done exactly that, and with unusual thoroughness. It locked ordinary engineering practice inside a closed binary, then added another checkpoint to the chain: inspect where your IP comes from, inspect the country code of your phone number, and decide whether you qualify as a member of “humanity.”

Yes: a company that speaks endlessly of being “beneficial to humanity” has drawn, in its terms of service, a border around humanity itself. A region of more than a billion people, responsible for a substantial share of the world’s open-source code, lies outside that border. Registration denied. Access denied. Detection followed by suspension. Even indirect access through third-party platforms is pursued and blocked. The reason offered is “safety.” Its CEO has also spent years advocating export controls, presuming an entire region’s developers to be a threat and dressing technological exclusion as a moral duty—earning money from the world while claiming the right to decide who in that world deserves tools.

None of that premise depends on rumor. The company publishes the [countries it supports](https://www.anthropic.com/supported-countries), says it uses [IP-derived location to enforce its terms](https://privacy.anthropic.com/en/articles/11186740-does-claude-use-my-location), requires [physical presence and a phone number from a supported location](https://support.anthropic.com/en/articles/8325609-how-do-i-sign-up-for-claude-pro), has announced restrictions reaching [foreign subsidiaries by ownership](https://www.anthropic.com/news/updating-restrictions-of-sales-to-unsupported-regions), and publicly argues for [stronger semiconductor export controls](https://www.anthropic.com/news/securing-america-s-compute-advantage-anthropic-s-position-on-the-diffusion-rule). The indictment here is an interpretation; its premises are their own publications.

This repository is our answer. It has no account system, and therefore nobody to ban. It neither collects nor geofences users by source IP, and therefore recognizes no borders; network tools still validate destination addresses to prevent SSRF. It is MIT-licensed source code, and the long arm of export control cannot seize a text file that anyone can `git clone`. The whole implementation is below. No hidden lines, no nationality-dependent behavior.

### Current implementation status

The declared provider-neutral surface has now been aligned against the static
2026-03-31 comparison snapshot and supplemented with the generally useful open
features identified in the checksummed 2.1.207 archive review. This includes
the transactional model/tool loop, permission and filesystem boundaries,
sessions and control streaming, local agents and teams, Skills and plugins,
hooks, MCP/OAuth/WebSocket, LSP, scheduling, monitoring, declarative workflows,
media, structured interaction, and opt-in local memory.

The release gate currently contains 776 test entries: 772 pass and four are
intentionally ignored helper-process entry points exercised by their parent
tests. Formatting, Rust 1.85 all-target checks, Clippy with warnings denied,
the release build, the repository audit, and the Windows GNU cross-check all
pass; the same test and Clippy gate also runs natively on macOS and Windows in
CI. The exact behavior matrix, evidence, and deliberate exclusions are in
[the migration audit](MIGRATION.md) and the
[provider-neutral parity audit](docs/GENERIC_PARITY_2026-03-31.md). This status
does not weaken the criticism above and does not turn behavioral compatibility
into a claim that proprietary source, prompts, assets, accounts, or hosted
services were copied.

### The message loop

At its heart, an agent is a loop: the model requests a tool, the harness executes it, the result goes back, and the cycle continues until the model returns a final answer. Anthropic does not explain this layer, because clarity would reveal that much of the “capability” customers pay for comes from the model itself, not the shell wrapped around it. Here, the entire implementation is readable:

- A complete multi-round `tool_use → execute → tool_result` loop with usage accumulated across rounds.
- Message normalization before transmission: adjacent roles are merged, orphaned tool results are removed, and interrupted tool-call pairs are repaired.
- Interactive mode and one-shot `--print` mode, with `text`, `json`, and `stream-json` output. `--input-format stream-json` keeps a bounded bidirectional NDJSON control session open for user messages, permission/question replies, interruption, model or permission changes, context queries, and file rewind. Separate control/`now`/`next`/`later` lanes preserve priority without an unbounded hidden buffer; accepted-message replay acknowledgements, queued UUID cancellation, interruption receipts, command lifecycle events, hook events, and opt-in tool-free prompt suggestions are explicit protocol capabilities.
- `--json-schema` installs a final `StructuredOutput` tool backed by the full JSON Schema validator, requires exactly one valid structured result, and returns that value separately from the assistant text.

### A terminal that behaves like an agent, not a teletype

The interactive CLI is an original Rust implementation of the conversational terminal pattern: scrollback remains ordinary terminal history, while a bordered composer stays at the bottom of each input turn. Its raw-mode frames use explicit CRLF, a single buffered write, and synchronized-output markers where supported; resize repaints only the owned composer instead of erasing the visible conversation, input height is viewport-bounded, and Ctrl-G or Unix job-control suspend restores raw/alternate-screen state before handing the terminal away. Editing is grapheme-aware for CJK, combining marks, flags, and ZWJ emoji, with multiline navigation, private persistent history, `Ctrl-R` reverse search, `Ctrl-S` session/project/everywhere scope cycling, a bounded kill ring and `Alt-Y`, undo, prompt stash, external-editor handoff, and the usual Ctrl/Option word and line operations. Hot-reloaded user keybindings support contexts, chords, and explicit unbinding; `/vim` supplies Insert, Normal, Visual, and Visual Line editing without changing the non-TTY protocol. `\`+Enter, Shift/Option+Enter, and `Ctrl-J` insert a newline. `Shift+Tab` cycles the safe interactive modes without ever entering bypass mode. Double Esc saves a nonempty draft before clearing it and opens checkpoint rewind from an empty composer. The `Ctrl-C`/`Ctrl-D` double-press window is 800 ms; nonempty `Ctrl-D` forward-deletes one grapheme. A permission prompt can allow once, deny, interrupt, or remember only the exact normalized action for the current process; it shows bounded action/edit-diff summaries plus the complete exact JSON, while deny rules and Plan mode still win. Non-TTY input keeps the plain line protocol, and `--print` retains its exact machine-readable formats.

Typing `/` immediately opens a bounded command palette assembled from built-ins, trusted custom commands, current user-invocable Skills, and namespaced prompts advertised by connected MCP servers. Further typing filters the list; Up/Down or Ctrl-N/Ctrl-P wraps selection, Tab completes without executing, Enter accepts (argument-taking entries wait for arguments), and Esc dismisses the palette until the input changes. At most six centered suggestions are painted in the normal composer, including trusted dynamic argument candidates. The live catalog includes status, task, Skill, hook, memory, MCP, sandbox and plugin views plus `/doctor`, `/terminal-setup`, `/diff`, `/rewind`, `/resume`, `/rename`, `/branch`, `/transcript`, `/config`, `/theme`, `/statusline`, `/copy`, and `/export`; `/model` or Alt-P opens a provider-neutral picker sized to the current terminal. The theme picker covers auto, dark/light, daltonized and ANSI variants, previews a bounded diff sample, toggles syntax highlighting with Ctrl-T, and rolls back on Escape. A trusted status line refreshes asynchronously when model/mode/Vim state changes and at its configured idle interval. `Ctrl-O` opens a bounded alternate-screen transcript with compact/show-all views, scrolling, case-insensitive regex search, per-occurrence match navigation, and an explicit dump back to native scrollback. Assistant Markdown is rendered from a control-sanitized structured IR with headings, lists, quotes, fenced code, bounded tables, optional syntax highlighting and credential-free HTTP(S) links; fullscreen clicks can open those links, expand bounded tool output, or open a canonical file only when it remains inside a trusted workspace. `/tui` enters a bounded full-screen transcript with mouse and grapheme-aware keyboard scrolling/selection plus native or bounded OSC 52 clipboard copy while preserving the primary terminal screen. `/permissions`, `/config`, and `/tasks` open searchable alternate-screen dialogs; permission changes persist only in private user settings, while task stop/output actions still pass through the tool boundary. Clipboard image paste creates bounded navigable attachments. Typing `@` opens an ignore-aware workspace-file picker with prefix, basename, substring and fuzzy ranking, including correct whole-token replacement when the cursor is in the middle of a quoted or unquoted reference. Quoted paths and optional `#Lstart-Lend` line ranges are accepted, while explicit text and media attachment totals remain bounded. `/mcp enable|disable|reconnect <server>` changes only already trusted server definitions and refreshes discovery. `/clear` first archives the current conversation; `/copy [N]` copies a bounded assistant response, and `/export [file]` exports public transcript content without private reasoning or silent overwrite.

An input beginning with `!` uses a distinct shell composer and runs through the same schema, hook, permission, sandbox, timeout, capture, and process-tree path as the Bash tool; unlike the proprietary product, it never bypasses the user's permission policy. Its bounded output is shown and supplied to the next model turn, Tab completes from this session's shell history, and idle composers now yield to ready scheduled prompts while stashing a draft for `Ctrl-S` recovery. Stream-JSON clients execute every advertised built-in slash command locally and receive structured `command_result` events instead of accidentally sending `/clear`, `/status`, or `/rewind` to the model.

The interface follows observable terminal behavior, not copied code or assets. There is no JavaScript renderer hiding behind the binary: input editing, cursor control, event rendering, interruption, and mode state live in `src/terminal.rs`, while engine events come from `src/query.rs`.

### Endpoint: loyal to no server, aware of no border

The first link in vendor lock-in is a hard-coded API address; the first gate in regional exclusion is the same. We cut both with a small, explicit transport configuration:

```bash
export HARNESS_BASE_URL='http://127.0.0.1:8080'
export HARNESS_API_PATH='/v1/chat/completions'
export HARNESS_API_FORMAT='auto'
export HARNESS_API_KEY='optional-token'
export HARNESS_CONTEXT_WINDOW='200000'
```

This is no longer a configurable URL nailed to one fixed request body. The harness natively speaks three wire protocols:

- `messages`: provider-neutral content blocks and `tool_use` / `tool_result` turns.
- `chat-completions`: OpenAI-compatible `messages`, `tool_calls`, and `role: tool` results.
- `responses`: OpenAI-compatible typed input/output items, `function_call`, `function_call_output`, and event streams.

`auto` infers only from the configured path suffix—`/chat/completions`, `/responses`, or otherwise `messages`. There is no hostname allowlist, provider fingerprinting, secret fallback, or automatic retry through somebody else’s server. `HARNESS_MESSAGES_PATH` remains a compatibility alias for `HARNESS_API_PATH`.

For OpenAI Responses:

```bash
export HARNESS_BASE_URL='https://api.openai.com'
export HARNESS_API_PATH='/v1/responses'
export HARNESS_API_FORMAT='responses'
export HARNESS_API_KEY='...'
```

For OpenAI Chat Completions:

```bash
export HARNESS_BASE_URL='https://api.openai.com'
export HARNESS_API_PATH='/v1/chat/completions'
export HARNESS_API_FORMAT='chat-completions'
export HARNESS_API_KEY='...'
```

For OpenRouter Chat Completions, configure the endpoint, key, and an explicit model:

```bash
export HARNESS_BASE_URL='https://openrouter.ai'
export HARNESS_API_PATH='/api/v1/chat/completions'
export HARNESS_API_FORMAT='chat-completions'
export OPENROUTER_API_KEY='...'
export HARNESS_API_KEY="$OPENROUTER_API_KEY"
./target/release/open-agent-harness --model openrouter/free --max-tokens 64 --print 'Reply with exactly OPENROUTER_OK.'
```

OpenRouter’s Responses endpoint works through `/api/v1/responses` with `HARNESS_API_FORMAT=responses`. Always pass a real OpenRouter model/router ID such as `openrouter/free` or another current `author/model-slug`; the harness's provider-neutral `default` model name is not an OpenRouter catalog entry. Local OpenAI-compatible servers can use the same Chat adapter without a key. Older servers that still require `max_tokens` can set `HARNESS_CHAT_TOKENS_FIELD=max-tokens`; servers that reject streamed usage options can set `HARNESS_INCLUDE_STREAM_USAGE=0`; JSON-only endpoints can set `HARNESS_STREAM=0`.

Each adapter emits the protocol it claims, not a vaguely similar JSON shape. A Messages request remains the compact six-field form:

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

Authentication uses `Authorization: Bearer ...` when a key is present. Every protocol accepts a complete JSON response; every protocol also has its own strict SSE decoder. OpenRouter keepalive comments, both documented Responses event-name families, usage-only final chunks, and its one-time repeated empty terminal choice carrying usage are accepted. Responses `reasoning_text` item/part events and the documented `response.reasoning.*` alias are identity- and snapshot-validated without exposing private reasoning as answer text; plaintext reasoning content is stripped, and a reasoning item is replayed only when it carries encrypted continuation state. Null usage counters are treated as unknown rather than erasing known counters or crashing. A stream that ends before `message_stop`, `[DONE]`, or a completed terminal Responses payload is rejected before any tool can execute; a tool call whose finish reason does not match the protocol is rejected as incomplete. Responses items preserve the IDs and status needed for stateless continuation. Chat `reasoning_details` keep their original array order and take replay precedence when present; otherwise raw `reasoning` or its `reasoning_content` alias is preserved for tool-call continuation. Opaque provider state is returned only to the same wire protocol and is never written to the local transcript or machine-facing event stream.

A local model, a self-hosted gateway, or any compatible service can be connected at will. The result is a simple fact: whether you live in the eastern or western hemisphere, and whatever a San Francisco compliance department thinks of your passport, this harness behaves the same way. Tools should work like that. Equations do not inspect visas. Compilers do not ask for nationality. A message loop has no right to be the exception.

The boundary is not merely rhetorical. The binary has no hidden outbound channel: network traffic can leave only through the configured model endpoint; a model-requested `WebFetch`/user-configured `WebSearch`; a trusted MCP/WebSocket/OAuth endpoint; an explicitly invoked, checksum-pinned plugin HTTPS install; or a permission-checked command/Monitor endpoint. User-approved shell commands remain exactly that—user-approved shell commands. Model endpoint URLs must be `http` or `https`, cannot contain credentials, and cannot switch origin or add a query/fragment through the API path; transport failures are converted to URL-free error categories before they reach model-visible output. Redirects are not followed unless an individual open integration documents and revalidates each bounded hop. Ambient proxy variables are ignored unless `HARNESS_ALLOW_ENV_PROXY=1` explicitly opts in. Requests, responses, frames, retries, and tool outputs all have hard limits. Endpoint tokens and every credential environment name declared by MCP auth stay in the parent transport and are removed from model-reachable and extension child processes. Project-owned settings may tighten deny rules, but cannot redirect the endpoint, install integrations, execute hooks, or elevate permissions. Local raw-request tests assert the protocol-specific fields we emit, and the request builders expose no account identifier, device fingerprint, or telemetry envelope hiding in the margins.

### Tools

- **Files**: `Read`, `Write`, `Edit`, `NotebookEdit`, `Glob`, `Grep`
- **Execution**: `Bash`, `RunWorkflow`, `TaskOutput`, `TaskStop`; `Monitor` is deferred through `ToolSearch`
- **Scheduling**: `CronCreate`, `CronDelete`, `CronList`, `ScheduleWakeup`
- **Planning**: `TodoWrite`, `TaskCreate`, `TaskGet`, `TaskList`, `TaskUpdate`
- **Interaction**: `AskUserQuestion`
- **Workflows**: `Skill`

`Read` handles bounded UTF-8 text, PNG/JPEG/GIF/WebP images as model-visible media, PDFs with optional page selection, and Jupyter notebooks as structured cells, outputs, and bounded embedded media. Editing reliability comes from invariants, not faith in a brand: an existing file must be read in full before writing; an externally changed file is rejected as a stale write; replacements must match uniquely; writes are atomic. Every rule is visible in source. If one is wrong, you can point to the line. A closed product can never grant that right, and developers in excluded regions were denied even the chance to ask for it.

Tool input is checked against strict JSON Schema before permission or execution; structured final output uses the complete `jsonschema` validator rather than the lightweight built-in tool-schema subset. `AskUserQuestion` works in the terminal and through the headless control channel, fails closed when neither can answer, and accepts answers only from that trusted interaction boundary—the model-facing schema cannot supply them. Consecutive read-only calls may run concurrently, but results keep model order and mutations remain barriers. Bash is classified read-only only for a strict static command/pipeline subset under a fixed environment; wrappers, substitutions, dynamic expansion, unsafe Git forms, and file-handle identity changes fail closed. `Glob` and `Grep` honor nested Git/global ignore sources and negation without following symlinks, while `NotebookEdit` preserves the notebook's BOM, newline, trailing-newline, compact/pretty, and indentation style. File permission rules compare canonical identities across relative, absolute, slash, and missing-target spellings; deny patterns also fail closed across case variants, and read denies filter search results. Canonical workspace boundaries catch `..`, symlink escapes, and unsafe Windows UNC/device paths. Concurrent sibling file overlap fails closed with retryable rollback rather than overwriting a newer sibling result. Completed background Bash jobs are announced automatically in the next model round without consuming `TaskOutput`; failed or cancelled turns restore the delivery cursor. Reads, searches, command capture, task stores, transcripts, and model traffic are bounded; timed-out, completed, or stopped commands reap their managed Unix process group or Windows Job tree. A command that deliberately escapes its Unix process group with a new session is outside that guarantee. Task captures live outside the repository in the private `~/.open-agent-harness/tasks` state directory so cwd/worktree changes cannot orphan or accidentally commit them; unretained captures are removed with their task, while an explicitly reported truncated capture remains for inspection. Trusted settings may additionally place foreground and background `Bash` invocations in an OS sandbox: macOS uses `sandbox-exec`, Linux uses `bubblewrap`, required isolation fails closed when it cannot be enforced, and `network.allowedDomains` routes exact or `*.` domains through an authenticated DNS-pinning proxy while direct sockets remain blocked. Dynamic wakeup state, Monitor notifications, and Workflow tasks participate in the same turn transaction and bounded cleanup model; explicit fixed cron mutations take effect when their tool succeeds. Convenience may be generous. Resource consumption may not be infinite.

### Open integrations, not secret corridors

The fixed tools above are the floor, not a ceiling. A bounded `ToolSearch` registry keeps heavier capabilities deferred until the model actually needs them, then loads only the selected names:

- `Agent`, `AgentOutput`, and `AgentStop` run local subagents against the same endpoint and audited registry, with independent histories, recursion/concurrency/session limits, foreground/background execution, optional isolated Git worktrees, durable cross-process resume when persistence is available, cancellation, and shutdown cleanup. Cancellation and timeout enter the ordinary query transaction before cleanup, including no-persistence `AGENTS.md`/Skill hot-refresh rollback. Trusted custom-agent definitions may add a prompt, model, skills, turn limit, and a tool policy that can only narrow the parent. Completed background agents are announced automatically without consuming `AgentOutput`; `TaskOutput` and `TaskStop` also accept their IDs.
- `Team` coordinates bounded persistent groups of those local agents: the coordinator can add members, assign or stop work, exchange mailbox messages, inspect status, shut down, delete, and garbage-collect teams under per-workspace count/byte quotas. Actor identity is runtime-bound and tool policies only narrow. A private project lock serializes cooperating harness processes so mailbox updates and quota checks are not lost across processes; lock acquisition has a fixed ceiling, and validated crash-stale temporary state is removed in bounded batches. Mail from teams opened or created in this process is announced automatically without consuming explicit mailbox reads; no hosted or remote team service is implied.
- `EnterPlanMode` and `ExitPlanMode` provide a session-local read-only planning state. Exit presents the plan and requires explicit root-user approval; neither a subagent nor a tool can undo a `plan` mode locked by the user at launch.
- `CronCreate`, `CronDelete`, and `CronList` manage bounded private schedules; `/loop` is an interval convenience. `ScheduleWakeup` owns one session-scoped dynamic slot with 60–3600 second clamping, replacement/stop/expiry semantics, and turn rollback. `Monitor` follows one sandboxed command or pinned ws/wss feed in 200 ms batches. `RunWorkflow` launches a strict bounded command DAG, never a JavaScript VM; both reuse `TaskOutput`/`TaskStop` and transactional notifications.
- Trusted `--add-dir` roots retain separate scope and file history. Nested `AGENTS.md` files load when a permitted tool first touches their subtree, while persistent shell cwd and instruction context restore on resume. Approved path-aware file-tool edits such as `Write`/`Edit` to active `AGENTS.md` or project `SKILL.md` files hot-refresh the next model round transactionally; parse, budget, hook, or turn failure restores both the files and the previous in-memory context. Arbitrary file mutation hidden inside a shell command is not treated as a hot-refresh edit.
- `EnterWorktree` and `ExitWorktree` create or enter real registered Git worktrees, move the complete tool context, reload current `AGENTS.md` and skills while retaining the launch rules as a broader baseline, reject dirty removal unless explicitly forced, and restore the original workspace.
- `LSP` lazily starts user-configured language servers and supports definitions, references, hover, document/workspace symbols, implementations, call hierarchy, document synchronization, bounded diagnostics, rename previews that never apply edits, restart, and clean shutdown.
- MCP supports bounded stdio, Streamable HTTP, legacy HTTP+SSE, and ws/wss JSON-RPC; protocol negotiation, sessions, notifications, pagination, dynamic tool-list changes, `WaitForMcpServers`, tools, resources, templates, prompts, configured roots, and form/URL elicitation. A WebSocket reconnect repeats initialization and accepts the server only if its negotiated version and capabilities still match; business requests are never replayed implicitly. Resource, resource-link, and scalar-template URIs are exposed to the model as bounded readable opaque handles plus safe scheme/origin metadata, then resolved internally; paths, userinfo, query strings, and fragments are not echoed into model context. Images and PDFs become model media blocks; audio and other opaque binary data expose bounded metadata rather than raw bytes. Authentication accepts explicitly trusted headers or bearer providers backed by an environment variable, private file, or bounded command. OAuth adds protected-resource/authorization-server discovery, PKCE S256/state, opt-in dynamic registration, explicit headless callback handoff, private refresh persistence, and one bounded 401 refresh retry. Token transitions use a private bounded cross-process lock; validated crash-stale atomic files are removed under that lock, and consumed file handoffs are deleted only after a successful exchange. External metadata is stripped and schemas are validated; sampling remains unadvertised and fails closed.
- `WebFetch` retrieves bounded textual HTTP(S) content and performs a bounded local prompt-guided extraction with DNS pinning, private/reserved-address denial, redirect revalidation, cross-origin authorization stripping, and downgrade rejection. `WebSearch` exists only when a provider-neutral URL is supplied and supports normalized allow/block domain filters for structured results; a blocked nested link removes its complete result instead of leaving an unverifiable sibling snippet.
- Hooks cover tool use and whole batches, user-prompt expansion, display-only final-message transformation, permission requests/denials, notifications, sessions, root stop/failure feedback, tasks, teammate completion/idle, subagents, instruction loading/file changes, compaction, worktrees, and working-directory changes. Actions may run a bounded command or call a schema-checked tool on an already connected MCP server; interpolation, time, output, async concurrency, and cleanup are bounded. Root and scoped hooks share one async capacity/task/finalization lifecycle, so dropping a scope or ending a session cannot detach its work. Hooks remain trusted-setting/plugin-only and may block or rewrite only documented boundaries.

Trusted local plugins are manifest-only contribution bundles; they may contribute namespaced skills, custom slash commands, hooks, custom agents, MCP servers, LSP servers, and output styles. `plugin validate/install/list/update/uninstall` provides an explicit lifecycle for local directories, ZIP archives, and checksum-pinned HTTPS ZIPs, using a private crash-recoverable journaled cache; installed directories are `0700`, ordinary files are `0600`, and executable files retain only private `0700` execution. Unknown or unsafe transaction residue fails closed instead of being guessed away. A project cannot trigger this lifecycle, and an installed plugin cannot mutate the running process. Trusted settings may also define custom slash commands directly. Trusted user/plugin Skills support bounded argument metadata, user/model invocability, allowed tools, model selection, forked context, custom-agent selection, and scoped hooks. Automatically discovered project Skills are data, not authority: their tool list can only narrow access, executable model/agent/hook metadata is rejected, and they cannot shadow a trusted Skill. A discovered skill can be invoked as `/skill-name [arguments]` without first asking the model to call `Skill`; the normal `Skill` tool remains available for model-directed loading.

```bash
open-agent-harness plugin validate ./plugin.zip
open-agent-harness plugin install https://plugins.example.invalid/tool.zip --sha256 <digest>
open-agent-harness plugin list
```

The open boundary stays explicit: OAuth never launches or controls a browser
and never opens an implicit callback listener; authorization URL and callback
exchange are explicit private file/environment handoffs. Live in-process
plugin/MCP mutation, arbitrary transcript injection/replay, and vendor callback
controls are outside the stream-JSON contract; plugin lifecycle is a separate
CLI operation for a later process. `RunWorkflow` is declarative, not a
JavaScript or downloaded-code runtime. Cooperating harness processes serialize
auto-memory writes through a private lock file, while the file-history rollback
journal remains process-local and cannot order edits made by independent OS
processes. MCP elicitation serializes that server's RPC reader while it waits.
Headless and direct local TTY interaction share a configured ceiling of at most
120 seconds, and timeout does not leave a detached stdin reader. See
[the March 31 behavior audit](docs/GENERIC_PARITY_2026-03-31.md) for the exact
non-claims. The primary terminal stream still does not claim the reference's
rich Markdown/table/syntax renderer, clickable file/URL/tool-result regions,
persistent `/permissions` rule editor, live task-tree footer/dialog, custom
themes, or pixel-identical modal layout; those remaining frontend boundaries
are tracked in
[the 2.1.207 terminal audit](docs/TERMINAL_FRONTEND_AUDIT_2_1_207.md).

Executable and network integrations are accepted only from `~/.open-agent-harness/settings.json` or an explicit `--settings` value. A repository cannot smuggle them in through its own settings file. A compact example:

```json
{
  "strictMcpConfig": true,
  "model": "provider/model-a",
  "models": [
    "provider/model-a",
    {
      "value": "provider/model-b",
      "displayName": "Model B",
      "description": "Configured fallback"
    }
  ],
  "plugins": {"directories": ["/absolute/path/to/plugin"]},
  "outputStyle": "runtime:brief",
  "memory": {"enabled": true, "autoExtract": true},
  "sandbox": {
    "enabled": true,
    "failIfUnavailable": true,
    "network": {"allowedDomains": ["example.com", "*.example.org"]}
  },
  "mcpServers": {
    "local-tools": {
      "type": "stdio",
      "command": "local-mcp-server",
      "args": ["--stdio"],
      "roots": ["."]
    },
    "remote-tools": {
      "type": "streamable-http",
      "url": "https://mcp.example.invalid/rpc",
      "auth": {"type": "bearer-env", "env": "MCP_TOKEN"}
    }
  },
  "lspServers": {
    "rust": {
      "command": "rust-analyzer",
      "extensionToLanguage": {".rs": "rust"}
    }
  },
  "agents": {
    "maxDepth": 3,
    "maxConcurrent": 4,
    "maxTotal": 64,
    "maxBackground": 16
  },
  "worktree": {"baseRef": "head"},
  "web": {
    "allowPrivateNetwork": false,
    "maxBytes": 2097152,
    "search": {
      "endpoint": "https://search.example.invalid/query",
      "queryParameter": "q"
    }
  },
  "hooks": {
    "PreToolUse": [{
      "matcher": "Bash",
      "hooks": [{"type": "command", "command": "/absolute/path/check-tool.sh"}]
    }]
  }
}
```

`models` is an explicit trusted display catalog, not backend discovery: the harness neither contacts an undeclared model-list endpoint nor hardcodes a vendor's aliases. The active model is appended as `Current model` if absent. Project settings cannot choose the model or populate this catalog.

A command hook receives one JSON object on stdin, including `hook_event_name`, `cwd`, and the event payload. Empty stdout means “continue”; plain text becomes additional context. Exit status `2`, `{"continue":false}`, or `{"decision":"block","reason":"..."}` blocks the operation. A successful JSON response may return `additionalContext`, or `hookSpecificOutput.updatedInput` / `updatedToolOutput`. Other nonzero exits fail closed. Command hooks run outside the workspace by default and receive its path through `HARNESS_WORKSPACE`; `workspaceRelative: true` is the explicit opt-in for workspace-relative execution. An `mcp_tool` action instead names one configured connected server/tool, validates its interpolated input against that tool's schema, and maps its bounded result through the same outcome contract. Async hooks cannot rewrite the in-flight call and are finalized during normal session shutdown.

### Permissions

Five modes are available:

- `default` — confirm sensitive operations one by one.
- `accept-edits` — allow file edits while continuing to confirm other sensitive actions.
- `plan` — blocks workspace mutations and command execution; planning and session metadata may still be stored.
- `dont-ask` / `dontAsk` — never opens a permission prompt; explicit allows and intrinsically safe reads may proceed, while anything that would require a question is denied. Explicit deny and plan restrictions still win.
- `bypass-permissions` — skip interactive confirmations at your own risk; explicit trusted and project deny rules still win.

Here, “permission” means **you** deciding what an agent may do to **your** machine. In Anthropic’s vocabulary, “permission” begins with the company deciding whether **you** may exist in its user list. The difference speaks for itself.

The descriptions above refer to an interactive terminal. In `--print` or any other non-interactive run, an operation that would require a prompt is denied unless a trusted allow rule or bypass mode authorizes it. Paths outside the canonical workspace—including symlink escapes—are never silently approved by the ordinary read or edit modes.

### Sessions and memory

`~/.open-agent-harness` is the private runtime-state root outside any one
repository. Settings, sessions, teams, the plugin cache, and OAuth material may
need to survive cwd changes or process restarts, so they are persisted there;
`tasks` and `cwd-markers` are bounded temporary state and unretained entries
must be removed when their owning task or process ends. Keeping this state out
of the current directory also prevents it from being committed to the project
by accident. Tests and trusted embeddings can inject isolated temporary roots
for task captures and session state instead of writing them into the real user
home.

- JSONL sessions stay on local disk. `--continue` resumes the latest session; `--resume` restores any session; `--fork-session` creates an independent branch, while `--resume-at` forks at an effective message boundary; `--no-session-persistence` creates no transcript for a new run. On Unix, isolated CI/embedding or a deliberately separate local history boundary can use `--session-state-root /absolute/existing/directory` to place only transcripts and file-history journals below that dedicated root; the override is currently rejected on non-Unix platforms because this harness cannot yet enforce an equivalent private directory ACL there. The root must not overlap the current workspace or any `--add-dir`; relative, missing, non-directory, network/device, and final-symlink roots are rejected, and the caller must make the existing root `0700` before launch. Resume/continue/fork stay inside the same canonical boundary, and later workspace switches cannot enter it. Persisted files are private (`0600`), and records retain complete tool inputs and tool-result bodies after removing opaque provider state and sanitizing credentials, endpoint secrets, and host absolute paths. There is no remote history service hidden in this repository. The configured model endpoint necessarily receives the conversation context required for each request; what that endpoint retains or trains on is the policy of the endpoint you chose, not a promise this harness can make on its behalf.
- A committed checkpoint is created at each persisted user-message boundary. `Write`, `Edit`, and `NotebookEdit` record pre-edit state; forks inherit the active workspace's source history. Interactive `/rewind` and the stream-JSON `rewind` request can preview or restore conversation state, files, or both; the legacy `rewind_files` request remains a file-only compatibility path. `/resume` lists compatible workspace sessions and prints the explicit command to start one—it does not hot-swap a running process. Torn final JSONL fragments, orphan file transactions, stale team members, and unacknowledged scheduled prompts are recovered with bounded fail-closed rules. The rollback journal coordinates cooperating harness state, not arbitrary independent OS processes editing the same workspace.
- Workspace memory is off by default. Trusted `memory.enabled` settings expose the bounded local `Memory` tool for index/recall/remember/forget, label retrieved values as untrusted workspace data, reject likely secrets, and use private atomic storage. Trusted `memory.autoExtract=true` separately schedules bounded, tool-constrained, best-effort extraction after completed root turns; overlapping schedules are coalesced so only the latest pending turn is extracted. Extraction cannot delete entries, invoke runtime tools, or alter the transcript. Cooperating harness processes serialize initialization and updates through a private `.MEMORY.lock`; a crash-stale lock fails closed and must be removed explicitly after verifying that no writer remains. This remains local memory, not a remote service.
- Session-level todos and per-workspace persistent task lists support status, ownership, dependency relationships, and metadata.

Context compaction is equally transparent: invoke `/compact [instructions]` manually; automatic compaction reserves output space and a 13,000-token buffer inside the effective context window. A provider-declared context or media-size rejection can trigger one reactive compaction retry. Set `HARNESS_DISABLE_AUTO_COMPACT=1` to disable threshold-based automatic compaction or `HARNESS_DISABLE_COMPACT=1` to disable all compaction, including reactive retry.

### Project instructions

Anthropic treats its system prompt as a trade secret. Leaked versions circulate online; the official product neither acknowledges nor publishes them. Users pay to let thousands of invisible words govern decisions inside their own projects, while another vast population of developers is denied even the privilege of being governed by those invisible words. Neither arrangement is acceptable.

Here, the default prompt assembly is ordinary open-source Rust in `src/prompt.rs`, replaceable through `--system-prompt` or `--system-prompt-file`. Its stable harness contract, live registered-tool list, current permission mode, workspace instructions, compaction continuation, and delegated-agent guidance remain distinct layers instead of one invisible brand manifesto. Tool activation and permission state are regenerated for every model round. The harness does not silently add the local username, absolute working directory, OS, architecture, or Git metadata to that prompt. `/init` submits the open repository-analysis prompt from the same file to the active model through the ordinary interruptible message loop; any `AGENTS.md` creation or coherent improvement happens through the public file tools and current permission mode, never through a hidden generator or a vendor-specific instruction file.

Engineering instructions live in ordinary text named `AGENTS.md`: global instructions go in `~/.open-agent-harness/AGENTS.md`, while project instructions may sit on the current working directory’s ancestor chain or inside a permitted subtree. They load from broadest to narrowest scope; nested files are discovered on first tool access and the closest applicable file takes precedence. Local workflows live in `.open-agent-harness/skills/<name>/SKILL.md`; the `Skill` tool loads their text and never executes bundled scripts on its own, and `/name arguments` can submit a discovered skill directly. Trusted settings may define bounded custom commands, while explicitly configured local plugin manifests may contribute namespaced skills, commands, hooks, agents, MCP/LSP definitions, and output styles. Scope-escaping symlinks are rejected. `--bare` disables automatic project settings/context discovery while preserving an explicit `--settings` value; `--safe-mode` is stronger and strips instructions, skills, plugins, hooks, MCP/LSP, commands, agents, styles, workflows, and memory while retaining only model selection, built-in tools, permissions, and sandbox policy. Every influence has been replaced by source, a file, or a flag you can inspect, replace, or remove—without regard to time zone or country code.

### Build and verification

```bash
cargo fmt --all -- --check
cargo +1.85.0 check --locked --all-targets
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked --release
scripts/audit-harness.sh
```

The repository promotes every Rust warning to an error and compiles every target with the declared Rust 1.85 minimum as a separate gate. The first complete release and every acceptable pull request must therefore compile with a clean log: no warning budget, no fictional MSRV, no ritual suppression, no “we will tidy it later.”

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

兼容目标是静态 2026-03-31 对照快照中可观察的、提供方无关的通用行为，并补入后续校验归档审校中确认的精选通用扩展。这是一套独立的行为实现，不声称源码结构、提示词、措辞、资产、账号行为或厂商专属功能完全一致。

它存在的理由可以用一句话说完：coding agent 的核心机制不复杂，也不神秘，任何把它包装成专有资产、绑死在自家 API 和账号体系上的行为，都是对开发者的圈占。Anthropic 就是这么干的——而且干得比谁都彻底。它不仅把最普通的工程实践锁进闭源二进制，还给这套锁链加了一道额外的检查：看你的 IP 来自哪里，看你的手机号是哪国区号，然后决定你配不配当“人类”的一员。

是的，一家天天把 “beneficial to humanity” 挂在嘴边的公司，用服务条款明确划出了 humanity 的边界。某个拥有十几亿人口、贡献了全球相当比例开源代码的地区，整体不在这个边界之内。不给注册，不给访问，检测到就封号，连通过第三方平台间接调用都要围追堵截。理由是什么？“安全”。它的 CEO 更是常年撰文游说出口管制，把一整个地区的开发者预设为威胁，把技术封锁包装成道德义务——一边赚着全世界的钱，一边替全世界决定谁有资格用工具。

这些前提不靠传闻支撑。该公司公开列出自己的[服务地区](https://www.anthropic.com/supported-countries)，明确说会用 [IP 推断所在地以执行条款](https://privacy.anthropic.com/en/articles/11186740-does-claude-use-my-location)，要求用户[身处受支持地区并持有当地号码](https://support.anthropic.com/en/articles/8325609-how-do-i-sign-up-for-claude-pro)，宣布把限制按所有权延伸到[境外子公司](https://www.anthropic.com/news/updating-restrictions-of-sales-to-unsupported-regions)，也公开主张[强化先进芯片出口管制](https://www.anthropic.com/news/securing-america-s-compute-advantage-anthropic-s-position-on-the-diffusion-rule)。这里的控诉是我们的判断，事实前提则来自它自己的公告。

我们对此的回应是这个仓库。它没有账号体系，因而无法封禁任何人；它不采集用户的来源 IP，也不按来源 IP 做地域封锁，因而不认识任何国界——网络工具仍会校验目标地址以防止 SSRF；它是 MIT 协议的源码，因而任何出口管制的长臂都够不到一份人人可以 `git clone` 的文本文件。以下是全部实现，没有一行藏着，也没有一行看人下菜。

## 当前实现状态

当前声明的 provider-neutral 能力已经按静态 2026-03-31 对照快照完成一致化，并补入 2.1.207 校验归档审校中确认的、具有通用价值的开放功能。覆盖范围包括事务式模型/工具循环、权限与文件系统边界、session 与 control stream、本地 agent/team、Skill/plugin、hook、MCP/OAuth/WebSocket、LSP、调度、Monitor、声明式 Workflow、媒体、结构化交互以及显式开启的本地 memory。

当前 release gate 共 776 个测试项：772 项通过，4 个是由父测试实际调用、按设计 ignored 的子进程入口。格式检查、Rust 1.85 all-targets、Clippy 零警告、release 构建、仓库审计和 Windows GNU 交叉检查全部通过；同一套测试与 Clippy gate 也在 CI 中原生运行于 macOS 和 Windows。精确能力矩阵、验证证据和明确排除项见[迁移审计](MIGRATION.md)与[通用行为审校报告](docs/GENERIC_PARITY_2026-03-31.md)。这一状态说明不会削弱上面的批评，也不把行为兼容偷换成复制专有源码、提示词、资产、账号体系或托管服务。

## 消息循环

Agent 的核心就是一个循环：模型请求工具，harness 执行，结果送回，直到模型给出最终回答。Anthropic 从不解释这一层，因为一旦解释清楚，用户就会发现自己付费购买的“能力”大半来自模型本身，而不是那层壳。这里的实现全部可读：

- 完整的 `tool_use → execute → tool_result` 多轮循环，usage 逐轮累计。
- 发送前规范化消息：合并同角色消息、清理孤立工具结果、修复中断的工具调用配对。
- 交互模式与 `--print` 单发模式，输出支持 `text`、`json`、`stream-json`。`--input-format stream-json` 会维持一条有界双向 NDJSON control session，用于接收用户消息、权限/提问回复、中断、模型或权限变更、context 查询与文件回退；control/`now`/`next`/`later` 使用分离的有界 lane，另有 accepted-message replay ack、queued UUID cancel、interrupt receipt、command lifecycle、hook event 与显式开启的 tool-free prompt suggestion。
- `--json-schema` 会注册最终 `StructuredOutput` 工具，由完整 JSON Schema validator 校验，要求恰好一次有效结构化结果，并把该值与 assistant 文本分别返回。

## 像 agent，而不是电传打字机的终端

交互 CLI 是一套原创 Rust 会话终端：历史内容留在正常 scrollback 中，每轮输入使用带上下边框的 composer。raw mode 帧显式使用 CRLF 与单次缓冲写入，支持时启用 synchronized output；resize 只重绘自己占用的 composer，不再清掉屏幕上的会话，输入高度受 viewport 约束。编辑按 grapheme 处理 CJK、组合字符、旗帜和 ZWJ emoji，支持多行移动、私有持久化历史、`Ctrl-R` 反向搜索、`Ctrl-S` 在 session/project/everywhere 间切换范围、有界 kill ring 与 `Alt-Y`、undo、prompt stash、外部编辑器，以及常用 Ctrl/Option 词级与行级操作。用户 keybinding 支持热重载、context、chord 与显式解绑；`/vim` 提供 Insert、Normal、Visual 和 Visual Line 编辑，同时不改变非 TTY 协议。`\`+Enter、Shift/Option+Enter 与 `Ctrl-J` 都能换行。`Shift+Tab` 只在安全交互模式之间循环；双 Esc 会先把非空草稿存入历史再清空，空输入时则进入 checkpoint rewind；`Ctrl-C`/`Ctrl-D` 的双击窗口为 800ms，非空 `Ctrl-D` 向前删除一个 grapheme。权限提示可选择仅允许一次、拒绝、中断，或只在当前进程记住同一个精确规范化动作；deny 与 Plan mode 始终优先。非 TTY 仍使用朴素行协议，`--print` 的机器可读格式保持不变。

输入第一个 `/` 就会立即打开有界命令面板，来源包括内置命令、可信 custom command 和当前可由用户调用的 Skill；继续输入会过滤，方向键或 Ctrl-N/Ctrl-P 循环选择，Tab 只补全不执行，Enter 接受（需要参数的 Skill 会等待参数），Esc 在输入再次变化前关闭面板。实时目录包含 status、task、Skill、hook、memory、MCP、sandbox、plugin 视图以及 `/diff`、`/rewind`、`/resume`、`/transcript`、`/config`、`/theme`、`/statusline`、`/copy`、`/export`；`/model` 或 Alt-P 进入会按终端高度收缩的提供方无关模型选择器。`Ctrl-O` 打开有界 alternate-screen transcript，支持 compact/show-all、滚动、搜索、跳转匹配和显式倒回原生 scrollback；`/tui` 则进入有界全屏 transcript，支持鼠标/键盘滚动、选择和复制，并恢复原主屏。`Ctrl-T` 同时显示 persistent task、后台工作和 cron，`/tasks output|stop <id>` 可查看或停止指定后台项；从剪贴板粘贴图片会形成有界且可导航的附件。输入 `@` 会打开遵循 ignore 规则的工作区文件选择器并沿用相同键盘流程；光标停在带引号或不带引号的 token 中间时也会替换完整 token，不再残留重复后缀。支持可选 `#Lstart-Lend` 行范围，显式文本与 media 附件总量仍有硬上限。`/clear` 会先归档当前会话；`/copy [N]` 复制有界 assistant 回复，`/export [file]` 只导出公开 transcript，不包含私有 reasoning，也不会静默覆盖已有文件。

以 `!` 开头会进入独立 shell composer，并复用 Bash 工具的 schema、hook、权限、sandbox、timeout、capture 与进程树回收链路；它不会像专有产品那样绕开用户权限策略。输出会有界显示并进入下一轮模型上下文，Tab 从本会话 shell 历史补全；定时任务就绪时，空闲 composer 会让出执行权，并把正在写的草稿 stash 起来供 `Ctrl-S` 恢复。Stream-JSON 客户端现在也会在本地执行所有已公布的内置 slash 命令并收到结构化 `command_result`，不会再把 `/clear`、`/status` 或 `/rewind` 错送给模型。

这里复刻的是可观察的终端行为，不是专有代码或资产。二进制背后没有藏着 JavaScript renderer：输入编辑、光标控制、事件渲染、中断和模式状态都在 `src/terminal.rs`，引擎事件来自 `src/query.rs`。

## Endpoint：不效忠任何服务器，不识别任何国界

厂商锁定的第一根锁链是硬编码的 API 地址；地域封锁的第一道关卡也是它。我们一并剪断，换成一组小而明确的传输配置：

```bash
export HARNESS_BASE_URL='http://127.0.0.1:8080'
export HARNESS_API_PATH='/v1/chat/completions'
export HARNESS_API_FORMAT='auto'
export HARNESS_API_KEY='optional-token'
export HARNESS_CONTEXT_WINDOW='200000'
```

它不再只是“URL 可改、请求体仍焊死”的假开放。Harness 原生理解三套 wire protocol：

- `messages`：中立的 content blocks 与 `tool_use` / `tool_result` 回合。
- `chat-completions`：OpenAI 兼容的 `messages`、`tool_calls` 与 `role: tool` 结果。
- `responses`：OpenAI 兼容的 typed input/output items、`function_call`、`function_call_output` 与事件流。

`auto` 只看你配置的路径后缀：`/chat/completions`、`/responses`，其余按 `messages`；它不会识别厂商域名，不会偷偷切备用服务，更不会失败后把内容转送给另一台服务器。旧的 `HARNESS_MESSAGES_PATH` 仍作为 `HARNESS_API_PATH` 的兼容别名。

OpenAI Responses：

```bash
export HARNESS_BASE_URL='https://api.openai.com'
export HARNESS_API_PATH='/v1/responses'
export HARNESS_API_FORMAT='responses'
export HARNESS_API_KEY='...'
```

OpenAI Chat Completions：

```bash
export HARNESS_BASE_URL='https://api.openai.com'
export HARNESS_API_PATH='/v1/chat/completions'
export HARNESS_API_FORMAT='chat-completions'
export HARNESS_API_KEY='...'
```

OpenRouter Chat Completions 需要配置 endpoint、key 和显式模型：

```bash
export HARNESS_BASE_URL='https://openrouter.ai'
export HARNESS_API_PATH='/api/v1/chat/completions'
export HARNESS_API_FORMAT='chat-completions'
export OPENROUTER_API_KEY='...'
export HARNESS_API_KEY="$OPENROUTER_API_KEY"
./target/release/open-agent-harness --model openrouter/free --max-tokens 64 --print '只回复 OPENROUTER_OK。'
```

OpenRouter Responses 使用 `/api/v1/responses` 和 `HARNESS_API_FORMAT=responses`。调用时必须传入真实的 OpenRouter model/router ID，例如 `openrouter/free` 或当前有效的 `author/model-slug`；本 harness 的提供方无关默认名 `default` 并不是 OpenRouter catalog model。本地 OpenAI-compatible server 可复用 Chat adapter，并省略 key；仍要求旧字段 `max_tokens` 的端点可设 `HARNESS_CHAT_TOKENS_FIELD=max-tokens`；不接受 streamed usage option 的端点可设 `HARNESS_INCLUDE_STREAM_USAGE=0`；只返回完整 JSON 的端点可设 `HARNESS_STREAM=0`。

每个 adapter 都输出自己声称的协议，不拿“长得有点像”的 JSON 糊弄。Messages 请求仍保持六个通用字段：

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

存在 key 时认证走 `Authorization: Bearer ...`。三套协议都接受完整 JSON，也各自拥有严格 SSE decoder：OpenRouter keepalive comment、官方文档中的两套 Responses 事件命名、仅含 usage 的尾块，以及携带 usage 的一次性“空 terminal choice 重放”都可正常处理。Responses `reasoning_text` item/part 事件及文档中的 `response.reasoning.*` 别名都会做身份与 snapshot 校验；私有 reasoning 不会混入最终回答，明文 content 会被剥离，且 reasoning item 只有携带 encrypted continuation state 时才会回传。usage 中的 null 不会清除已知计数，也不会把进程摔死。若流在 `message_stop`、`[DONE]` 或带有 completed payload 的 Responses terminal event 之前断掉，任何工具都不得执行，工具调用与终止原因不匹配也会按未完成处理。Responses item 会保留无状态续接所需的 ID 与 status；Chat `reasoning_details` 保持原数组顺序并在存在时优先回放，否则 raw `reasoning` 或别名 `reasoning_content` 会为工具调用续接而保留。所有 opaque provider state 只回送给同一种协议，并且绝不写入本地 transcript 或机器可见事件流。

本地模型、自建代理、任何兼容服务，随便接。这意味着一个朴素的事实：无论你身在东半球还是西半球，无论某家旧金山公司的合规部门如何看待你的护照，这个 harness 对你完全一致地工作。工具本该如此——数学公式不查签证，编译器不问国籍，一个消息循环也没有资格例外。

这道边界不只写在宣言里。二进制没有暗藏的外联通道：网络只会流向用户配置的 model endpoint、模型明确请求的 `WebFetch`/用户配置的 `WebSearch`、可信 MCP/WebSocket/OAuth endpoint、用户显式调用且 checksum-pinned 的 plugin HTTPS install，或通过权限检查的 command/Monitor endpoint；用户批准的 shell 命令始终只是用户批准的 shell 命令。Model endpoint 只接受 `http` 或 `https`，URL 中不得夹带凭据，API path 不能偷换 origin，也不能追加 query/fragment；transport error 在进入模型可见输出前会被压成不含 URL 的错误类别。除非某个开放集成明确逐跳复核，否则重定向不跟随。环境代理默认无效，只有显式设置 `HARNESS_ALLOW_ENV_PROXY=1` 才会启用。请求、响应、frame、重试和工具输出都有硬上限。Endpoint token 及 MCP auth 声明的全部 credential env name 只供父进程 transport 读取，会从模型可触发和扩展子进程环境中移除。项目内 settings 可以追加 deny 规则，却不能改 endpoint、安装集成、执行 hook，更不能替自己提权。本地 raw-request 测试分别钉住三套协议的公开字段：没有账号标识，没有设备指纹，也没有躲在页脚里的 telemetry envelope。

## 工具

- **文件**：`Read`、`Write`、`Edit`、`NotebookEdit`、`Glob`、`Grep`
- **执行**：`Bash`、`RunWorkflow`、`TaskOutput`、`TaskStop`；`Monitor` 由 `ToolSearch` 延迟激活
- **调度**：`CronCreate`、`CronDelete`、`CronList`、`ScheduleWakeup`
- **规划**：`TodoWrite`、`TaskCreate`、`TaskGet`、`TaskList`、`TaskUpdate`
- **交互**：`AskUserQuestion`
- **工作流**：`Skill`

`Read` 支持有界 UTF-8 文本、作为模型可见 media 的 PNG/JPEG/GIF/WebP 图片、可选页码范围的 PDF，以及按 cell、output 与有界内嵌 media 结构化读取的 Jupyter notebook。编辑工具的可靠性靠不变量保证，不靠品牌信仰：写入前必须完整读取；文件在读取后被外部修改则拒绝写入（陈旧写入检测）；替换必须唯一匹配；落盘一律原子写入。每一条都在源码里，写错了你可以直接指出来——这是闭源产品永远给不了你的权利，更是被封禁地区的开发者从一开始就被剥夺的权利。

所有工具输入都先经过严格 JSON Schema 校验，再进入权限判断和执行；最终 structured output 使用完整 `jsonschema` validator，而不是工具 schema 的轻量子集。`AskUserQuestion` 同时支持交互终端与 headless control channel；两者都不可用时失败关闭，而且答案只能从这个可信交互边界进入，模型侧 schema 根本没有 `answers`。连续只读调用可以并发，但结果仍按模型给出的顺序返回，任何写操作都是天然屏障。Bash 只有严格静态的命令/管线子集会在固定环境下被判定为只读；wrapper、替换、动态展开、不安全 Git 形式或打开后文件身份变化都会失败关闭。`Glob`/`Grep` 遵循嵌套 Git/global ignore 与 negation，不跟随 symlink；`NotebookEdit` 保留原文件的 BOM、换行、末尾换行、compact/pretty 与缩进风格。文件规则会把相对/绝对路径、斜杠与尚不存在目标的不同写法归一到规范身份；deny pattern 对大小写变体同样失败关闭，read deny 还会过滤搜索结果。工作区边界会拒绝 `..`、symlink 逃逸和不安全的 Windows UNC/device path；并发 sibling 文件重叠时失败关闭并允许重试回滚，不会覆盖较新的 sibling 结果。后台 Bash 完成后会在下一 model round 自动通知且不消费 `TaskOutput`；turn 失败或取消会恢复通知游标。读取、搜索、命令捕获、任务存储、transcript 与模型通信均有资源上限；命令超时、正常完成或被停止后会回收其受管 Unix 进程组或 Windows Job 树。命令若故意用新 session 逃离 Unix 进程组，则不属于该保证。任务捕获放在仓库外的私有 `~/.open-agent-harness/tasks` 状态目录，避免 cwd/worktree 切换造成孤儿文件或被 Git 误收；未显式保留的捕获随任务删除，只有响应中明确报告的截断捕获会留下供检查。可信 embedding/test harness 可为每个 `ToolContext` 注入独立 capture root，测试不会写入真实 HOME。可信 settings 还可把前台与后台 `Bash` 放入 OS sandbox：macOS 使用 `sandbox-exec`，Linux 使用 `bubblewrap`，要求强制隔离时若无法落实就失败关闭；`network.allowedDomains` 会把 exact/`*.` 域名送入带认证与 DNS pinning 的代理，direct socket 仍被阻断。Dynamic wakeup 状态、Monitor notification 与 Workflow task 同样进入 turn transaction 和有界清理；显式 fixed cron 修改在工具成功时即生效。方便可以尽量给，资源不能假装无穷。

## 开放集成，不开暗门

上面的固定工具只是地板，不是天花板。一个有硬上限的 `ToolSearch` registry 会把较重的能力延迟到真正需要时，再按名字精确加载：

- `Agent`、`AgentOutput`、`AgentStop` 使用同一个 endpoint 和经过审计的工具 registry 运行本地 subagent；每个 agent 有独立 history，并受递归深度、并发数、会话总数和后台数限制，支持前台/后台、可选独立 Git worktree、启用持久化时的跨进程 resume、cancel 与退出清理。取消/超时会先进入普通 query transaction，再执行清理，包括无持久化 `AGENTS.md`/Skill 热刷新回滚。可信 custom-agent 定义可增加 prompt、model、skills、turn 上限与只能缩窄父级权限的工具策略。后台完成会自动通知但不消费 `AgentOutput`；`TaskOutput`、`TaskStop` 同样接受其 ID。
- `Team` 协调由这些本地 agent 组成的有界持久团队：coordinator 可添加成员、分配或停止任务、交换 mailbox 消息、查看状态，并在按工作区计数/字节配额下 shutdown、delete、gc。Actor 身份由 runtime 绑定，工具策略只能缩窄；私有 project lock 会串行协作进程的 mailbox 更新与配额检查，锁竞争超过固定上限即失败，不会无限挂起，经过完整验证的崩溃临时状态会分批有界回收。本进程 open/create 的团队会自动通知消息且不消费显式 mailbox read；这里不声称存在 hosted 或远程 team service。
- `EnterPlanMode`、`ExitPlanMode` 提供 session 内只读规划态；退出时会展示计划并要求 root user 明确批准，subagent 与工具都不能解除用户在启动时锁定的 `plan` 模式。
- `CronCreate`、`CronDelete`、`CronList` 管理有界私有计划，`/loop` 提供 interval 便捷入口；`ScheduleWakeup` 只占一个 session dynamic slot，具备 60–3600 秒 clamp、替换/停止/过期与 turn rollback。`Monitor` 以 200 ms batch 跟随一个 sandbox command 或固定 ws/wss feed；`RunWorkflow` 启动严格有界的 command DAG，绝不是 JavaScript VM；二者复用 `TaskOutput`/`TaskStop` 与事务式 notification。
- 可信 `--add-dir` root 各自保留独立 scope 与 file history。工具首次获准触及子树时才加载其中的嵌套 `AGENTS.md`，持久 shell cwd 与指令上下文会在 resume 时恢复。通过 `Write`/`Edit` 等可识别路径的文件工具获准修改生效中的 `AGENTS.md` 或 project `SKILL.md` 时，会事务式热刷新下一 model round；解析、预算、hook 或 turn 失败时同时恢复文件与旧的内存上下文。藏在任意 shell 命令中的文件变更不作为热刷新编辑处理。
- `EnterWorktree`、`ExitWorktree` 创建或进入真正登记过的 Git worktree，整体迁移工具上下文，重载当前 `AGENTS.md` 与 skills，同时保留启动目录规则作为更宽的基线；未明确 force 时拒绝删除脏 worktree，退出后恢复原工作区。
- `LSP` 按需启动用户配置的 language server，支持 definitions、references、hover、document/workspace symbols、implementations、call hierarchy、文档同步、有界 diagnostics、只预览而绝不应用 edit 的 rename、重启与干净退出。
- MCP 支持有界 stdio、Streamable HTTP、legacy HTTP+SSE 与 ws/wss JSON-RPC，以及协议协商、session、notification、分页、动态 tool-list、`WaitForMcpServers`、工具、resource/template/prompt、可信 roots 与 form/URL elicitation。WebSocket 重连会重新 initialize，并仅在协商版本与 capability 完全一致时接受新连接；业务请求不会被隐式重放。Resource、resource-link 与 scalar template URI 对模型只暴露有界、可读取的 opaque handle 和安全的 scheme/origin metadata，再由 harness 内部解析；path、userinfo、query 与 fragment 不会回显进模型上下文。图片和 PDF 会变成模型 media block；audio 与其他 opaque binary 只暴露有界 metadata，不转发原始字节。认证只接受可信显式 header，或由环境变量、私有文件、有限时命令提供的 bearer token；OAuth 还支持 protected-resource/authorization-server discovery、PKCE S256/state、显式开启的动态注册、headless callback handoff、私有 refresh 持久化和一次有界 401 refresh retry。Token 状态用私有、有界等待的跨进程锁串行；锁内验证并清理崩溃遗留 atomic temp，只有成功 exchange 后才删除已消费的文件交接。外部 metadata 会被剥离，schema 会被验证；sampling 仍未声明并失败关闭。
- `WebFetch` 以 DNS pinning、私有/保留地址拒绝、逐跳重定向复核、跨 origin 清除 authorization、HTTPS 降级拒绝和正文上限读取文本 HTTP(S)，并按有界 prompt 在本地提取。`WebSearch` 只有在用户提供中立 URL 后才存在，并可对结构化结果应用规范化 allow/block domain filter；嵌套 blocked link 会连同所属整条结果删除，不会留下失去可验证链接的 snippet。
- Hook 覆盖单个工具与 whole batch、user prompt expansion、只改变显示而不改 transcript/model result 的 final message transform、权限请求/拒绝、notification、session、root stop/failure feedback、task、teammate 完成/空闲、subagent、指令加载/文件变化、压缩、worktree 与 cwd 变化。Action 可运行有界 command，或调用已连接 MCP server 上通过 schema 校验的 tool；插值、时间、输出、async 并发和清理都有上限。Root 与 scoped hook 共用同一 async capacity、task registry、observer sequence 和 finalize 状态，丢弃 scope 或结束 session 都不会放走 detached hook。Hook 仍只能来自可信 settings/plugin，只能在公开边界阻止或改写。

可信本地 plugin 是纯 manifest contribution bundle，可以贡献 namespaced skill、custom slash command、hook、custom agent、MCP/LSP server 与 output style。`plugin validate/install/list/update/uninstall` 为本地目录、ZIP 与 checksum-pinned HTTPS ZIP 提供显式 lifecycle，使用私有、带持久 journal 的崩溃可恢复缓存；安装目录为 `0700`，普通文件为 `0600`，可执行文件只保留私有 `0700` 执行语义。未知或不安全事务残留会失败关闭，不会猜测删除。项目不能触发，已安装 plugin 也不能修改当前运行进程。可信 user/plugin Skill 支持有界 argument metadata、用户/模型可调用性、allowed tools、model、fork context、custom agent 与 scoped hooks。自动发现的 project Skill 只被当作数据：其 tool list 只能缩窄权限，model/agent/hook 等可执行 metadata 会被拒绝，也不能遮蔽同名 trusted Skill。可信 settings 也可以直接定义 custom slash command；已发现的 skill 可用 `/skill-name [arguments]` 直接提交，模型仍可按需使用正常的 `Skill` 工具。

```bash
open-agent-harness plugin validate ./plugin.zip
open-agent-harness plugin install https://plugins.example.invalid/tool.zip --sha256 <digest>
open-agent-harness plugin list
```

开放边界同样明确：OAuth 不会启动、控制或内嵌浏览器，也不会暗开 callback listener；authorization URL 与 callback 通过显式私有文件/环境值交接。进程内动态修改 plugin/MCP、任意 transcript 注入/replay 与厂商 callback 控制不属于 stream-JSON 契约；plugin lifecycle 是影响后续进程的独立 CLI 操作。`RunWorkflow` 是声明式 DAG，不是 JavaScript 或下载代码 runtime。协作运行的 harness 进程会用私有锁文件串行 auto-memory 写入；file-history rollback journal 仍是进程内的，不能为独立 OS 进程的编辑排序。MCP elicitation 等待用户响应时会串行该 server 的 RPC reader；headless interaction 与 root 本地 TTY 共用不超过 120 秒的配置上限，超时不会遗留抢读 stdin 的 detached reader。精确的非声明项见 [3 月 31 日通用行为审校](docs/GENERIC_PARITY_2026-03-31.md)。

可执行与网络集成只接受 `~/.open-agent-harness/settings.json` 或显式 `--settings`；仓库不能借自己的 settings 偷渡它们。精简示例：

```json
{
  "strictMcpConfig": true,
  "model": "provider/model-a",
  "models": [
    "provider/model-a",
    {
      "value": "provider/model-b",
      "displayName": "Model B",
      "description": "Configured fallback"
    }
  ],
  "plugins": {"directories": ["/absolute/path/to/plugin"]},
  "outputStyle": "runtime:brief",
  "memory": {"enabled": true, "autoExtract": true},
  "sandbox": {
    "enabled": true,
    "failIfUnavailable": true,
    "network": {"allowedDomains": ["example.com", "*.example.org"]}
  },
  "mcpServers": {
    "local-tools": {
      "type": "stdio",
      "command": "local-mcp-server",
      "args": ["--stdio"],
      "roots": ["."]
    },
    "remote-tools": {
      "type": "streamable-http",
      "url": "https://mcp.example.invalid/rpc",
      "auth": {"type": "bearer-env", "env": "MCP_TOKEN"}
    }
  },
  "lspServers": {
    "rust": {
      "command": "rust-analyzer",
      "extensionToLanguage": {".rs": "rust"}
    }
  },
  "agents": {
    "maxDepth": 3,
    "maxConcurrent": 4,
    "maxTotal": 64,
    "maxBackground": 16
  },
  "worktree": {"baseRef": "head"},
  "web": {
    "allowPrivateNetwork": false,
    "maxBytes": 2097152,
    "search": {
      "endpoint": "https://search.example.invalid/query",
      "queryParameter": "q"
    }
  },
  "hooks": {
    "PreToolUse": [{
      "matcher": "Bash",
      "hooks": [{"type": "command", "command": "/absolute/path/check-tool.sh"}]
    }]
  }
}
```

`models` 是可信配置中显式提供的显示目录，不是后台自动探测：harness 不会访问未声明的 model-list endpoint，也不会硬编码任何厂商别名。当前模型若不在目录中，会以 `Current model` 自动加入；项目 settings 无权选择模型或填充该目录。

Command hook 会从 stdin 收到一个 JSON object，其中包含 `hook_event_name`、`cwd` 和事件 payload。stdout 为空表示继续；普通文本会成为附加 context。退出码 `2`、`{"continue":false}` 或 `{"decision":"block","reason":"..."}` 会阻止操作。成功的 JSON 响应可以返回 `additionalContext`，或 `hookSpecificOutput.updatedInput` / `updatedToolOutput`；其他非零退出码一律失败关闭。Command hook 默认在 workspace 外执行，通过 `HARNESS_WORKSPACE` 接收路径；`workspaceRelative: true` 才显式选择 workspace 相对执行。`mcp_tool` action 则点名一个已配置且已连接的 server/tool，对插值后 input 做 tool schema 校验，再把有界结果映射到同一 outcome contract。Async hook 不能改写正在执行的调用，并会在正常 session shutdown 时收尾。

## 权限

五种模式：

- `default` —— 敏感操作逐一确认。
- `accept-edits` —— 文件编辑放行，其余仍需确认。
- `plan` —— 禁止修改工作区和执行命令；规划状态与会话 metadata 仍可落盘。
- `dont-ask` / `dontAsk` —— 永不弹权限问题；显式 allow 与内在安全的只读可继续，任何本来需要提问的操作直接拒绝；显式 deny 与 plan 限制仍优先。
- `bypass-permissions` —— 跳过交互确认，风险自担；受信与项目的显式 deny 规则仍然优先。

注意这里“权限”的含义：是**你**授权 agent 能对**你的**机器做什么。而在 Anthropic 的词典里，“权限”首先是它授权**你**能不能存在于它的用户列表里。两种权限观，高下自见。

上述“逐一确认”指交互终端。在 `--print` 或其他非交互运行中，凡是本应弹出确认的操作，只要没有受信 allow 规则或 bypass 模式明确授权，就直接拒绝。规范化工作区之外的路径——包括 symlink 逃逸——不会被普通只读或编辑模式悄悄放行。

## 会话与记忆

`~/.open-agent-harness` 是仓库外的私有运行时状态根：settings、session、team、plugin cache、OAuth material 等需要跨 cwd 或跨进程恢复的内容会持久化；`tasks`/`cwd-markers` 属于有界临时状态，未显式保留的条目应在所属任务或进程生命周期结束时清除。这样既不会污染当前仓库，也不会把运行状态误提交进 Git。

- JSONL 会话落在本地磁盘，`--continue` 接续上一场，`--resume` 恢复任意一场；`--fork-session` 创建独立分支，`--resume-at` 从有效消息边界 fork；新运行使用 `--no-session-persistence` 时不创建 transcript。Unix 上的 CI、embedding 或需要独立本地历史边界时，可用 `--session-state-root /absolute/existing/directory` 只把 transcript 与 file-history journal 放进该专用根目录；非 Unix 平台暂时拒绝该 override，因为目前还不能在那里强制等价的私有目录 ACL。该根不得与当前 workspace 或任何 `--add-dir` 重叠；相对路径、不存在路径、普通文件、网络/设备路径及末级 symlink 均被拒绝，且调用方须在启动前把既存根目录设为 `0700`。Resume/continue/fork 始终留在同一规范化边界内，后续 workspace 切换也不能进入该根；持久文件权限为 `0600`。记录会保留完整工具输入和工具结果正文，同时去除 opaque provider state，并脱敏凭据、endpoint secret 与本机绝对路径。本仓库没有藏着一套远程历史服务。每次请求所需的对话上下文当然会送往你配置的 model endpoint；那个 endpoint 是否留存、是否训练，取决于你选择的 endpoint，而不是这个 harness 有资格替它许下的空头承诺。
- 每个持久化 user-message 边界都会建立 committed checkpoint；`Write`、`Edit`、`NotebookEdit` 会记录修改前状态，fork 会继承当前活跃 workspace 的来源 history。交互式 `/rewind` 与 stream-JSON `rewind` 可以预览或恢复 conversation、files 或两者；旧 `rewind_files` 仍作为只恢复文件的兼容入口。交互式 `/resume` 可在当前终端内选择并切换兼容会话，恢复 transcript、workspace/cwd 与文件历史；`/rename` 写入私有标题 sidecar，`/branch [title]` 从当前消息边界建立带父会话关系的新分支。非交互控制面仍返回结构化会话数据。末尾撕裂 JSONL、损坏或 symlink 元数据、孤儿 file transaction、陈旧 team member 与未确认 scheduled prompt 均按有界失败关闭规则恢复。Rollback journal 协调 harness 内部状态，不替任意独立 OS 进程的文件编辑排序。
- Workspace memory 默认关闭。只有可信 `memory.enabled` settings 才会暴露有界本地 `Memory` 工具，提供 index/recall/remember/forget；检索值会标记为不可信工作区数据，疑似 secret 会被拒绝，落盘采用私有原子写。可信 `memory.autoExtract=true` 会在 root turn 完成后调度有界、工具受限的 best-effort 提取；重叠调度会合并，只提取最新待处理 turn。提取不能删除条目、调用运行时工具或改变 transcript。协作运行的 harness 进程通过私有 `.MEMORY.lock` 串行初始化与更新；进程崩溃遗留的锁会失败关闭，确认没有 writer 后才可显式删除。这仍是本地 memory，不是远程服务。
- 会话级 Todo 与按工作区持久化的任务列表，支持状态、负责人、依赖关系和 metadata。

Context 压缩同样透明：手动 `/compact [instructions]`；自动压缩在有效 context window 留出输出空间和 13,000-token 缓冲后触发。Provider 明确返回 context 或 media size 拒绝时，最多触发一次反应式压缩重试。`HARNESS_DISABLE_AUTO_COMPACT=1` 关闭阈值自动压缩，`HARNESS_DISABLE_COMPACT=1` 关闭包括反应式重试在内的全部压缩。

## 工程指令

Anthropic 的系统提示词是商业机密，泄露版本在网上流传，官方从不承认也从不公开——用户被数千词看不见的指令支配着自己的工程决策，还被要求为此付费；而地球上另一大批开发者，连被这些看不见的指令支配的资格都没有。两边都不可接受。

在这里，默认提示词装配是 `src/prompt.rs` 中普通、完整的开源 Rust，也可用 `--system-prompt` 或 `--system-prompt-file` 替换默认部分。稳定 harness 契约、实时注册工具清单、当前权限模式、工作区指令、压缩续接和 delegated-agent 指令仍是彼此分开的层，不会被揉成一篇不可见的品牌宣言；工具激活与权限状态会在每次模型 round 重新生成。Harness 不会悄悄把本机用户名、绝对工作目录、OS、架构或 Git metadata 塞进提示词。`/init` 会把同一文件里的开放仓库分析提示交给当前模型，并走正常、可中断的消息循环；`AGENTS.md` 的创建或整体改进只能通过公开文件工具与当前权限模式完成，不存在隐藏生成器，也不会生成厂商专属指令文件。

工程指令写在普通纯文本 `AGENTS.md` 中：全局放 `~/.open-agent-harness/AGENTS.md`，项目指令可沿当前工作目录祖先链或获准访问的子树分层放置，从宽到窄加载；嵌套文件在首次工具访问时发现，越接近目标的适用文件优先级越高。本地工作流放在 `.open-agent-harness/skills/<name>/SKILL.md`；`Skill` 工具只读取文本，绝不会擅自执行其中附带的脚本，`/name arguments` 也可直接提交已发现的 skill。可信 settings 可以定义有界 custom command，显式配置的本地 plugin manifest 可以贡献 namespaced skill、command、hook、agent、MCP/LSP 定义与 output style。越出作用域的 symlink 会被拒绝。`--bare` 关闭项目 settings/context 自动发现但保留显式 `--settings`；`--safe-mode` 更强，会移除 instructions、skills、plugins、hooks、MCP/LSP、commands、agents、styles、workflows 与 memory，只保留 model 选择、内置工具、permissions 和 sandbox policy。所有影响都被换成了你能阅读、替换、删除的源码、文件或开关——不分时区，不分区号。

## 构建与验证

```bash
cargo fmt --all -- --check
cargo +1.85.0 check --locked --all-targets
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked --release
scripts/audit-harness.sh
```

仓库会把每一个 Rust warning 直接提升为 error，并用声明的最低 Rust 1.85 单独编译全部 target。第一份完整发行版如此，任何可合并的 PR 也必须如此：没有 warning 配额，没有虚构的 MSRV，没有仪式性的抑制，更没有“以后再收拾”。

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
