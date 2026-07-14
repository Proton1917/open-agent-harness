# Harness migration audit

## Boundary and method

The current tree is an original, provider-neutral Rust implementation of the
generally useful coding-agent behavior selected from the static 2026-03-31
comparison snapshot. The private snapshot is evidence, never a build input: it
stays under ignored `reference/`, is not executed, is absent from Git history,
and cannot enter a release artifact.

A later checksummed archive was also reviewed statically for additional
provider-neutral generic behavior. Selected open extensions from that review
are included in the matrix below; this does not broaden the claim to source
structure or vendor-specific product parity.

This audit concerns observable behavior and open protocol contracts. It does
not claim identical source structure, prompts, wording, terminal code, assets,
accounts, hosted services, or vendor-specific features. Account, subscription,
identity, entitlement, telemetry, experiments, marketing behavior, copied
implementation, and copied assets remain outside the project.

## Coverage matrix

| Harness subsystem | Rust implementation | Status | Verification evidence |
|---|---|---:|---|
| CLI and output formats | `src/{cli,main,terminal,control}.rs` | complete for declared surface | text/JSON/stream-JSON; bounded priority lanes, cancellation receipts, queued-message cancellation, lifecycle/replay events, prompt suggestions; Rust composer and transactional interruption |
| Prompt, web, and compaction | `src/{prompt,query,compact,web_tools}.rs` | complete for declared behavior | separate stable/live/style/instruction/delegation layers; prompt-guided fetch; search domain filters; one reactive retry after endpoint context/media-size rejection |
| Model endpoint boundary | `src/{api,config,protocol}.rs` | complete for declared formats | Messages, OpenAI-compatible Chat Completions and Responses; query/fragment-free path inference/override, same-origin validation, URL-free transport errors, no redirects, proxy opt-in, bounded JSON/SSE |
| Request privacy and credentials | `src/{api,protocol,session,process}.rs`, subprocess modules | complete | protocol-scoped provider state, sanitized persistence, bearer header only; endpoint and configured MCP credential env names are removed from model-reachable/extension child processes |
| Query ordering and transactions | `src/{query,messages,file_history,cron,agents}.rs`, `src/tools/mod.rs` | complete for one-process contract | strict call/result pairing, mutation barriers, atomic message/file/new-task/notification/dynamic-wakeup rollback; agent cancellation enters the same query transaction before cleanup; concurrent sibling overlap fails closed and remains retryable |
| Structured output and interaction | `src/structured_output.rs`, `src/tools/ask_user.rs`, `src/{query,control}.rs` | complete | full JSON Schema final result; terminal/headless question and plan approval paths fail closed without a responder; model input cannot supply trusted user answers |
| File, media, and notebooks | `src/tools/{read,write,edit,notebook}.rs`, `src/protocol.rs` | complete for declared formats | text, image, PDF, notebook cells/outputs/media; full-read guard, freshness, unique replacement, byte ceilings, atomic write |
| Native search | `src/tools/{glob,grep}.rs` | complete | bounded traversal/regex/context/pagination plus permission-aware result filtering |
| Shell and background work | `src/tools/{bash,tasks}.rs`, `src/{process,sandbox,sandbox_proxy}.rs` | complete | compound permission analysis; bounded private capture with cancellation-safe cleanup and injectable test/embedding root; automatic non-consuming completion notices; Unix groups and Windows trees; optional/required sandbox policy plus authenticated DNS-pinned domain allowlist proxy |
| Permissions and path identity | `src/{permissions,tools/mod}.rs` | complete for declared policy | atomic shell commands, nested execution, wrappers and opaque expansion; canonical file identities; deny precedence; noninteractive and symlink fail-closed behavior; Windows drive/UNC/device paths |
| Trusted roots and instructions | `src/{cli,context}.rs`, `src/tools/mod.rs` | complete | `--add-dir`, separate root scope/history, first-touch nested `AGENTS.md`, persistent cwd/context refresh, transactional hot refresh after path-aware file-tool edits to `AGENTS.md`/project Skills, scope-safe symlinks |
| Sessions and file history | `src/{session,file_history}.rs` | complete for declared scope | continue/resume/fork/resume-at, private bounded records, per-user checkpoints, file-only rewind and diff dry-run, worktree/cwd restoration |
| Plan and durable tasks | `src/plan.rs`, `src/tools/work_items.rs` | complete | root-user approval before plan exit; user plan lock preserved; bounded task ownership/dependencies/metadata and lifecycle hooks |
| Scheduling, monitoring, and workflows | `src/{cron,monitor,workflow}.rs`, `src/tools/{cron,wakeup,workflow}.rs` | complete for declared local scope | private session/durable cron, `/loop`, transactional single-slot wakeups, command/ws monitors with exact rollback, and strict background workflow DAGs using Bash permission/sandbox/process controls |
| Local agents and worktrees | `src/{agents,worktree}.rs` | complete for local scope | narrowing custom definitions, background/foreground, isolated worktrees, durable cross-process resume when persistence is available, and cancellation/timeout rollback including no-persistence hot-refresh edits |
| Persistent local teams | `src/team.rs`, `src/tools/team.rs` | complete for declared local scope | bounded membership/assignment/mailbox/status/stop/shutdown, automatic tracked-team notices, task/idle events, delete/GC, per-workspace count/byte quota, bounded private cross-process project locking, and validated batch cleanup of crash-stale temporary state |
| Skills | `src/skills.rs`, `src/tools/skill.rs` | complete for declared trust model | trusted user/plugin metadata covers arguments, invocability, allowed tools, model, fork context, custom agent, and scoped hooks; project Skills only narrow authority |
| Plugins and output styles | `src/{plugins,plugin_manager}.rs`, trusted settings merge | complete for declared local lifecycle | namespaced skills/commands/hooks/agents/MCP/LSP/output styles plus explicit validate/install/list/update/uninstall; local directory/ZIP or checksum-pinned HTTPS ZIP; private crash-recoverable journaled cache with normalized `0700` executable and `0600` ordinary files |
| Opt-in workspace memory | `src/{auto_memory,tools/memory}.rs` | complete for declared local scope | explicit bounded tool plus optional bounded best-effort completed-root-turn extraction, with overlapping schedules coalesced to the latest pending turn; untrusted-data labeling, secret rejection, constrained extraction tool, private atomic commit |
| Deferred registry | `src/tools/mod.rs` | complete | bounded exact activation, dynamic discovery refresh, duplicate/size rejection, narrowed delegated registries |
| LSP client | `src/{lsp,rpc}.rs` | complete for declared operations | lazy initialize/sync/query, symbols, diagnostics, non-applying rename preview, restart, cancellation, shutdown |
| MCP client | `src/{mcp,mcp_oauth,mcp_websocket,rpc}.rs` | complete for declared capabilities | stdio, Streamable HTTP, legacy HTTP+SSE, ws/wss; sessions, pagination, notifications, wait/status, tools/resources/templates/prompts, roots, elicitation, OAuth metadata/PKCE/private refresh with bounded cross-process locking and stale-temp cleanup, opaque URI-safe handles and bounded media mapping |
| Local hooks | `src/{hooks,plugins,mcp}.rs` | complete for real lifecycle boundaries | matcher, sync/async/once, command or connected `mcp_tool`, bounded interpolation/JSON/time, rewrite/block/context; tool batch, prompt expansion, display-only final transform, permission/notification/stop/task/team/instruction/compaction/worktree/cwd events; one shared root/scoped async lifecycle |
| Resource ceilings and cleanup | `src/{api,rpc,process,team,monitor,workflow,plugin_manager,mcp_oauth}.rs`, `src/tools/` | complete | hard request/response/process/file/count/depth/time limits; bounded priority/notification queues and capture files; failed control-reader startup is reported without panic; unretained Monitor/task captures and Windows/Unix process trees are cleaned; plugin/OAuth/Team crash residue is validated before bounded recovery or removal |
| Warning-free portable gate | `.cargo/config.toml`, `.github/workflows/ci.yml` | declared release gate | Rust 1.85 all-target build plus Linux/macOS/Windows test and clippy paths |
| Open-source boundary | `.gitignore`, `scripts/audit-harness.sh` | complete | Rust runtime, transparent maintenance scripts, ignored comparison material, artifact scan |

The active tool surface is capability-driven. `StructuredOutput`, `Memory`,
Team, agents, worktrees, web, LSP, and MCP helpers appear only when enabled or
selected. Plugin contributions and output styles come only from explicitly
trusted local manifests/settings; project settings can tighten deny rules but
cannot install executable/network integrations or elevate authority.

## Deliberate exclusions and exact non-claims

- No account, subscription, billing, identity, entitlement, update,
  experiment, analytics, geofencing, or hidden remote-control path.
- No claim of source-structure, byte-for-byte, prompt, branding, asset, or
  vendor-specific parity with the comparison material.
- MCP OAuth does not launch, automate, or embed a browser and does not open an
  implicit callback listener. It performs standards-based metadata discovery
  and PKCE, writes the authorization URL to a configured private file, accepts
  the explicit callback through a private file or named environment value, and
  persists refresh material privately. Dynamic client registration is opt-in.
- No graphical browser or general document-rendering UI. File/MCP media is
  model-visible bounded content; web tools are textual fetch/search adapters.
- Stream-JSON does not permit live in-process plugin/MCP installation or
  configuration mutation, arbitrary transcript injection/replay, or vendor
  callback families. Plugin lifecycle changes are explicit CLI operations and
  affect a later process; user-message replay is only an acknowledgement of an
  accepted input UUID/content pair.
- `RunWorkflow` is a strict declarative command DAG, not a JavaScript VM or an
  arbitrary code-loading workflow engine. Every command step still crosses the
  ordinary Bash permission, sandbox, timeout, capture, and process-tree gates.
- `ConfigChange` is not emitted: runtime environment/config mutation is
  rejected, so there is no accepted change boundary to observe.
- Explicit `TaskStop` remains an immediate destructive action: a process that
  was stopped cannot be resurrected if a later operation in the same turn
  fails. Turn rollback still removes newly launched tasks, restores delivery
  cursors, and cleans unretained captures.
- Automatic team mailbox delivery tracks teams opened or created in the
  current process and does not consume explicit mailbox reads. Persisted teams
  must be opened before automatic tracking begins.
- Cooperating harness processes serialize auto-memory initialization and updates
  through a private `.MEMORY.lock`. A crash-stale lock fails closed and requires
  explicit removal
  after verifying that no writer remains; unrelated external writers do not
  participate in this protocol.
- The file-history rollback journal is also process-local. Independent OS
  processes editing the same workspace do not share transaction ordering;
  file freshness checks and atomic writes still apply.
- MCP elicitation waits synchronously on that server's RPC reader. Other
  runtime workers continue, but the same MCP connection does not process
  another server request until the user response or cancellation. Headless and
  direct local TTY interaction share a configured ceiling of at most 120
  seconds; timeout leaves no detached stdin reader.
- A completion hook may reject the recorded outcome of a one-shot team member,
  but cannot resume an already exited process; the assignment fails closed
  instead of remaining falsely running.
- LSP is limited to the operations declared by its tool schema. Subagents,
  worktrees, mailboxes, and teams are local; no remote/cloud team service,
  hosted session, or graphical control plane is implied.
- `WebSearch` requires a URL supplied by trusted configuration. The repository
  ships no search provider, endpoint, credential, or fallback relay.

These boundaries keep the implementation open, testable, provider-neutral,
and honest. The precise release claim is “aligned with the 2026-03-31
snapshot's provider-neutral general behavior for the declared open scope,” not
“complete parity with a proprietary product.”

## Release gate

Every command below must finish successfully and print no compiler warning:

```bash
cargo fmt --all -- --check
cargo +1.85.0 check --locked --all-targets
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked --release
bash -n scripts/audit-harness.sh
scripts/audit-harness.sh
target/release/open-agent-harness --help
target/release/open-agent-harness --version
```

CI repeats tests and clippy on Linux, macOS, and Windows. The static audit also
checks that comparison files are untracked, the release artifact does not
contain private reference material or hidden account/telemetry metadata, the
runtime has no hard-coded public endpoint, and required engineering
instructions remain outside release packaging.
