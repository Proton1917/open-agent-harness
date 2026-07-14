# Changelog

## Unreleased

- Aligned the declared open surface with the provider-neutral general behavior of the static 2026-03-31 comparison snapshot; this does not claim identical source structure, prompts, assets, account behavior, or vendor-specific features.
- Hardened permissions with atomic compound-shell analysis, nested/wrapper/expansion checks, canonical filesystem identities, `Read` deny filtering for `Glob`/`Grep`, Windows drive/UNC/device-path handling, fail-closed retryable rollback for concurrent sibling file overlap, and a non-prompting `dontAsk` mode that permits only explicit allows and intrinsically safe reads.
- Added trusted multiple roots, first-touch nested `AGENTS.md`, transactional hot refresh after approved path-aware file-tool edits to `AGENTS.md`/project Skills, persistent shell cwd, worktree/session restoration, file-only rewind dry-run, isolated agent worktrees, durable cross-process agent resume, and explicit root-user approval before leaving plan mode.
- Expanded persistent local teams with automatic non-consuming mailbox/completion delivery, task and idle lifecycle handling, deletion/GC, and total per-workspace count/byte quotas; background Bash and agent completions use the same transactional delivery model.
- Implemented full bounded execution metadata for trusted user/plugin Skills while keeping automatically discovered project Skills authority-narrowing. Trusted plugins now have explicit validate/install/list/update/uninstall CLI lifecycle operations for local directories, ZIPs, and checksum-pinned HTTPS ZIPs, with private transactional cache updates.
- Expanded local hooks across permission, notification, root stop/failure feedback, task/team, instruction loading/file changes, subagent, compaction, worktree, and cwd boundaries; added `PostToolBatch`, `UserPromptExpansion`, display-only `MessageDisplay`, and schema-checked `mcp_tool` actions with bounded interpolation and async cleanup. `ConfigChange` remains absent because accepted in-process configuration mutation is intentionally unsupported.
- Extended MCP with form/URL elicitation, model-visible image/PDF mapping, bounded metadata for audio/opaque binary, readable opaque resource/resource-link/template handles, configured roots, stdio/Streamable HTTP/legacy HTTP+SSE/WebSocket transports, OAuth metadata discovery and PKCE with private refresh persistence, and `WaitForMcpServers`. Sampling and undeclared capabilities remain fail-closed.
- Added prompt-guided bounded `WebFetch`, structured `WebSearch` domain filtering, one reactive compaction retry after provider-declared context/media-size rejection, and opt-in trusted, bounded, best-effort `memory.autoExtract` after completed root turns; overlapping extraction schedules coalesce to the latest pending turn. Cooperating memory writers serialize first initialization and updates through a private lock file; file-history rollback journals remain process-local.
- Added `CronCreate`/`CronDelete`/`CronList`, `/loop`, transactional `ScheduleWakeup`, sandboxed command/WebSocket `Monitor`, and strict declarative background `RunWorkflow` DAGs with shared task output/stop semantics and bounded shutdown.
- Added terminal/headless `AskUserQuestion`, full-validator structured output, model-visible image/PDF/notebook content, private session forks/checkpoints, trusted Bash sandbox policies with DNS-pinned `allowedDomains`, and `--safe-mode` customization suppression.
- Expanded bidirectional stream-JSON with bounded priority lanes, `cancel_async_message`, interruption receipts that preserve queued work, command lifecycle events, optional user-message replay acknowledgements, and opt-in tool-free prompt suggestions. Live in-process plugin/MCP mutation and vendor callback families remain outside the open control contract.
- Scrubbed every configured MCP credential environment variable from model-reachable and extension child processes, made Monitor notification rollback/loss limits exact, removed Workflow and unretained background-task capture leaks, and closed queue, wakeup, control-reader startup, and cross-platform descendant-process cleanup races found by multi-agent cross-audit.
- Closed the final generic trust-boundary findings: model input cannot forge `AskUserQuestion` answers; API paths are query/fragment-free and transport errors do not retain request URLs; nested blocked search links remove their complete result; plugin executable state survives private-mode installation; scoped hooks share one finalized async runtime; and persistent Team updates/quotas serialize across processes through a validated private lockfile.
- Closed crash/cancellation cleanup gaps found by the release-boundary audit: plugin install/update/uninstall now use a durable recovery journal with real subprocess crash-point coverage, while candidate validation avoids re-entering the installed-registry lock and still checks non-activating monitor references; OAuth token state uses a bounded cross-process lock and validates/removes crash-stale atomic files; consumed callback/authorization handoffs are deleted after a successful exchange; Team lock waits and stale-temp cleanup are bounded; interrupted no-persistence agents enter the query transaction before rollback; and test task captures use isolated temporary roots instead of the real user home.

## v0.4.0 — 2026-07-13

- Added native provider-neutral adapters for content-block Messages, OpenAI-compatible Chat Completions, and OpenAI-compatible Responses, with path inference, explicit format selection, streaming and complete JSON responses, legacy Chat token-field selection, and optional streamed usage negotiation.
- Added strict protocol-specific SSE state machines. Truncated streams, non-completed or stream-conflicting terminal payloads, mismatched finish reasons, missing/conflicting/sparse indices, mixed tool-call dialects, duplicate lifecycle events, malformed tool arguments, and all documented midstream endpoint error forms now fail before tool execution; null usage and OpenRouter keepalive/usage conventions are handled safely.
- Added both documented OpenRouter Responses event-name families, exact stateless Responses item replay, and ordered Chat `reasoning_details` continuity without persisting opaque provider state or forwarding it to another wire protocol.
- Hardened tool execution against duplicate call IDs, malformed historical pairing, empty replacements, UTF-8 byte-limit bypasses, and replacement amplification before allocation.
- Reworked local-agent scheduling and cleanup so cancellation remains observable, timeouts include queueing, nested foreground delegation cannot deadlock at concurrency one, active IDs are reserved atomically, and model changes propagate to future subagents.
- Preserved the current user prompt verbatim across automatic history compaction and made `Ctrl-C` transactional in plain interactive and `--print` modes as well as the conversational terminal.
- Added local mock-server compatibility tests for multi-round Chat and Responses tool flows, OpenRouter stream conventions, complete JSON mode, credential redaction, and fail-closed truncation. No public endpoint or real credential is used by the test suite.

## v0.3.0 — 2026-07-12

- Rebuilt the interactive conversational-terminal structure in Rust: startup card, bordered Unicode composer, bounded paste, history, multiline input, safe permission-mode cycling, live request state, streamed response rendering, and tool-call/result rows.
- Added transactional active-turn interruption. `Ctrl-C` cancels model or tool work, rolls back uncommitted messages and turn-owned background jobs, and returns to the composer; double `Ctrl-C` exits only from idle input.
- Replaced the minimal generic prompt with an open layered prompt assembler covering the harness contract, task execution, action boundaries, live tools and permission state, project instructions, context continuity, compaction, and delegation without automatically transmitting absolute cwd or device metadata.
- Added a provider-neutral `/init` prompt that analyzes a repository and creates or coherently improves `AGENTS.md`.
- Preserved the non-TTY fallback and exact `--print` text, JSON, and stream-JSON contracts.

## v0.2.0 — 2026-07-12

The first release covering the complete open backend migration boundary while preserving the existing CLI.

- Added a bounded deferred-tool registry with exact `ToolSearch` activation and dynamic discovery refresh.
- Added local foreground/background/resumable subagents with independent histories, recursion and concurrency limits, task aliases, cancellation, and shutdown cleanup.
- Added provider-neutral MCP stdio and Streamable HTTP transports with sessions, SSE notifications, tools, resources, templates, prompts, pagination, and list-change refresh.
- Added lazy local LSP clients with document synchronization, definitions, references, hover, symbols, implementations, call hierarchy, diagnostics, restart, and shutdown.
- Added safe Git worktree entry/exit, session-local plan transitions, provider-neutral textual web fetch/search, and trusted bounded command hooks.
- Added a shared bounded JSON-RPC runtime, stricter process lifecycle handling, Unix process-group cleanup, Windows process-tree cleanup, and Windows-native shell selection.
- Expanded the zero-warning gate to the declared Rust 1.85 minimum and to Linux, macOS, and Windows; retained the transparent Shell repository auditor as an intentional quality tool while keeping the runtime entirely Rust.
- Replaced the stale migration claims with an explicit coverage matrix and exact non-claims for UI, remote, media, and optional protocol features outside the open harness boundary.

## v0.1.0 — 2026-07-12

The first public release of `open-agent-harness`.

- Provider-neutral messages endpoint with SSE and JSON responses, retry handling, exact request-contract tests, redirect refusal, proxy opt-in, and bounded traffic.
- Multi-round model/tool loop with message normalization, ordered read-only concurrency, usage accounting, rollback on failed model rounds, and transparent context compaction.
- Fifteen Rust tools covering files, notebooks, search, shell/background processes, planning state, and local text workflows.
- Strict tool schemas, canonical workspace boundaries, stale-write protection, atomic writes, private persistence, subprocess credential isolation, Unix process-group cleanup, and explicit resource ceilings.
- Local sessions, resume/continue, todos, persistent task relationships, layered `AGENTS.md`, local skills, and bare mode.
- Warnings promoted to errors in the repository and CI; deterministic tests use local mock endpoints and no public service.
- MIT licensed, with private comparison material and local engineering instructions excluded from Git and release artifacts.
