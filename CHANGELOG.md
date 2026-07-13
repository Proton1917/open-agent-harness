# Changelog

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
