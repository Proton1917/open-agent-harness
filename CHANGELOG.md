# Changelog

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
