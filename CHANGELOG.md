# Changelog

## v0.1.0 — 2026-07-12

The first public release of `open-agent-harness`.

- Provider-neutral messages endpoint with SSE and JSON responses, retry handling, exact request-contract tests, redirect refusal, proxy opt-in, and bounded traffic.
- Multi-round model/tool loop with message normalization, ordered read-only concurrency, usage accounting, rollback on failed model rounds, and transparent context compaction.
- Fifteen Rust tools covering files, notebooks, search, shell/background processes, planning state, and local text workflows.
- Strict tool schemas, canonical workspace boundaries, stale-write protection, atomic writes, private persistence, subprocess credential isolation, Unix process-group cleanup, and explicit resource ceilings.
- Local sessions, resume/continue, todos, persistent task relationships, layered `AGENTS.md`, local skills, and bare mode.
- Warnings promoted to errors in the repository and CI; deterministic tests use local mock endpoints and no public service.
- MIT licensed, with private comparison material and local engineering instructions excluded from Git and release artifacts.
