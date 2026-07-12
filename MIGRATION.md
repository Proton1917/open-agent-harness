# Harness logic audit

## Release scope

`v0.1.0` is an original, provider-neutral Rust coding-agent harness. The private comparison snapshot is evidence, not a dependency: it remains read-only under the ignored `reference/` tree, is never compiled or executed, and is absent from Git history and release artifacts.

The release scope is deliberately the harness rather than a branded product shell:

1. unchanged CLI input and output surfaces;
2. a user-configured messages endpoint with SSE or JSON reconstruction;
3. the model → tool → result loop, permissions, safe concurrency, and context accounting;
4. bounded local file, notebook, search, shell, background-task, planning, and workflow tools;
5. private local sessions, compaction, project instructions, settings, and skills;
6. deterministic local protocol tests, privacy tests, static auditing, and a warning-free release build.

Account systems, subscriptions, entitlement checks, telemetry, experiments, proprietary UI, closed remote services, voice characters, marketing behavior, and copied prompts or assets are intentionally outside the repository.

## Coverage matrix

| Harness subsystem | Rust implementation | Status | Verification evidence |
|---|---|---:|---|
| CLI and output formats | `src/cli.rs`, `src/main.rs` | complete | help/version smoke; text, JSON, and stream JSON paths |
| Endpoint trust boundary | `src/api.rs`, `src/config.rs` | complete | same-origin URL validation, no redirects, proxy opt-in, bounded bodies |
| Request privacy contract | `src/api.rs`, `tests/privacy_boundary.rs` | complete | raw request contains only six documented fields and bearer auth |
| Credential isolation | `src/main.rs`, `src/tools/bash.rs`, `src/tools/grep.rs` | complete | real binary proves endpoint token is absent from tool subprocesses and message bodies |
| SSE and JSON response reconstruction | `src/api.rs` | complete | block accumulation, partial tool JSON, truncation and malformed-stream failures |
| Query and tool loop | `src/query.rs` | complete | multi-round tool use/result test; failed model round rolls back in-memory history |
| Message normalization | `src/messages.rs` | complete | adjacent-role merge, orphan removal, interrupted-pair repair |
| Context accounting | `src/tokens.rs`, `src/query.rs` | complete | messages, system text, and tool schemas included in estimates |
| Context compaction | `src/compact.rs`, `src/query.rs` | complete | manual and automatic thresholds, continuation formatting, disabled modes |
| Strict tool schemas | `src/tools/schema.rs`, `src/tools/mod.rs` | complete | unknown or invalid fields fail before permission checks and side effects |
| Ordered safe concurrency | `src/tools/mod.rs` | complete | consecutive read-only calls run together; mutations remain barriers |
| File editing | `read.rs`, `write.rs`, `edit.rs` | complete | full-read guard, exact stale-content rejection, unique replacement, atomic write |
| Notebook editing | `src/tools/notebook.rs` | complete | replace, insert, delete, cell IDs, output clearing, size boundary |
| Search | `src/tools/glob.rs`, `src/tools/grep.rs` | complete | real matches, pagination, traversal, time, byte, and process limits |
| Shell and background processes | `src/tools/bash.rs`, `src/tools/tasks.rs` | complete | private bounded capture, timeout, task output, stop, process-group cleanup |
| Planning state | `src/tools/work_items.rs` | complete | bounded todo/task persistence, ownership, dependencies, metadata, private files |
| Permissions and workspace scope | `src/permissions.rs`, `src/tools/mod.rs` | complete | non-interactive denial, deny precedence, outside-path and symlink-escape tests |
| Settings trust | `src/config.rs` | complete | project files cannot set environment, redirect endpoint, or elevate permission mode |
| Sessions and resume | `src/session.rs` | complete | private permissions, clear/compact boundaries, bounded JSONL, sanitized tool records |
| Engineering instructions | `src/context.rs` | complete | broad-to-specific `AGENTS.md`, bare mode, scope-escaping symlink rejection |
| Local workflows | `src/skills.rs`, `src/tools/skill.rs` | complete | precedence, size/count limits, text-only loading, no automatic script execution |
| Resource ceilings | `src/api.rs`, `src/main.rs`, `src/session.rs`, `src/tools/` | complete | explicit request, response, prompt, file, output, scan, store, and transcript limits |
| Warning-free source gate | `.cargo/config.toml`, `.github/workflows/ci.yml` | complete | warnings are errors in test, clippy, and release compilation |
| Open-source boundary | `.gitignore`, `scripts/audit-harness.sh` | complete | private reference and local instructions untracked; source and binary static scan |

The registered model surface contains 15 tools: six file/search tools, three execution tools, five planning tools, and one local workflow loader.

## Claims not made by this release

This release does not pretend that every feature ever placed around an agent loop belongs in its core. It does not claim a built-in MCP client, LSP client, worktree manager, remote web service, or recursive subagent scheduler. Those can be added later as open, independently testable integrations without changing the CLI or weakening the trust boundary. None is required for a claim made in `README.md`.

## Release gate

Every command below must finish successfully and print no compiler warning:

```bash
cargo fmt --all -- --check
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked --release
bash -n scripts/audit-harness.sh
scripts/audit-harness.sh
target/release/open-agent-harness --help
target/release/open-agent-harness --version
```

The static audit must additionally prove that the release binary contains no removed brand identifier, no private reference file is tracked, `AGENTS.md` remains ignored and untracked, no hard-coded public endpoint exists in runtime source, and the release contains no hidden account or telemetry metadata.
