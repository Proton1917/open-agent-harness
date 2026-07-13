# Harness migration audit

## Boundary and method

The current tree is an original, provider-neutral Rust implementation of general coding-agent harness behavior. The private comparison snapshot is static evidence, never a build input: it stays under the ignored `reference/` tree, is not executed, is absent from Git history, and cannot enter a release artifact.

The migration preserves the machine-facing CLI contract and rebuilds both the reusable backend machinery and the observable conversational-terminal structure in Rust. Account systems, subscriptions, identity, entitlement, telemetry, experiments, closed remote services, marketing behavior, copied prompts, copied implementation, and copied assets are outside the project.

## Coverage matrix

| Harness subsystem | Rust implementation | Status | Verification evidence |
|---|---|---:|---|
| CLI and output formats | `src/cli.rs`, `src/main.rs`, `src/terminal.rs` | complete for declared surface | stable text/JSON/stream-JSON print paths; Rust composer, history, multiline input, mode cycling, event rows, transactional interrupt |
| Prompt assembly and init | `src/{prompt,query,commands,compact,agents}.rs` | complete for declared layers | open stable contract, live tools/mode, workspace context, compaction/delegation prompts, `/init` for `AGENTS.md`; no automatic absolute cwd or device metadata |
| Model endpoint boundary | `src/{api,config,protocol}.rs` | complete for declared formats | Messages, OpenAI-compatible Chat Completions and Responses; path inference/override, same-origin validation, no redirects, proxy opt-in, bounded JSON/SSE |
| Request privacy contract | `src/{api,protocol,session}.rs`, `tests/{privacy_boundary,protocol_compat,protocol_fail_closed}.rs` | complete | protocol-specific raw requests contain only documented fields and bearer authorization; Responses item identity and Chat reasoning state are memory-only and protocol-scoped |
| Credential isolation | `src/main.rs`, subprocess modules, `tests/credential_isolation.rs` | complete | model token removed before workers and excluded from child environments |
| Query loop and rollback | `src/query.rs`, `src/messages.rs` | complete | multi-round tool flow, usage accumulation, strict unique call IDs, pairing repair, atomic failure rollback |
| Context accounting and compaction | `src/tokens.rs`, `src/compact.rs`, `src/query.rs` | complete | system, messages, tools, output reserve, automatic/manual compaction; the active user prompt is preserved verbatim |
| Strict schemas and scheduling | `src/tools/schema.rs`, `src/tools/mod.rs` | complete | validation before permission; ordered bounded concurrency with mutation barriers |
| File and notebook tools | `src/tools/{read,write,edit,notebook}.rs` | complete | full-read guard, exact freshness, unique replacement, pre-allocation size proof, UTF-8 byte limits, atomic write, cell operations |
| Native search tools | `src/tools/{glob,grep}.rs` | complete | bounded traversal, regex, filters, context, pagination, file/time/byte ceilings |
| Shell and background jobs | `src/tools/{bash,tasks}.rs`, `src/process.rs` | complete | private bounded capture, blocking/nonblocking output, stop, Unix groups, Windows trees |
| Todo and task state | `src/tools/work_items.rs` | complete | bounded private persistence, ownership, dependencies, metadata |
| Session plan state | `src/plan.rs`, `src/permissions.rs` | complete | dynamic enter/exit; user-locked plan mode cannot be escaped by a tool |
| Permissions and workspace scope | `src/permissions.rs`, `src/tools/mod.rs` | complete | deny precedence, noninteractive denial, canonical and symlink boundaries |
| Settings trust | `src/config.rs` | complete | project settings can tighten deny rules only; executable/network settings stay trusted |
| Sessions and resume | `src/session.rs` | complete | private bounded JSONL, clear/compact boundaries, sanitized tool records |
| Engineering instructions | `src/context.rs` | complete | broad-to-specific `AGENTS.md`, bare mode, scope-safe symlink handling |
| Local workflows | `src/skills.rs`, `src/tools/skill.rs` | complete | bounded text loading and precedence; bundled files are never auto-executed |
| Deferred tool registry | `src/tools/mod.rs` | complete | bounded search/select, exact activation, dynamic refresh, duplicate/size rejection |
| Local subagents | `src/agents.rs`, `tests/agent_flow.rs` | complete | independent histories, recursion/concurrency/total caps, queue-inclusive timeout, active-ID reservation, nested scheduling, background/resume/cancel/cleanup |
| Git worktree isolation | `src/worktree.rs` | complete | create/enter/keep/remove, dirty refusal, context/instruction/skill refresh, real Git tests |
| LSP client | `src/lsp.rs`, `src/rpc.rs` | complete for declared operations | lazy initialize/sync/query, diagnostics, restart, cancellation, shutdown, real subprocess test |
| MCP client | `src/mcp.rs`, `src/rpc.rs` | complete for declared capabilities | stdio and Streamable HTTP, sessions, SSE, pagination, tools/resources/templates/prompts, refresh |
| Explicit web tools | `src/web_tools.rs` | complete for textual fetch/search | DNS pinning, address policy, redirect revalidation, header stripping, body limits |
| Local command hooks | `src/hooks.rs` | complete for declared events | matcher, sync/async/once, bounded JSON I/O, timeout, rewrite/block/context responses |
| Resource ceilings and cleanup | `src/api.rs`, `src/rpc.rs`, `src/process.rs`, `src/tools/` | complete | hard request/response/process/file/count/depth/time limits and shutdown services |
| Warning-free portable gate | `.cargo/config.toml`, `.github/workflows/ci.yml` | complete | warnings are errors; Linux release audit plus macOS and Windows test/clippy jobs |
| Open-source boundary | `.gitignore`, `scripts/audit-harness.sh` | complete | Rust-only runtime, practical source tooling, ignored references/instructions, binary scan |

The always-active base surface remains fifteen local tools. The normal executable also exposes `ToolSearch`, which lazily activates local agents, plan transitions, worktrees, web access, LSP, and configured MCP tools. MCP resource/prompt helpers appear only when a configured server advertises the corresponding capability.

## Deliberate exclusions and exact non-claims

“Harness complete” does not mean copying every ornament that has ever surrounded a message loop. The following boundaries are intentional and are not claimed by the README:

- No account, subscription, billing, identity, entitlement, update, experiment, analytics, or hidden remote-control path.
- No native protocol adapter beyond the three declared formats. Other services are supported only when they deliberately expose one of those open-compatible wire contracts; the harness does not guess from a hostname or silently translate through a hosted relay.
- No copied terminal implementation, text, branding, or assets. The repository does include an original Rust conversational composer and observable state machine; rich interactive question panels, command-directory browsers, remote-team UI, scheduled notifications, voice characters, and marketing surfaces are not claimed.
- No image/PDF/browser rendering layer. `Read` and web integrations return bounded textual data; a graphical browser is not implied.
- MCP advertises no roots, sampling, elicitation, or task client capability. Unsupported server-to-client requests fail closed; `ping` is answered on request-response channels. Tools, resources, templates, prompts, notifications, stdio, and Streamable HTTP are the declared MCP scope.
- LSP is limited to the operations listed by the `LSP` tool schema. It is not a complete editor client.
- Hooks are local command hooks from trusted settings. There is no hidden hosted hook service, and arbitrary project settings cannot install one.
- `WebSearch` is an adapter for a URL the user supplies. The repository ships no search provider, credential, hostname, or fallback service.
- Subagents are local recursive harness runs. Remote agents, peer messaging, team coordination, and cloud sessions are not represented as local capability.

These exclusions keep the implementation open, testable, provider-neutral, and honest. Adding one later requires an explicit public contract, failure boundary, resource budget, privacy test, and warning-free implementation.

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

CI repeats tests and clippy on Linux, macOS, and Windows. The static audit additionally proves that the release binary and runtime source contain no removed brand identifier, private reference files are untracked, `AGENTS.md` stays ignored, the runtime contains no hard-coded public endpoint, warning suppressions and placeholders are absent, and no hidden account or telemetry metadata exists.
