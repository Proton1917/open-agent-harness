# Migration coverage ledger

## Purpose

The migration audit has four machine-checked source ledgers plus one shared
category table:

- `MIGRATION_COVERAGE.tsv` covers every top-level tool, command, service, and
  separately checksummed native backend module.
- `MIGRATION_FAMILY_INVENTORY.tsv` covers all 551 top-level utility, hook, and
  component families; descendants inherit their exact parent-family category.
- `MIGRATION_SURFACE_INVENTORY.tsv` names all 324 source files outside those
  six large families, including the four `native-ts` compatibility files.
- `MIGRATION_PROTOCOL_INVENTORY.tsv` covers every control subtype, initialize
  field, custom-agent option, stdin union member, and stdout union member
  extracted from the source SDK schemas and active print dispatcher.
- `MIGRATION_FAMILY_CATEGORIES.tsv` binds every family category to implemented,
  equivalent, excluded, or pending status and concrete Rust evidence.

The strict script additionally partitions all 1,902 JavaScript/TypeScript
source files across the three source-file ledgers and exact-compares the five
protocol namespaces against the fourth. An added, removed, renamed, or newly
placed source file or protocol member therefore cannot disappear behind a
prose parity claim.

## Authorities

- General behavior and interactive product logic: the read-only
  `reference/source-snapshot` tree at commit
  `4b9d30f7953273e567a18eb819f4eddd45fcc877`.
- Rust terminal mechanics only: read-only `reference/codex` and
  `reference/grok-build` implementation patterns.
- Selected provider-neutral backend evidence only: the checksummed 2.1.207
  static-analysis archive. It is not a frontend authority.

## Status meanings

- `implemented`: the same open capability has a direct Rust implementation.
- `equivalent`: the behavior exists behind an intentionally different open
  shape, with the difference stated in the row.
- `excluded`: the source capability requires vendor identity, hosted services,
  desktop/voice authority, copied assets, or a non-Rust arbitrary runtime that
  is outside this repository's declared boundary.
- `pending`: provider-neutral behavior still requiring implementation or a
  defensible classification. Project-wide migration closure requires zero
  pending rows in every coverage layer, not merely one file.

## Current closed batches

The raster-media row is backed by `src/image_processing.rs`. Real decode and
normalization now covers `Read`/explicit file mentions, clipboard images,
exact shell data URIs, MCP image blocks/resources, and direct SDK/stream user
blocks. The shared path verifies MIME signatures, enforces bounded decoding,
never enlarges smaller images, constrains dimensions to 2000x2000, targets
3.75 MiB raw output, and fails closed for corrupt or still-oversized media.

The hook/environment batch is backed by `src/hooks.rs`, `src/tools/mod.rs`,
`src/query.rs`, and `src/main.rs`. Static `FileChanged` matchers and absolute
hook-returned `watchPaths` now feed a bounded request-boundary watcher with
`ignoreInitial` rebasing, add/change/unlink detection, no symlink traversal,
resource caps, and exact fingerprint acknowledgement for harness-owned writes.
External instruction and Skill changes are refreshed before the next model
request. This closes the source watch-list behavior without claiming a
resident background OS watcher or lossless reporting of every intermediate
write between request boundaries.

The first frontend-service batch is backed by `src/sleep_inhibitor.rs`,
`src/terminal_notifications.rs`, the interaction wait lifecycle, typed private
UI settings, and a real PTY regression. macOS work-only sleep prevention is
self-expiring and pauses for user dialogs. Idle notifications are replaceable,
cancel on activity, run trusted Notification hooks before delivery, sanitize
OSC content, and support explicit multiplexer passthrough. These close the
`preventSleep.ts` and generic `notifier.ts` behaviors without importing account,
analytics, or desktop-notification services.

The prompt-suggestion frontend service is backed by `src/query.rs`, the
single-slot generation state in `src/main.rs`, typed user UI settings, composer
rendering in `src/terminal.rs`, and a delayed-response real-PTY regression.
Interactive suggestions are explicitly enabled, tool-free, transcript-free,
bounded to a short single line, replaceable, and generation-checked after
cancellation. Enter sends the ghost prompt; Tab or Right accepts it for
editing. Print-mode stream JSON remains an explicit protocol option.

All 36 top-level source services are now exact-name inventory rows. Generic
runtime services map to Rust implementations or deliberately different open
equivalents; account, subscription, telemetry, hosted synchronization,
internal test fixtures, and voice authority are explicitly excluded. The two
remaining generic service gaps were closed without importing vendor identity:
background-agent progress is driven by exact bounded child query/tool events
and is visible/actionable in the unified task UI, while explicit
`memory.autoConsolidate=true` runs a five-unique-session, 24-hour-gated,
tool-constrained consolidation pass. Consolidation uses a private bounded
sidecar, rejects stale concurrent memory, validates all update/delete
operations, preserves newly observed sessions, and commits through the memory
lock and atomic writer.

The active side-question frontend path is backed by `src/query.rs`,
`src/terminal.rs`, `src/control.rs`, and `src/main.rs`. The active user message
is added to an immutable side context before the main future borrows the
engine. TTY `/btw` then runs concurrently through a single bounded tool-free
request while up to eight ordinary inputs queue for later turns. Coordinated
inline/fullscreen composer ownership, modal suspension, and nested raw-mode
guards prevent permission prompts from racing terminal input. Stream JSON uses
its own four-slot immediate lane and returns a correlated `side_question`
control response without waiting for the main turn. Real PTY and local mock
server tests verify response ordering, empty tool registration, queue progress,
and absence from the primary transcript.

The MCP-server and terminal-panel batch closes two source entrypoint/frontend
gaps without importing provider identity. `mcp serve` constructs no model
client or transcript; it exposes the local Rust tool registry through a
bounded newline JSON-RPC state machine, requires initialization, validates
schemas, remains non-interactive, and denies mutation unless trusted rules or
an explicit bypass authorize it. The client-side MCP runtime also keeps
configured, plugin, and stream-control layers separate, supports delayed SDK
transport connection and bounded message exchange, refreshes discovered tools
after topology changes, and fails closed on cross-layer name collisions.
Runtime flag settings are in-memory, bounded, validated before application,
credential-redacted on inspection, and extend a shared monotonic child-process
secret scrubber. The terminal panel is disabled by default,
uses an explicit user-only setting and Alt-J binding, suspends terminal
ownership around the child, scrubs configured credentials, keeps a private
process-instance tmux server when available, and falls back to a direct login
shell. Exact ownership prevents adopting or killing a pre-existing server.

The remaining-source ledger records source semantics rather than filenames
alone. `LocalMainSessionTask` is inactive because its mounted UI gate compiles
to constant false; the external `useMoreRight` hook is an explicit no-op.
Team-memory paths depend on hosted account synchronization and a remote feature
gate, while the open harness separately implements private local workspace
memory. Release migrations, hosted bridges/sessions, telemetry-generated
types, copied bundled prompts, voice, and decorative assets remain explicit
excluded rows rather than silent omissions.

The protocol-semantic ledger is generated from the active source dispatcher
and SDK schemas, then exact-compared by name: 33 control subtypes, 8 initialize
fields, 14 custom-agent options, 5 stdin members, and 31 stdout members. Open
controls now include transactional plugin/LSP/MCP replacement, SDK hooks and
MCP transport, selected main-agent initialization, bounded deterministic title
generation, and acknowledged session termination. Partial output is emitted
only when requested and uses a schema-compatible `stream_event` envelope;
accepted-user replay, compaction, hook/tool progress, and agent task events
carry their required identities and lifecycle fields. Deliberately different
rows explicitly identify provider billing/account metadata, hosted keep-alive
and remote bridges, private reasoning/provider state, and exact
provider-specific thinking-token budgets instead of filling them with invented
values.

## Closure gate

`--strict` now requires zero pending rows in the tool/command/service/native,
family, remaining-source, and protocol-semantic layers. Passing it establishes
exhaustive source classification for the declared provider-neutral scope;
release closure still also requires the repository's full format, check, test,
clippy, release-build, cross-target, harness-audit, and Git/reference-hygiene
checks.

Run the current subset gate with:

```bash
scripts/audit-migration-coverage.sh --strict
```
