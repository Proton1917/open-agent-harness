# Migration coverage ledger

## Purpose

`MIGRATION_COVERAGE.tsv` is the machine-checked ledger for the complete tool
and command directories in `reference/source-snapshot` and every native module
in the separately checksummed backend archive. It prevents an added, removed,
or renamed source tool/command/native module from disappearing behind a prose
parity claim.

This is one coverage layer, not a declaration that the whole migration is
finished. `--strict` means that this tool/command/native subset has no
`pending` row. Service, hook, frontend, and utility-family closure remains
governed by its own evidence and must be added to this ledger before
project-wide completion can be claimed.

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
  pending rows in every coverage layer, not merely this file.

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

## Current project-wide work still open

- Expand the ledger to every top-level service/utility family with exact Rust
  and test evidence.
- Close or explicitly classify concurrent side-question behavior while a main
  model turn is active.
- Re-run the full required check set and the final repository/remote hygiene
  audit only after those broader layers have zero pending provider-neutral
  behavior.

Run the current subset gate with:

```bash
scripts/audit-migration-coverage.sh --strict
```
