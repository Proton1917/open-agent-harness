# Terminal frontend audit against the source-available 2026-03-31 snapshot

## Authority and scope

The only design authority for the interactive CLI frontend is
`reference/source-snapshot`, the source-available 2026-03-31 snapshot. The
separate 2.1.207 archive is not evidence for terminal layout, rendering,
editing, commands, key behavior, dialogs, or other frontend interaction.

Rust and platform-level terminal mechanics are additionally cross-checked
against `reference/codex` and `reference/grok-build`, specifically for event
invalidation, alternate-screen ownership, modal suspension, panic restoration,
job-control recovery, and keyboard/input cleanup. They are implementation
pattern references, not additional authorities for product behavior or UI
parity. All three reference trees remain read-only.

This project independently implements the generally useful, provider-neutral
behavior in Rust. It does not copy source structure, React/Ink components,
prompts, wording, assets, brand identity, account flows, telemetry, hosted
services, or vendor-only commands. Functional equivalence for the declared
open surface is the goal; pixel identity and full proprietary-product parity
are not.

## Source surfaces reviewed

The frontend comparison is grounded principally in these source files:

- `src/screens/REPL.tsx` for the conversation lifecycle, input ownership,
  loading state, dialogs, and footer composition.
- `src/components/PromptInput/PromptInput.tsx`, `inputPaste.ts`, and the input
  buffer/history hooks for editing, history, paste references, mode changes,
  attachments, suggestions, and model selection.
- `src/utils/suggestions/commandSuggestions.ts` and `src/commands.ts` for slash
  discovery, aliases, ordering, dynamic commands, and command availability.
- `src/commands/{clear,resume,rewind,rename,branch,tag,copy,export,model,mcp,
  config,theme,statusline,permissions,tasks,effort,output-style}` for generic
  command behavior.
- `src/components/StatusLine.tsx` and the status-line setup implementation for
  refresh behavior and the public JSON input schema.

## Implemented provider-neutral behavior

### Composer and discovery

- Grapheme-safe multiline editing, visual-line movement, undo, kill/yank,
  private scoped history, reverse search, stash/restore, external-editor
  handoff, Vim modes, bounded viewport rendering, resize recovery, and the
  source-aligned 800 ms double-interrupt exit window.
- `/` immediately opens the bounded live command catalog. Built-ins, trusted
  custom commands, user-invocable Skills, workflows, plugin commands, and
  connected MCP prompts use deterministic conflicts and aliases. Selection,
  filtering, completion, argument hints, and dynamic candidates are covered by
  unit and real-PTY tests.
- Slash names embedded later in ordinary prompt text receive non-executing
  Tab/Right completion. `@` file references support quoted paths, fuzzy
  matching, ignore rules, middle-of-token replacement, and bounded ranges.
- Text paste follows the source behavior: ANSI is removed, tabs become four
  spaces, line endings are normalized, and content over 800 characters or the
  small visible-line allowance becomes a `[Pasted text #N ...]` reference.
  The full text is restored only for submission; the terminal keeps the
  collapsed display. Clipboard images remain bounded attachments.

### Conversation and session surfaces

- `/clear` archives the old conversation and creates a new resumable session.
  `/resume` searches compatible sessions across registered worktrees and shows
  bounded title/tag/color/worktree previews before switching in process.
- `/rename`, `/tag`, and `/branch` use strict private metadata. `/tag` toggles
  the same tag off, matching the source command semantics.
- `/rewind` selects a real message boundary and supports code plus conversation,
  code only, conversation only, and summarize-from-here. The original session
  is preserved as the parent; a failed multi-surface fork is removed only after
  exact ownership checks.
- `/btw` runs a tool-free contextual side question without blocking or mutating
  the main transcript. During an active model turn, a bounded raw-mode composer
  keeps `/btw` immediate and queues at most eight ordinary follow-up inputs;
  its side snapshot includes the active user message. The same operation uses
  a dedicated bounded stream-JSON control lane, so it does not wait behind the
  main turn. `/copy` and `/export` expose bounded public answer text without
  private reasoning or silent overwrite.
- Ctrl-T and `/tasks` include background agents as first-class actionable rows.
  Their bounded detail is driven by exact child model/tool/retry/compaction
  events, updates on the existing 250 ms UI snapshot cadence, and uses the same
  `TaskOutput`/`TaskStop` boundary as other background work.
- Explicitly enabled prompt suggestions use one replaceable, tool-free
  background request after successful turns. A short single-line result is an
  empty-composer placeholder: Enter sends it, while Tab or Right accepts it for
  editing. Key or paste activity aborts the request and advances a generation
  guard; a delayed stale completion cannot reappear. The request is absent from
  the transcript, and the same option retains the stream-JSON push surface.

### Rendering, progress, and dialogs

- Streaming Markdown, code, tables, links, tool previews, parallel-tool
  activity, elapsed/stall state, retry attempts, usage, reasoning lifecycle,
  and multiline failures are rendered through bounded control-safe paths.
- Normal scrollback and the optional fullscreen transcript both preserve a
  fixed composer, resize/reflow, search, scrolling, selection, clipboard copy,
  and canonical trusted actions without leaking alternate-screen state.
- Model, theme, permission, settings, task, MCP, resume, and rewind pickers are
  terminal-sized and preserve the draft. `/effort`, `/output-style`, plugin
  reload, and the status-line public schema are provider-neutral.
- The trusted status line refreshes asynchronously and receives source-shaped
  model/workspace/context fields with local absolute paths intentionally
  reduced to public relative or opaque identities.

### Terminal lifecycle correction (2026-07-16)

- Startup now seeds the bounded transcript and full session header before the
  fullscreen owner enters the alternate screen. Empty-transcript text is a
  visual-only placeholder and cannot become a stale transcript row.
- Fullscreen rows use absolute cursor addressing instead of tty-dependent
  newlines, and byte-identical frames are suppressed until explicit
  invalidation or state change.
- A single fullscreen owner is suspended around a serialized modal lease, then
  repainted after model, transcript, permission, settings, task, and related
  dialogs release their own surface. Partial setup, panic, job-control resume,
  and external-editor return paths restore terminal and input modes.
- Real-PTY fixtures mark the master descriptor close-on-exec, drain terminal
  output while awaiting a clean exit, and assert that controlling-PTY hangup
  cannot leave detached harness children behind.
- A real-PTY prompt-suggestion fixture delays one response until after typing,
  proves the stale generation stays hidden, then proves re-arming, Enter
  acceptance, tool-free registration, and delivery into the next main request.
- Completed interactive turns arm one bounded idle-notification timer. Any
  terminal event or next scheduled/submitted prompt cancels it; a real-PTY
  regression proves cancellation and re-arming. Delivery supports sanitized
  iTerm2, Kitty, Ghostty, raw BEL, and tmux/screen passthrough, with the trusted
  `Notification` hook ordered before the terminal sequence.
- macOS active-turn work uses a self-expiring `caffeinate` child. Reference
  counting, restart, abnormal-child recovery, and cleanup are deterministic;
  permission, question, plan-approval, and elicitation wait guards suspend the
  assertion until actual work resumes. No process is started on other targets.

## Deliberate boundaries

- Account, login, billing, subscription, entitlement, cloud session, remote
  control, telemetry, feedback, upgrade, branded model routing, and other
  vendor services are excluded.
- Prompt packages such as vendor review/advisor commands are not frontend
  primitives; equivalent user Skills or custom commands can provide them.
- Custom proprietary themes, brand colors, exact modal wording/layout, and
  frequency data derived from a vendor's private installation are not copied.
- The 2.1.207 archive may inform a separate generic backend capability audit,
  but it cannot expand or redefine the frontend claim in this document.
