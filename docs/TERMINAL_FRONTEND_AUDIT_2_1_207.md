# Terminal frontend audit against 2.1.207

## Scope and evidence

This audit covers provider-neutral terminal behavior only. It does not claim
source, prompt, wording, asset, account, subscription, telemetry, hosted
service, or brand parity.

The normative version baseline is the checksummed local 2.1.207 archive under
`reference/`.

Evidence below refers to line numbers in
`decompiled/formatted/$bunfs/root/src/entrypoints/cli.js` inside that archive.
Current public documentation is useful for discovering features, but it is not
used to back-port behavior introduced after 2.1.207.

## Observed provider-neutral surface

| Area | 2.1.207 evidence | Required generic behavior |
| --- | --- | --- |
| Keybindings | 215911-216195, 216425-217383 | Contextual hot-reloaded bindings, chords, unbinds, reserved keys, deterministic conflict resolution |
| Composer | 803560-803659, 804071-805293 | Grapheme-safe multiline editing, history edge navigation, reverse search, slash/file completion, argument hints |
| Vim | 652826-654581 | INSERT/NORMAL and visual selection, motions/operators/counts/text objects, grouped undo/dot repeat, history fallback |
| Clipboard media | 215925-215950 | Cross-platform bounded image paste with visible attachments and removal/navigation |
| Slash palette | 804071-805293 | Builtin/custom/Skill/MCP prompt sources, stable conflict handling, filtering, selection, dynamic argument completion |
| Model picker | 651697-651826 | Modal selection which preserves the current draft; picker context and session-only state |
| Transcript | 216025-216057, 820068-820190, 883316-883348 | Toggleable verbose viewer, bounded scroll/search, show-all, external editor/scrollback handoff |
| Fullscreen | 640650-640723, 663837-663992, 702550-702645 | Alternate screen, fixed composer, virtual viewport, sticky bottom, unseen-message pill, wheel/keyboard scroll and selection copy |
| Clear and rewind | 645079-645266, 798470-798553, 814072-814244 | Transactional fresh conversation, previous session remains resumable, double-Escape message selector and scoped restore |
| Configuration | 661446-661717 | Searchable safe UI settings and bounded `key=value` updates |
| Theme | 718168-718294 | Provider-neutral theme tokens, picker preview, cancellation rollback and persistence |
| TUI switch | 718526-718827 | `/tui default|fullscreen`, current-mode query and conversation-preserving transition |
| Status line | 630615-630659, 68694-68729, 739667-739699 | Trusted bounded command, JSON stdin, timeout, normalized output, fixed footer rendering |
| Permissions | 216002-216014, 828448-831097 | Typed preview/options, keyboard navigation and explanation without weakening policy checks |
| Tasks/footer | 714930-715318, 809243-809323 | Separate todo toggle and background-task dialog, live bounded status/footer items |

## Current implementation status

- Implemented and covered by unit or real-PTY tests in the current worktree:
  hot-reloaded contextual keybindings; grapheme-safe Vim Insert/Normal/Visual
  editing; case-insensitive session/project/everywhere history search;
  bounded visible/removable clipboard images; stable builtin/custom/Skill
  command conflicts; draft-preserving Alt-P, Ctrl-T and Ctrl-O surfaces;
  compact/show-all transcript search; transactional `/clear`; double-Escape
  checkpoint selection; `/copy` and workspace-confined `/export`.
- `/tui default|fullscreen` now preserves the conversation while switching to
  an alternate-screen virtual viewport with sticky bottom, unseen counts,
  resize/reflow, page/wheel navigation, mouse single/double/triple/drag
  selection and bounded clipboard copy. The normal composer remains fixed at
  the bottom and is covered by a real PTY lifecycle test.
- Private user-only `/config`, `/theme` and `/statusline` settings are strict,
  atomic and bounded. Theme presets affect composer tokens; trusted status-line
  commands receive public JSON, scrub credentials, time out, cap output and
  reap their process tree. Todo and background-task surfaces remain separate.
- Deliberate remaining boundaries: direct MCP prompt entries and terminal
  rendering of injected dynamic argument candidates, keyboard-only shift
  extension of fullscreen selections, theme preview/custom-theme editing,
  periodic status-line refresh while the composer is fully idle, and exact
  proprietary modal layout/wording. These are not claimed as complete product
  parity.

## Explicit exclusions

- Vendor account, login, billing, entitlement, usage-plan, hosted-session and
  proprietary model-routing behavior.
- Vendor prompt text, brand colors, terminal assets, analytics and telemetry.
- Features documented only after 2.1.207 unless independently useful and
  explicitly declared as a later generic extension.
- Pixel-identical or source-structure reproduction. The target is observable,
  provider-neutral functional compatibility with bounded Rust implementations.
