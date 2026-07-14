# March 31 provider-neutral behavior audit

## Decision

The current Rust tree implements the selected provider-neutral, generally
useful coding-agent behavior observed in the 2026-03-31 comparison snapshot.
The P0 permission gaps and the previously listed common-workflow gaps have been
closed for the declared open surface.

A follow-up static review of the separately checksummed 2.1.207 archive was
used only to identify additional provider-neutral behavior that remains useful
outside any vendor account or hosted service. The resulting additions are
listed below. They extend the open surface; they do not change the March 31
baseline or turn this document into a claim of proprietary-product parity.

This is a behavioral compatibility statement, not a claim of source
reproduction. The project does not claim identical source structure, prompts,
wording, UI assets, account behavior, hosted services, or vendor-specific
features. “March 31 provider-neutral behavior aligned for the declared scope”
is accurate; “complete parity with the proprietary product” is not.

## Confirmed provider-neutral behavior

### Execution, permissions, and transactions

- The multi-round model/tool loop preserves strict tool-call/result ordering,
  bounded concurrency, mutation barriers, and success/error/cancellation
  transaction boundaries.
- Shell permission evaluation decomposes compound commands, nested shell
  execution, substitutions, and common wrappers. A broad allow must cover each
  atomic command, while deny rules fail closed for opaque runtime expansion.
- Filesystem rules compare normalized canonical identities across equivalent
  relative, absolute, slash, and missing-target spellings; deny patterns also
  fail closed across case variants. Read denies filter `Glob` and `Grep`
  discovery results.
- Concurrent sibling writes use fail-closed overlap detection and retryable
  rollback. This preserves a newer sibling write instead of pretending that an
  unsafe nested rollback can always be reconstructed.
- Managed Unix process groups, Windows Job trees, Windows
  drive/UNC/device-path handling, file freshness, full-read-before-overwrite,
  and atomic writes have explicit success and failure paths. A command that
  deliberately starts a new Unix session is outside the process-group cleanup
  guarantee.
- `dontAsk` is a real non-prompting policy: explicit deny and plan restrictions
  still win, explicit allows and intrinsically safe reads may proceed, and an
  operation that would require a question is denied instead of silently
  upgrading authority.
- Interactive permission prompts may retain an exact normalized invocation for
  the current process. The grant is bounded, shared across context forks, never
  persisted or widened to a prefix, and remains subordinate to live project
  deny rules and Plan mode.
- Delegated-agent cancellation and timeout enter the ordinary query transaction
  instead of dropping its future from outside. This preserves no-persistence
  hot-refresh rollback before descendant/background cleanup.
- Trusted command sandboxes may deny network completely or expose only an
  authenticated DNS-pinning proxy for exact/`*.` allowed domains. Configured
  MCP credential environment names are scrubbed from model-reachable and
  extension subprocesses while remaining readable by the parent transport.
- Model API paths cannot add query credentials or fragments, and transport
  failures are reduced to URL-free categories before becoming model-visible.
  `AskUserQuestion` answers enter only through the trusted terminal/control
  interaction boundary; the model-facing schema cannot provide them.

### Workspace, session, planning, and teams

- Trusted `--add-dir` roots have independent scope and file history. Nested
  `AGENTS.md` files load when a permitted tool first touches their subtree;
  persistent shell cwd, worktree state, and instruction context restore on
  resume. Approved path-aware file-tool edits such as `Write`/`Edit` to active
  `AGENTS.md` or project `SKILL.md` files are prepared and committed as one
  context transaction; parse, budget, hook, or turn failure restores both files
  and the previous in-memory context. Arbitrary file mutation hidden inside a
  shell command is not treated as a hot-refresh edit.
- Sessions support continue/resume, forks, resume-at-message boundaries,
  file-only rewind/dry-run, and durable local subagent history. Agents may use
  isolated Git worktrees and resume them across processes when persistence is
  available.
- Plan exit presents the plan and requires an explicit root-user approval; a
  launch-time plan lock and a subagent cannot bypass that decision.
- Persistent local teams provide bounded assignment, mailboxes, completion,
  stop/shutdown, deletion, garbage collection, and per-workspace count/byte
  quotas. A validated private project lock serializes cooperating processes so
  mailbox writes and quota checks cannot race; acquisition has a fixed ceiling,
  and validated crash-stale temporary state is removed in bounded batches.
  Bash, agent, and tracked team completions are delivered automatically into
  the next model round without consuming their explicit output APIs.
- Private cron jobs support session-only and durable schedules plus `/loop`.
  `ScheduleWakeup` is a single transactional dynamic-pacing slot: replacement,
  stop, expiry, turn rollback, and explicit interruption do not mutate fixed
  cron jobs.
- `Monitor` streams bounded 200 ms batches from one permission-checked command
  or pinned ws/wss endpoint, with exact notification rollback and capture
  limits. `RunWorkflow` executes a strict bounded declarative DAG in the
  background; it is not a JavaScript runtime, and command steps reuse Bash
  permission, sandbox, timeout, capture, and process-tree controls.
- Task captures use the private user-state root so cwd/worktree changes cannot
  commit or orphan them; unretained files are RAII-cleaned. Trusted embeddings
  and tests can inject an isolated capture root, and the repository test suite
  does not create task captures in the real user home.

### Open extension surface

- Trusted user/plugin Skills honor bounded declarative metadata for arguments,
  user/model invocability, allowed tools, model, forked context, custom agent,
  and scoped hooks. Automatically discovered project Skills remain untrusted:
  their tool list can only narrow authority and executable model/agent/hook
  metadata is rejected.
- Trusted manifest-only plugins may contribute namespaced skills, commands,
  hooks, custom agents, MCP servers, LSP servers, and output styles. Selection
  remains explicit and project settings cannot install executable/network
  integrations. A separate CLI validates, installs, lists, transactionally
  updates, or uninstalls local directory/ZIP and checksum-pinned HTTPS ZIP
  bundles in a private cache. A durable journal recovers interrupted install,
  update, and uninstall operations; unknown or unsafe residue fails closed.
  Executable state is authenticated and normalized to private `0700`, while
  ordinary files use `0600`. It never mutates the running process.
- Hooks cover tool, permission, prompt, notification, session, stop/failure,
  task/team, subagent, instruction, file, compaction, worktree, and cwd
  boundaries. Root `Stop` feedback is bounded and may request another round;
  `StopFailure` cannot replace the original failure.
- Hooks additionally support one post-batch boundary, user prompt expansion,
  a display-only final message transform, and schema-checked calls to an
  already connected MCP tool. MCP hook input interpolation, time, output,
  concurrency, and async lifetime are all bounded. Root and scoped hooks share
  one async capacity, task registry, observer sequence, and finalization state.
- Stream-JSON uses separate bounded control/now/next/later lanes and supports
  queued-message cancellation, interruption receipts that preserve queued
  work, command lifecycle events, optional accepted-user replay
  acknowledgements, and one opt-in tool-free prompt suggestion request.

### MCP, web, prompt, context, and memory

- MCP supports bounded stdio, Streamable HTTP, legacy HTTP+SSE, and ws/wss;
  sessions,
  pagination, notifications, dynamic tool lists, tools/resources/templates/
  prompts, roots, and form/URL elicitation are mapped through provider-neutral
  interfaces. `WaitForMcpServers` reports pending, connected, failed,
  authentication-required, disabled, and unknown states without busy polling.
  Direct resources, resource links, and bounded scalar templates
  use readable opaque handles plus safe scheme/origin metadata, so URI paths,
  userinfo, query strings, and fragments are not reflected into model context.
  Images and PDFs become model media blocks; audio and other opaque binary data
  expose bounded metadata rather than raw bytes.
- HTTP/WebSocket MCP authentication can use explicitly trusted bearer tokens
  sourced from an environment variable, private file, or bounded command
  without placing the token in a URL. OAuth supports protected-resource and
  authorization-server discovery, PKCE S256/state, opt-in dynamic client
  registration, explicit headless callback handoff, private token/refresh
  persistence, and one bounded refresh retry after 401. Cooperating processes
  serialize token-state transitions through a private lock with a fixed wait
  ceiling; crash-stale atomic files are validated before removal, and consumed
  file handoffs are removed only after a successful exchange.
- `WebFetch` accepts a bounded prompt for local extraction, and `WebSearch`
  supports normalized allow/block domain filters when the configured endpoint
  returns structured links. A blocked nested link removes its enclosing result
  rather than leaving model-visible text without a verifiable link.
- Prompt layers keep the stable harness contract, live capabilities, permission
  state, instructions, selected output style, compaction continuation, and
  delegated-agent guidance distinct. One reactive compaction retry handles a
  provider-declared context or media-size rejection.
- Workspace memory remains disabled by default. Trusted settings may expose
  the explicit `Memory` tool and separately opt into bounded, tool-constrained,
  best-effort extraction after completed root turns. Overlapping schedules are
  coalesced so only the latest pending turn is extracted. Conversation and
  existing memory are treated as untrusted data; likely secrets are rejected
  and accepted entries are committed atomically.

## Honest remaining boundaries

- OAuth never launches, controls, or embeds a browser and never opens an
  implicit callback listener. Authorization URL and callback exchange are
  explicit private file/environment handoffs; this is suitable for headless
  operation but is not a graphical account-login flow.
- Stream-JSON does not permit live in-process plugin/MCP installation or
  configuration mutation, arbitrary transcript injection/replay, or vendor
  callback families. Plugin lifecycle is a separate CLI operation for a later
  process, and accepted-user replay is a bounded delivery acknowledgement.
- The interactive composer does not accept a concurrent one-off side query
  while another model turn is active. Opt-in prompt suggestions run only after
  a completed print-mode turn and are never auto-executed.
- `RunWorkflow` intentionally accepts a strict declarative command DAG, not
  arbitrary JavaScript, downloaded workflow code, or cross-process resume.
- `ConfigChange` is intentionally not emitted because this runtime rejects
  in-process environment/config mutation; there is no accepted configuration
  mutation boundary to observe.
- Explicit `TaskStop` is intentionally immediate; a stopped OS process cannot
  be recreated by later turn rollback. New-task rollback, notification cursors,
  and unretained capture cleanup remain transactional.
- Automatic team mailbox delivery tracks teams opened or created in the current
  process. A persisted team must be opened before it joins automatic delivery;
  explicit mailbox reads remain available and are not consumed by delivery.
- A completion hook can reject the recorded result of a one-shot team member,
  but cannot resume a process that has already exited; the assignment is marked
  failed rather than left in a ghost-running state.
- Cooperating harness processes serialize auto-memory initialization and updates
  through a private `.MEMORY.lock`, in addition to private atomic replacement.
  A crash-stale lock fails closed and requires explicit removal after verifying
  that no writer remains; unrelated external writers do not participate in this
  protocol.
- The file-history transaction journal coordinates one harness process.
  Independent OS processes editing the same workspace do not share rollback
  ordering; ordinary freshness checks and atomic writes still apply.
- MCP elicitation waits synchronously on that server's RPC reader. Other
  runtime workers continue, but the same MCP connection does not process
  another server request until the user response or cancellation. Headless and
  direct local TTY interaction share a configured ceiling of at most 120
  seconds; timeout leaves no detached stdin reader.
- The project does not include accounts, subscriptions, identity, entitlement,
  telemetry, branded assets, hosted teams/sessions, or copied proprietary code,
  prompts, terminal implementation, or UI.

## Release language

Documentation and releases may say that the declared Rust harness is aligned
with the 2026-03-31 snapshot's provider-neutral general behavior and includes
the selected open generic extensions listed from the later archive review.
They must not say that it reproduces either reference source structure, is
byte-for-byte identical, or implements every proprietary or vendor-specific
feature.
