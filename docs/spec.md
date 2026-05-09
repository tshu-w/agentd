# agentd implementation specification

Compatibility: ignore backward compatibility; implement the target architecture directly.

This document is a self-contained implementation specification for agentd, designed to rebuild the entire system from scratch without reading the old codebase (prefer industry-standard patterns and paradigms).

### Global Conventions

**ID format**: type-prefixed random IDs (Stripe-style). Prefix distinguishes entity type; random part is 12 characters (`uuid4().hex[:12]`).

| Entity | Prefix | Example |
|---|---|---|
| Actor | `act_` | `act_7a3f1b9e2d4c` |
| Turn | `turn_` | `turn_e5b8a1c3f290` |
| Message | `msg_` | `msg_d4e7f2a1b839` |
| Trigger | `trig_` | `trig_c1a9b3e5f472` |

Actor parameter resolution: starts with known prefix (`act_`) → match by id; otherwise → look up by name among non-terminal actors in root scope. Child actors must be referenced by `actor_id`.

**Wire format**: agentd's own protocol (RPC, responses, event envelopes) uses snake_case throughout.

---
## 1. Background

agentd (agent daemon) provides a CLI for agents (primary) and humans (secondary) to manage long-lived agents.

Mental model: **supervisord for agent processes + structured mailbox**. Borrows actor model concepts (mailbox, state machine, supervision tree), tailored for CLI agent processes — covering both one-shot (`pi -p`, `claude`) and long-running (Pi RPC) modes. Does not intervene in agent internals; only manages lifecycle, input delivery, and session continuity (checkpoint).

### Core Positioning

- **Agent-agnostic**: CLI primarily serves self-orchestration across different agents; stable, composable, drivable by another agent
- **Event-driven**: actors wake on messages; mailbox is the unified input surface, accepting typed messages (`env.*`)
- **Thin framework**: keep the framework thin, let agents do the real work (e.g., replying to Telegram via SKILL, agents self-registering webhooks)

### v1 Constraints

Single-user, single-machine, local daemon. Unix socket RPC, SQLite persistence, PID-based process tracking. Backend mode covers one-shot only (one process per turn); long-running backend (process survives across turns) is reserved as an extension point, not implemented in v1.

### Typical Scenarios

- Send a message to an agent on your computer via IM and receive a reply (OpenClaw-style)
- Agent self-orchestration: one agent spawns/emits to other agents, forming collaboration

### Non-Goals

- **No process-level API**: the external abstraction is actor/turn, not processes. Turn completion is defined by `turn.end` events, not by process exit. No interfaces for querying PIDs or waiting on process exit.
- **No built-in platform-specific webhook logic**: HTTP inbox accepts generic typed messages only. GitHub signature verification, Telegram payload parsing, etc. are handled by the agent itself.
- **v1: no process health probing**: no active detection of hung backend processes. Relies on process exit or reconciliation on daemon restart as fallback.

### Future Directions

Explorable within the monorepo: agent memory, multi-agent chat rooms, long-lived conversation actors, higher-level collaboration patterns. Channels are an auxiliary integration layer, not central to the core model.

---
## 2. Core Model

### Actor

Long-lived container for an agent. One actor can process multiple turns.

| Category | Properties |
|---|---|
| Identity | actor_id, name, scope_id, parent_actor_id |
| Configuration | backend, backend_args, cwd, env |
| Runtime | state, checkpoint, mailbox |

### Turn

One logical processing cycle of an actor. Belongs to exactly one actor. Each actor has at most one active turn at a time.

Turns are not top-level command objects; they are exposed through the actor's status/logs/results.

### Mailbox

Actor's input queue, holding typed messages: `{"type": "message", "payload": {"text": "hello"}}`

All external input enters the system as messages. CLI `--message` is syntactic sugar for `type=message`.

In this document, **message** refers to input sent to an actor, **event** refers to system observation records (§4 Event Model).

### Backend

Adaptation layer that maps agentd turns to concrete agent execution.

Process modes:
- **One-shot**: one process per turn; process exit means turn ends. E.g., `pi -p "<prompt>"`, `claude -p "<prompt>"`, `codex exec "<prompt>"`.
- **Long-running**: process survives across turns, receives subsequent input via in-process protocol. E.g., Pi RPC.

Each backend declares its capability flags (e.g., `supports_steer`), which agentd uses to determine delivery strategy.

### Checkpoint

Session continuity across turns. Belongs to the actor, not the turn. Detailed semantics in §9.

### Entity Relationships

```
Actor 1──N Turn
Actor 1──N Message (mailbox)
Actor 1──1 Backend (binding)
Actor 1──0..1 Checkpoint
Actor 0..1──N Actor (parent/child)
```

### State Machine

#### Actor states

| State | Meaning |
|---|---|
| `idle` | No active turn; can receive input |
| `active` | Has one active turn (`pending` or `running`) |
| `closed` | Terminal state |

Transitions: `idle -> active`, `idle -> closed`, `active -> idle`, `active -> closed`

Key clarification: `active` = has an active turn, not "has a live process (e.g., Pi RPC)".

#### Turn states

| State | Meaning |
|---|---|
| `pending` | Turn created, backend not yet started |
| `running` | Backend has started processing |
| `ended` | Turn has ended |

Transitions: `pending -> running`, `pending -> ended`, `running -> ended`

#### Turn outcome

`succeeded` / `failed` / `canceled` / `interrupted`

### Scope and Tree Model

#### Name

`name` is optional. If not provided, it is `null` and the actor can only be referenced by `actor_id`.

#### Scope Rules

- Root actor: `scope_id = "__root__"`
- Child actor: `scope_id = parent_actor_id`
- Actor name uniqueness is enforced within `scope_id` (no duplicate names under the same parent; actors with `null` names are excluded from the check)

#### Parent/child

- Parent/child relationship is persisted in the `parent_actor_id` field
- Established via `parent_actor_id` parameter at spawn time
- Depth limit: `max_depth` (config, default 3)
- Per-parent child count limit: `max_children_per_parent` (config, default 8)

#### Close Subtree

Closing a parent recursively closes the entire subtree (all descendants, not just direct children). For each closed actor:
- If it has an active turn → outcome = `canceled`
- Actor → `closed`
- Its triggers are deleted

### Key Invariants

- Each actor has at most one active turn at a time
- Non-terminal actor names are unique under the same parent (`null` names excluded from check)
- `closed` is terminal, irreversible
- Checkpoint lifecycle outlasts turns

---
## 3. Operational Semantics

### Public Operations

#### Spawn

Create a new actor.

- No initial input → `idle` actor, no turn created
- With initial input → create actor + first turn
- Non-terminal actor names cannot be duplicated under the same parent (`null` names excluded from check)

#### Emit

Deliver a typed message to an actor.

Delivery mode (`deliver_as`, default `auto`):
- `auto`: automatically chosen based on actor state, turn state, and backend capability:
  - actor `idle` → `follow_up`
  - actor `active` + turn `running` + `supports_steer` → `steer`
  - actor `active` + turn `pending` → `follow_up`
  - actor `active` + does not support steer → `follow_up`
- `follow_up`: next-turn input; does not affect current turn
- `steer`: current-turn control input; only valid when backend supports it; errors directly if unsupported; no silent fallback

Rules:
- Emit to `closed` actor → error `actor_closed`
- Emit to `idle` actor → wake up (open new turn)
- Emit to `active` actor → scope determined by `deliver_as`
- `deliver_as=steer` prohibits carrying `env`

#### Stop / Close

Two public control actions, both actor commands:

| Action | Current turn outcome | Actor target state | Subtree |
|---|---|---|---|
| soft stop | `interrupted` | `idle` | unaffected |
| hard close | `canceled` | `closed` | all closed |

Execution strategy may vary by backend, but lifecycle semantics are fixed.

#### Wait

Wait for actor to return to `idle` or `closed`. Actor-centric; does not elevate turns to top-level command objects.

### Internal Mechanisms

#### Turn Formation

Handled by the scheduler. v1: one message opens one turn. Queued messages for the same actor are claimed in `(created_at, message_id)` FIFO order.

1. Actor `idle` + queued message exists
2. Scheduler claims the oldest queued message (mailbox state: `queued → claimed`) → creates `pending` turn → emits `turn.opened` (with input snapshot)
3. Actor `idle → active`
4. Runtime executes

Steps 2–3 must be atomic (same DB transaction) to prevent "message claimed but turn not created" inconsistency.

#### Completion and Terminal Intent

`turn.end` is the sole turn completion signal. Execution termination (e.g., process exit) is not the completion definition, only a cleanup/fallback signal.

Runtime tracks two types of internal state:
- **Terminal intent**: `none` | `stop` | `cancel`
- **In-turn controls**: e.g., `steer`

Outcome attribution:
- `turn.end` + intent `none` → `succeeded` or `failed` (determined by backend)
- `turn.end` + intent `stop` → `interrupted`
- `turn.end` + intent `cancel` → `canceled`
- No `turn.end` + execution termination → fallback attribution based on terminal intent

#### Turn-End Processing

After a turn ends, process in the following order:

1. Turn → `ended`, write outcome / result / error
2. Ack mailbox input (mailbox state: `claimed → acked`)
3. Actor → `idle` (or `closed` if needed)
4. If queued messages exist and actor is still open → trigger next turn

---
## 4. Event Model

### Definitions

- **Public event**: an event that crosses the daemon boundary into the persisted event log (`events` table) and the EventBus, becoming visible to external clients (`agentd logs`, `agentd wait --progress`, channels). Anything before this boundary (raw backend records, runtime internal state, debug logs) is not a public event.
- **Canonical schema**: the payload structure defined by this section (§4) for each event type. Each `turn.progress` subtype, the `turn.end` payload, etc. are canonical schemas. Public events MUST use canonical schemas only.
- **Raw backend event**: the JSON object emitted by a backend CLI on its stdout, in that backend's own private format (e.g., pi's `message_update.assistantMessageEvent`, codex's `item.completed`, claude's `assistant.content`). Raw backend events are adapter normalization inputs.

### Event Types

| Event | Payload highlights | Meaning |
|---|---|---|
| `actor.spawned` | actor_id, name(nullable), backend | Actor created |
| `turn.opened` | turn_id, input snapshot | Scheduler opened turn; input snapshot is the source of truth for opening input |
| `turn.started` | turn_id, exec_pid | Runtime started processing |
| `turn.progress` | See below | Live progress (text, thinking, tool call) |
| `turn.end` | outcome, result, error | Sole turn completion signal |
| `actor.closed` | reason | Actor entered terminal state |
| `actor.checkpoint.loaded` | | Checkpoint loaded |
| `actor.checkpoint.saved` | | Checkpoint saved |
| `actor.checkpoint.missed` | | No checkpoint to load |

Optional (may not be implemented in v1):
- `turn.control.accepted`: emitted when steer delivery is accepted by backend
- `turn.execution.terminated`: emitted on abnormal execution termination (runtime internal, not exposed to client)
- `trigger.fired`: emitted when a cron trigger fires

#### `turn.progress` Subtypes

Each backend adapter is responsible for mapping raw output to normalized, bounded progress events. Progress is for live observation (`logs --follow`, `ps --watch`, `wait --progress`), not for replaying backend raw protocol or carrying complete turn artifacts. Payload `type` MUST be one of:

| type | Meaning | Payload |
|---|---|---|
| `text` | Agent output text | `{"type": "text", "content": "..."}` |
| `thinking` | Agent thinking process | `{"type": "thinking", "content": "..."}` |
| `tool_call` | Agent tool invocation | `{"type": "tool_call", "name": "bash", "args": {...}, "status": "running\|completed\|failed"}` |

Per-backend mapping:

| Subtype | Pi (`--mode json`) | Claude (`stream-json`) | Codex (`--json`) |
|---|---|---|---|
| `text` | `text_delta` forwarded per-line | `assistant.content[type=text]` | `item.completed[type=agent_message]` |
| `thinking` | `thinking` event | `assistant.content[type=thinking]` | N/A |
| `tool_call` | `toolcall_start/end` | `assistant.content[type=tool_use]` | `item.completed[type=tool_call]` |

### Logging Rules

- Output normalized actor/turn events in order
- Turn boundary events (`turn.opened`, `turn.end`) must be visible to clients
- Clients should not need to understand backend raw protocols to follow the lifecycle
- Raw backend event objects MUST NOT be forwarded as public event payloads

---
## 5. External Interfaces

### 5.1 RPC Protocol

Transport: Unix socket, NDJSON. Envelope format aligns with JSON-RPC 2.0; streaming is a custom extension.

```
Request:  {"jsonrpc": "2.0", "id": "req-1", "method": "actor.spawn", "params": {...}}
Success:  {"jsonrpc": "2.0", "id": "req-1", "result": {...}}
Error:    {"jsonrpc": "2.0", "id": "req-1", "error": {"code": -32600, "message": "...", "data": {...}}}
Stream:   {"jsonrpc": "2.0", "id": "req-2", "event": {...}, "done": false}
          {"jsonrpc": "2.0", "id": "req-2", "result": {...}, "done": true}
```

Error codes: protocol-level errors use JSON-RPC 2.0 standard codes (-32700 parse error, -32600 invalid request, -32601 method not found, -32602 invalid params, -32603 internal error). Business errors use -32000 uniformly, with specific type in `error.data.type`: `not_found` / `actor_closed` / `conflict` / `forbidden` / `backend_error` / `daemon_unavailable` / `timeout` / `slow_consumer`

Delivery semantics: RPC success return = relevant DB transaction committed. Client timeout = outcome unknown; client should confirm via query.

`actor` parameter resolution: see Global Conventions.

### 5.2 Actor Methods

#### `actor.spawn`

Parameters: `name`(optional), `backend`, `parent_actor_id`, `backend_args`, `env`, `cwd`, `checkpoint`, message input (`message` or `type`+`payload`)

`cwd` resolution priority: explicit parameter > config directory > daemon working directory. In interactive (TTY) use, CLI omitting `--cwd` auto-fills caller's `$PWD`; in non-TTY use (e.g., channel subprocess) it is left empty so the daemon's config-directory fallback applies.

Response:
```json
// No input
{"actor_id": "act_7a3f1b9e2d4c", "state": "idle", "current_turn": null, "event_seq": 1}
// With input
{"actor_id": "act_7a3f1b9e2d4c", "state": "active", "current_turn": {"turn_id": "turn_e5b8a1c3f290", "state": "pending"}, "event_seq": 2}
```

#### `actor.emit`

Parameters: `actor`(required), message input (`message` or `type`+`payload`), `env`, `deliver_as`(`auto|steer|follow_up`)

Response: `{"actor_id": "act_7a3f1b9e2d4c", "delivery_mode": "follow_up", "woke": true, "event_seq": 42}`

#### `actor.wait`

Parameters: `actor`(required), `timeout`, `progress`, `since_seq`

Streaming response (when `progress=true`): replays up to 20 recent historical events (limited window), then pushes live events line by line, ending with `done: true` + actor state. Non-streaming blocks until actor returns to `idle` or `closed`. `wait` is a completion interface, not a full audit log; use `actor.logs --follow` for complete history catch-up.

```json
// Progress streaming events
{"jsonrpc": "2.0", "id": "req-3", "event": {"event_type": "turn.progress", ...}, "done": false}
// Final result
{"jsonrpc": "2.0", "id": "req-3", "result": {"actor": {...}, "result": "..."}, "done": true}
```

Timeout returns error (`data.type=timeout`), does not change actor state.

#### `actor.stop`

Parameters: `actor`(required)

Response: `{"actor_id": "act_7a3f1b9e2d4c", "state": "idle", "changed_count": 1}`

Edge cases: actor `idle` → idempotent return of current state (`changed_count: 0`); actor `closed` → error `actor_closed`.

#### `actor.close`

Parameters: `actor`(required)

Response: `{"actor_id": "act_7a3f1b9e2d4c", "state": "closed", "changed_count": 3}`

Closes actor and its entire subtree. Edge cases: actor `idle` → close directly; actor `closed` → idempotent return (`changed_count: 0`).

#### `actor.list`

Parameters: `include_terminal`, `watch`, `limit`. Watch mode streams snapshots.

#### `actor.logs`

Parameters: `actor`, `since_seq`, `follow`, `limit`.

The authoritative event stream interface. Non-follow mode returns a historical event snapshot; follow mode replays history (controlled by `limit`), then continuously pushes live events. This is the primary entry point for "full history + future".

#### Slow Consumer

All streaming interfaces (`logs --follow`, `wait --progress`, `ps --watch`) use bounded per-subscriber queues. When a client cannot keep up with event production, the server returns a `slow_consumer` error with `resume_seq`:

```json
{"jsonrpc": "2.0", "id": "req-1", "error": {"code": -32000, "message": "slow consumer", "data": {"type": "slow_consumer", "resume_seq": 142}}}
```

For event streams (`logs --follow`, `wait --progress`), clients should use `resume_seq` as `since_seq` for reconnection. Note that `resume_seq` points to the current global tail; events between the last consumed seq and `resume_seq` may be skipped. For snapshot streams (`ps --watch`), clients simply re-issue the watch request to receive a fresh snapshot.

#### `actor.status`

Parameters: `actor`, `include_events`, `include_result`, `since_seq`, `limit`

Response:
```json
{
  "actor": {"actor_id": "...", "name": "..." | null, "state": "active", ...},
  "current_turn": {"turn_id": "...", "state": "running", ...},
  "last_turn": {"turn_id": "...", "state": "ended", "outcome": "succeeded", "result": "...", "error": null},
  "events": [...],
  "next_seq": 100
}
```

### 5.3 Daemon Methods

- `daemon.status`: daemon health / configuration snapshot
- `daemon.doctor`: health check + optional auto-repair

### 5.4 Trigger Methods

- `trigger.add`: parameters `actor`, `schedule`, `type`, `payload`
- `trigger.ls`: optionally filter by actor
- `trigger.rm`: delete by trigger_id

Cron format: standard 5-field (minute hour day month weekday), no seconds or year fields. Timezone semantics: schedule is interpreted in daemon process local timezone (consistent with system cron behavior); internally stored `next_fire_at` is UTC.

### 5.5 HTTP Inbox Bridge

Optional HTTP ingress: `POST /v1/actors/{actor_id}/inbox`

Request body: `{"type": "env.webhook.github.push", "payload": {...}}`

Provider-agnostic, internally forwards to `actor.emit`. Not responsible for provider-specific semantics. Supports optional `Idempotency-Key` request header for best-effort in-memory webhook retry deduplication.

### 5.6 CLI

CLI is actor-first; turns only appear as attached information.

| Command | Description |
|---|---|
| `spawn` | Create actor (no input → idle, with input → active + turn) |
| `emit` | Send typed message to actor |
| `wait` | Wait for actor to return to idle/closed; `--timeout`, `--progress` |
| `stop` | Soft stop (default) or `--close` hard close; mapped to `actor.stop` / `actor.close` RPC respectively |
| `ps` | List actors |
| `logs` | View/follow actor logs |
| `status` | Daemon or actor status snapshot (actor output includes current/last turn) |
| `trigger add\|ls\|rm` | Manage triggers |
| `serve` | Run daemon in foreground |
| `init` | Scaffold `~/.config/agentd/` and install system service; config/AGENTS.md/.env created only if missing, skills always overwritten (upgrade sync) |
| `doctor` | Diagnose/repair |
| `service install\|uninstall` | Manage system service |

`service install` generates a platform-specific service definition (launchd plist on macOS, systemd user unit on Linux) and enables it. System environment variables (`PATH`, `HOME`, `XDG_*`, `PI_CODING_AGENT_DIR`, etc.) are snapshotted into the service definition. Secrets (API tokens, etc.) should be placed in `~/.config/agentd/.env` instead — the daemon loads this file at startup before resolving `${VAR}` references in config. Shell environment takes precedence over `.env` values.

Optional backend shortcut commands (`agentd pi/claude/codex`): syntactic sugar for spawn + wait + output result.

#### Output Format

- TTY: human-readable format
- Non-TTY (agent invocation), non-streaming commands output JSON envelope: success `{"ok": true, ...result_fields}`, failure `{"ok": false, "error": {"code": "...", "message": "..."}}`
- Streaming commands output one JSON object per line. Bounded streams (e.g., `wait --progress`) end with a final result envelope; open-ended streams (e.g., `logs --follow`, `ps --watch`) continue until client cancels

---
## 6. Configuration Model

### Resolution Priority

CLI flag → environment variable → config file → built-in defaults

### Config File Resolution

1. `-c` / `--config <path>`
2. `AGENTD_CONFIG` environment variable
3. `${XDG_CONFIG_HOME}/agentd/config.yaml`
4. `~/.config/agentd/config.yaml`
5. `~/.agentd.yaml`

No config file found = use built-in defaults.

### Workspace

Holds socket, database, pid file, log files.

Resolution: `AGENTD_WORKSPACE` → config `workspace` → `${XDG_STATE_HOME:-~/.local/state}/agentd`

### Key Config Items

```yaml
default_backend: pi

limits:
  max_depth: 3
  max_children_per_parent: 8
  max_total_workers: 64

channels:
  telegram:                                # built-in, no command needed
    spawn:                                 # optional defaults for actors from this channel
      cwd: ~/.config/agentd/agents/telegram
    env:
      TELEGRAM_BOT_TOKEN: ${TELEGRAM_BOT_TOKEN}
      TELEGRAM_ALLOWED_USERS: "123456"

inbox_gateway:
  enabled: false
  host: 127.0.0.1
  port: 8765
  public_base_url: null
```

- `default_backend`: used when `--backend` is omitted, default `pi`
- `limits`: concurrency and depth limits
- `channels`: channel adapters supervised by the daemon. Built-in channels (`telegram`, `cli`) need only `env`; custom channels specify `command` (list of strings). Each channel may include a `spawn:` block with `backend`, `cwd`, and `args` defaults for actors created by that channel. Environment values support `${VAR}` references resolved from the daemon's environment at startup. Channels with `enabled: false` are skipped. Install built-in channel dependencies via extras: `pip install agentd[telegram]`.
- `inbox_gateway`: HTTP inbox bridge configuration; `public_base_url` for reverse proxy scenarios

### Injected Actor Environment Variables

Each backend subprocess receives automatically:

| Variable | Value |
|---|---|
| `AGENTD_ACTOR_ID` | Actor's own ID |
| `AGENTD_INBOX_URL` | Optional; actor inbox HTTP endpoint |

### Environment Variable Model

Two layers:

- **Actor-level env**: set via `spawn.env`, held in memory at runtime (not persisted); serves as the default execution environment for all turns of that actor. Lost on daemon restart.
- **Turn-level env overlay**: set via `emit.env`, applies only to the turn triggered by that message; persisted in the `turn.opened` input snapshot; does not write back to actor default env.

`deliver_as=steer` prohibits carrying `env` (steer does not open a new turn; there is no place to apply env overlay).

Actual execution environment synthesis priority (high → low): turn env overlay > actor env > injected variables (`AGENTD_ACTOR_ID`, etc.) > daemon inherited environment.
Environment variables are injected at process startup. In v1 one-shot mode, each turn starts a new process, so overlay naturally takes effect.
Long-running backends do not support turn-level env overlay (process already running, no injection point).


---
## 7. System Components

### CLI

Command entry point. Parses commands, normalizes input, communicates with daemon via RPC.

### Daemon API Server

JSON-RPC 2.0 daemon on a Unix socket. Validates requests, resolves actor references, persists changes, invokes scheduler, streams logs/ps output. Optional HTTP inbox bridge (for agent self-registered webhooks).

### Scheduler

Orchestration layer. Makes decisions, does not execute.

- Responsible for: turn opening/ending, mailbox claiming, actor/turn state transitions, `follow_up`/`steer` delivery strategy, wakeup, close subtree, trigger delivery
- Not responsible for: directly executing backend processes

### Runtime

Execution layer. Executes, does not make decisions.

- Responsible for: executing already-formed turns, loading checkpoints, passing turn input, receiving steer, emitting `turn.started`/`turn.end`, reporting execution termination, updating checkpoints
- Not responsible for: choosing mailbox input, defining wakeup strategy

### Store

SQLite persistence layer. Persists actors, turns, mailbox, events, triggers. Enforces invariants, provides queries. Single-writer model.

### Backend Adapters

Integration module for each backend. Maps turn input to backend invocation, maps backend output to canonical signals. Handles checkpoint logic. Exposes capability flags (e.g., `supports_steer`).

Built-in: `pi`, `claude`, `codex`.

Backend adapter contract and per-adapter implementation details in §10.

### Module Structure

```
src/agentd/
├── cli/           → CLI entry, argument parsing
├── api/           → Daemon API server (RPC handling)
├── scheduler/     → Scheduler (state machine, turn management, EventBus)
├── runtime/       → Runtime (process execution, backend adapters)
│   └── backends/
├── store/         → Store (SQLite persistence)
├── config.py      → Configuration parsing
└── protocol.py    → Shared type definitions (RPC envelope, error codes)
```

### Streaming Subscription Backpressure

`logs --follow`, `ps --watch`, `wait --progress` subscribe to the daemon's internal public event stream (§4), not backend raw events. Each subscriber uses a count-bound queue. On overflow the subscriber is disconnected (returns `slow_consumer` error). For event streams (`logs --follow`, `wait --progress`), clients reconnect via `since_seq` to resume from the latest position; intermediate events may be skipped. For snapshot streams (`ps --watch`), clients re-issue the watch request to receive a fresh snapshot.

### Transport Size Limits

agentd uses two independent transport size limits:

| Limit | Value | Boundary | Purpose |
|---|---|---|---|
| `BACKEND_INPUT_MAX` | 16 MiB | runtime ← backend stdout | Hard guard on a single stdout line from an untrusted external process. Lines exceeding this are discarded with a warning (§11). |
| `AGENTD_FRAME_MAX` | 4 MiB | daemon socket / CLI stdout / channel subprocess readline | Frame limit for agentd's own line-delimited transport, applied uniformly to daemon socket, CLI output, and channel subprocess readers. |


### Local Security Model

- Unix socket file permissions `0600` (owner-only access)
- HTTP inbox bridge disabled by default; when enabled, listens on `127.0.0.1` only by default
- Public exposure requires a reverse proxy and external authentication layer; agentd does not include provider-specific auth

### Graceful Shutdown

SIGTERM / SIGINT both trigger the following sequence:

1. Stop accepting new RPC requests
2. Stop cron scheduler
3. Send stop to all running turns
4. Wait for all turns to end with timeout
5. Force terminate remaining processes after timeout
6. Close EventBus, Store

### Startup Reconciliation

On daemon startup, deterministically converge stale state:

1. Scan `running` turns' `exec_pid`; send SIGTERM to still-alive orphan processes
2. For each active actor's `running` turn: synthesize `turn.end(outcome=failed, error="daemon restarted")`, actor → `idle`
3. For each active actor's `pending` turn: reschedule execution
4. For each `idle` actor with queued messages: trigger wakeup

### Concurrency Limit

`max_total_workers` upper bound. When running turns reach the limit, new turns stay `pending` in queue until capacity frees up.

### Known Limitations (v1)

- No process heartbeat/liveness probe: if a backend process hangs (neither exits nor produces output), it cannot be actively detected. Relies on PID-based post-hoc detection.

---
## 8. Persistence Model

Model-level invariants are defined in §2. This section defines table structure, persistence layer constraints, and indexes.

### Actors

| Field | Description |
|---|---|
| `actor_id` | Primary key |
| `name` | Nullable; human-visible binding key |
| `scope_id` | Uniqueness domain |
| `parent_actor_id` | Nullable |
| `backend` | Adapter name |
| `backend_args` | JSON; command-line argument list |
| `cwd` | Working directory path |
| `state` | `idle\|active\|closed` |
| `checkpoint` | Nullable; JSON; `null` = disabled, non-null = enabled (spawn with `checkpoint=true` initializes to `{}`, actual data like `{"session_id": "..."}` written after turn ends) |
| `created_at`, `updated_at`, `closed_at` | |

### Turns

| Field | Description |
|---|---|
| `turn_id` | Primary key |
| `actor_id` | FK |
| `state` | `pending\|running\|ended` |
| `exec_pid` | Nullable; execution process PID |
| `result` | Nullable; TEXT; turn output |
| `outcome` | Nullable; termination classification: `succeeded\|failed\|canceled\|interrupted` |
| `error` | Nullable; TEXT; failure reason (present when failed) |
| `created_at`, `started_at`, `ended_at` | Nullable where applicable |

### Mailbox

| Field | Description |
|---|---|
| `message_id` | Primary key |
| `actor_id` | FK |
| `message_type` | |
| `payload` | JSON |
| `state` | `queued\|claimed\|acked` |
| `created_at`, `acked_at` | Nullable where applicable |

- `queued`: waiting to be claimed by a turn
- `claimed`: bound to an opened turn, waiting for that turn to complete
- `acked`: consumed by a completed turn

#### Message type taxonomy

| Form | Origin | Examples |
|---|---|---|
| bare predicate | peer-to-peer message authored by a user or another actor | `message` |
| `env.<source>.<event>` | inbound from outside the daemon | `env.telegram.message`, `env.webhook.github.push` |
| `env.<predicate>` | daemon-internal observation | `env.turn_completed` |

Daemon-internal `env.<predicate>` messages are environment observations delivered by the daemon, not authored by a peer actor.

- **`env.turn_completed`**: emitted to the direct parent's mailbox when a child actor's turn settles. Payload: `actor_id`, `actor_name`, `turn_id`, `outcome`, optional final text `result`, optional `error`. Delivery: best-effort, at-most-once.

### Events

Append-only log. `seq` is globally monotonically increasing, providing cross-actor global ordering.

| Field | Description |
|---|---|
| `seq` | Primary key; globally increasing |
| `actor_id` | |
| `turn_id` | Nullable |
| `event_type` | |
| `payload` | JSON |
| `created_at` | |

### Triggers

| Field | Description |
|---|---|
| `trigger_id` | Primary key |
| `target_actor_id` | FK |
| `kind` | v1: `cron` |
| `spec` | JSON; trigger specification (e.g., cron expression) |
| `message_type`, `payload` | Message generated when fired |
| `next_fire_at` | |
| `created_at` | |

Triggers are deleted when an actor closes. Trigger firing = system-generated `actor.emit`.

### Persistence Layer Constraints

- Turn opening input can be reconstructed from the event log (`turn.opened` event contains input snapshot)

### Indexes

- Unique index on `(scope_id, name)` for `idle|active` actors
- Per-actor unique index for `pending|running` turns
- Events indexed by actor + seq
- Events indexed by turn_id + seq (turn-level event queries)
- Mailbox indexed by actor + queued state
- Actors indexed by parent_actor_id (for close-subtree child lookup)
- Triggers indexed by target_actor_id (for deleting triggers on actor close)
- Triggers indexed by next_fire_at (for cron scheduling of due triggers)

---
## 9. Checkpoint Semantics

### Ownership

Belongs to actor, not turn. Single field `checkpoint` (actors table): `null` = disabled, non-null JSON = enabled. Spawn with `checkpoint=true` initializes to `{}`; `checkpoint=false` keeps `null`. After a turn ends, the backend adapter writes actual data (e.g., session_id) to this field.

### Defaults

| Type | Default | Rationale |
|---|---|---|
| Root actor | `true` | Long-lived, needs context continuity across turns |
| Child actor | `false` | Usually temporary tasks, each turn independent |

Can be explicitly overridden at spawn time.

- `checkpoint=true`: backend saves and restores session (`--session <file>` / `--resume <id>`)
- `checkpoint=false`: backend runs without session (`--no-session`), no save/restore

### Per-Turn Flow

1. **Load** (when `checkpoint=true`): runtime reads checkpoint from actor record → passes to backend adapter → emits `actor.checkpoint.loaded` (or `actor.checkpoint.missed`)
2. **Skip** (when `checkpoint=false`): no loading, backend starts without session
3. **Execute**: backend processes turn input
4. **Save** (when `checkpoint=true`): after turn ends, backend adapter extracts new checkpoint data → persists back to actor record → emits `actor.checkpoint.saved`

### Failure Strategy (v1)

- `checkpoint=true`, non-first turn, checkpoint exists but fails to load → turn outcome = `failed`, no silent fallback
- First turn, no checkpoint to load → normal, `actor.checkpoint.missed` is an informational event

### Public Semantics

Not an independent command surface. Manifested through `actor.checkpoint.*` events; clients can observe checkpoint status from logs.

Per-backend checkpoint implementations in §10.

---
## 10. Backend Adapter Implementation Details

### Common Contract

Each adapter must normalize backend behavior into canonical signals: `turn.started` / `turn.progress` / `turn.end` / checkpoint update. An adapter may pass result text to the runner via the `ParsedLine.result` field; this is a runtime-internal signal, orthogonal to `event_type`, and is not published to the public event stream. The final result is exposed through `turn.end` / `actor.wait` / `actor.status`.

Adapter output rules:

- Adapter MUST emit canonical payloads defined in §4 for all public events.
- Progress payloads are live-observation records (for example text/thinking deltas and tool name/arguments/status). Progress payloads MUST NOT contain raw backend objects, full tool output, full turn input, full final result, or cumulative partial content that could be expressed as deltas.
- Unmapped backend records are dropped. They MUST NOT appear in the public event stream.

Completion: `turn.end` is the sole turn completion signal. Fallback: only when no explicit terminal event arrives, synthesize `turn.end` based on execution termination + terminal intent.

Steering: `supports_steer=false` → `deliver_as=steer` fast-fails; `=true` → delivers to current turn.

Process termination strategy: v1 one-shot backends uniformly use `SIGTERM → wait timeout → SIGKILL`. No stdin pipe, no cooperative shutdown; SIGTERM is the softest available signal. Stop and close are identical in process termination means; they differ only at the semantic layer (stop → turn `interrupted` + actor `idle`; close → turn `canceled` + actor/subtree `closed`, see §3).

Message → prompt rendering rules: daemon renders mailbox messages into text before passing to backend CLI. Rules:

- `type=message`: use `payload.text` directly as prompt text
- Other typed events: serialize as `[{type}]\n` + payload key-value text block (nested values JSON-serialized). Strip `env.` prefix during rendering.

Channel-specific formatting (e.g., Telegram message structure expansion) is done by the channel adapter **before** calling `actor.emit`; daemon runtime is unaware of specific channel formats.

### pi

- Command: `pi --mode json -p "<prompt>" [--session <file>] <backend_args>`
- Output format: NDJSON (`--mode json`)
- Prompt input: `-p` flag, value is turn input text
- Completion detection: process exit. Pi emits `turn_end` after each assistant turn (tool-use turn, reply turn, …), so a single `turn_end` does **not** mean the conversation is finished. The adapter extracts the latest assistant text through `ParsedLine.result` (with `event_type="log"`) and lets the process continue until EOF; the raw `turn_end` object MUST NOT be forwarded as a public payload. The runner's generic "no explicit `turn.end` → synthesize on process exit" fallback handles completion.
- Progress mapping: pi's primary streaming event is `message_update` (wrapper); the adapter extracts `assistantMessageEvent.type` (`thinking_start`/`thinking_delta` → `thinking`, `text_delta` → `text`, `tool_use_start`/`tool_use_end` → `tool_call`). Standalone `tool_execution_start`/`tool_execution_end` events are also mapped to `tool_call` (using `toolName`/`args` fields).
- Checkpoint: `{session_id, session_cwd, session_timestamp}` → constructs exact file path `<session_dir>/--<cwd_slug>--/<ts_slug>_<session_id>.jsonl` → `--session <file>`. Falls back to error if checkpoint exists but file is not found (no glob).
- Capability: `supports_steer=false`

### claude

- Command: `claude -p "<prompt>" --output-format stream-json --verbose --permission-mode auto [--resume <session_id>] <backend_args>`
- Output format: stream-json NDJSON
- Prompt input: `-p` flag, value is turn input text
- Completion detection: `type=result` event → `turn.end`; assistant message with text → `turn.end` (fallback)
- Checkpoint: session_id → `--resume <session_id>`
- Capability: `supports_steer=false`

### codex

- Command: `codex exec "<prompt>" --json --full-auto [resume <thread_id>] <backend_args>`
- Output format: JSONL (`--json`)
- Prompt input: positional argument (first non-flag argument)
- Completion detection: `type=item.completed` + `item.type=agent_message` → `turn.end`; `type=turn.completed` → `turn.end` (fallback)
- Checkpoint: thread_id → `codex exec resume <thread_id>`
- Capability: `supports_steer=false`

---
## 11. Error Handling Strategy

### RPC Layer

| Situation | Handling |
|---|---|
| Request validation failure | JSON-RPC -32602 (invalid params), `data.type=invalid_params` |
| Actor not found | -32000, `data.type=not_found` |
| State conflict (e.g., emit to closed actor) | -32000, `data.type=conflict` |
| Backend start failure | -32000, `data.type=backend_error` |
| Streaming subscriber too slow, disconnected | -32000, `data.type=slow_consumer` |

### Scheduler Layer

| Situation | Handling |
|---|---|
| Turn opening failure | `turn.end(outcome=failed)`, actor back to `idle` |
| State transition violates invariant | Log error, reject operation, do not silently swallow |
| Concurrency limit reached | New turn stays `pending` in queue |

### Runtime Layer

| Situation | Handling |
|---|---|
| Backend process abnormal exit | Synthesize `turn.end(outcome=failed, error=exit code+stderr excerpt)` |
| Backend output parse error | Log warning, skip that line, do not terminate turn |
| Steer delivery failure | Return error to caller |
| Checkpoint load failure (non-first turn) | `turn.end(outcome=failed, error=reason)` |
| Backend stdout line exceeds `BACKEND_INPUT_MAX` (16 MiB) | Discard the line, log warning with backend / turn / dropped bytes. If a terminal record is unavailable as a result, fail the turn with `error=backend_record_too_large` (appended to any other failure reason on `exit_code != 0` / timeout paths). |

### Store Layer

| Situation | Handling |
|---|---|
| SQLite write failure | Propagate upward, no retry |
| Schema mismatch | Daemon refuses to start |

---
## 12. Channel Integration Layer

Auxiliary integration layer, not part of the core model. Each channel is a standalone script that interacts with the daemon via CLI.

### Architecture

```
IM Platform ←→ Transport ←→ Channel adapter (standalone script) ←→ agentd CLI
```

### Lifecycle

Channels can run standalone or be supervised by the daemon via the `channels` config section (§6). When supervised:

- `agentd serve` spawns each enabled channel as a child process
- Crashed channels are restarted with exponential backoff (1s → 2s → ... → 60s max)
- Backoff resets after the channel runs healthy for 30s
- `agentd` shutdown terminates all channel processes (SIGTERM → 5s → SIGKILL)

### Adapter Contract

Each adapter does two things:

- **Inbound**: platform message → `agentd emit <actor> --type env.<platform> --payload '{...}'` (adapter-layer logic: emit first, fallback to spawn if actor doesn't exist; this is the adapter's notify pattern, not daemon behavior — daemon's emit returns `not_found` for non-existent actors)
- **Outbound**: monitor actor progress/state → push platform-appropriate updates (typing indicator, progress, failures)

Adapter is responsible for platform-specific logic (message format, length limits, Markdown compatibility, permission checking). agentd is unaware of platform details.

Channel readline requirements:

- Channel subprocesses reading agentd CLI streaming output (e.g., `agentd wait --progress`, `agentd logs --follow`) MUST explicitly set a readline limit. The asyncio default (64 KiB) is not a safe upper bound for agentd-formatted streams.
- Recommended implementation value: 4 MiB (aligns with `AGENTD_FRAME_MAX` and the built-in `RpcClient`).
- Per-platform splitting, attaching, and formatting of the result (e.g., Telegram message limits, file attachments) is the channel adapter's responsibility.

Result delivery is channel-specific. Channels may relay final results, or the agent may reply directly through a channel skill (as the Telegram reference adapter expects).

### Transport Modes

Two transports cover all mainstream IM platforms:

| Transport | Mechanism | Applicable Platforms |
|---|---|---|
| **WebSocket** | Adapter initiates outbound WebSocket connection to platform | Lark, DingTalk, Slack (Socket Mode), Discord (Gateway) |
| **Long-polling** | Adapter polls platform HTTP API for new messages | Telegram (`getUpdates`), Matrix (Sync API) |

Both transports require no public IP.

### Platform Transport Reference

| Platform | Transport | No Public IP Needed |
|---|---|---|
| Telegram | Long-polling | ✅ |
| Lark | WebSocket | ✅ |
| DingTalk | WebSocket (Stream) | ✅ |
| Slack | WebSocket (Socket Mode) | ✅ |
| Discord | WebSocket (Gateway) | ✅ |
| Matrix | Long-polling (Sync) | ✅ |

### Telegram Adapter (Reference Implementation)

One of v1's deliverables. Built-in adapter (`agentd.channels.telegram`), using long-polling transport. Install via `pip install agentd[telegram]`.

#### Core Flow

1. Long-poll `getUpdates` for messages
2. Serialize processing per `chat_id` (no concurrency within same chat)
3. Message → notify pattern: try `agentd emit` first, fallback to `agentd spawn` if actor doesn't exist
4. `agentd wait <actor> --progress` to stream progress events, push to Telegram in real time
5. After turn ends, report failure/stop if needed; successful replies are sent by the agent via the Telegram skill

#### Progress Display

Progress pipeline has two layers:

**ProgressState (channel-agnostic, `lib.py`)**: parses the `turn.progress` event stream (§4 normalized format), maintains current phase and tool step count, outputs human-readable progress text.

| `turn.progress.payload.type` | Phase |
|---|---|
| `thinking` | Thinking |
| `tool_call` (`status=running`) | Running tool |
| `tool_call` (`status=failed`) | Tool failed |
| `text` | Generating reply |

`tool_call` with `status=completed` is silently ignored — after a tool finishes, the next event (thinking, another tool, or text) naturally takes over.

Tool summary extracts operation details from `turn.progress` payload's `name` and `args` (e.g., `name=read, args.path=src/main.py` → `Reading src/main.py`; `name=bash, args.command="npm test"` → `$ npm test`). Sensitive information (tokens, API keys) is automatically redacted.

Output format example:
```
✨ Running tool…
Step 3: Reading src/main.py
```

**ProgressReporter (Telegram-specific, `telegram.py`)**: manages a single editable Telegram message.

- **Delayed creation**: waits 1s before sending to avoid message flicker on fast tasks
- **Edit merging**: merges high-frequency updates via lock to avoid Telegram API rate limiting
- **Deletion on end**: deletes progress message after turn ends to keep chat clean

ProgressState can be reused by other channel adapters; only the rendering layer needs replacement.

#### Actor Naming

`telegram:<chat_id>`, one root actor per Telegram chat.

#### Auto New Session

Actor idle beyond threshold (default 2 hours): on next message, close + re-spawn (new checkpoint). Also supports `/new` command for manual trigger.

#### Platform-Specific Logic

- Markdown rendering failure fallback to plain text
- Typing indicator refreshed every 4.5s
- Management commands: `/ping`, `/help`, `/status`, `/logs`, `/stop`, `/new`
- `TELEGRAM_ALLOWED_USERS` whitelist authentication

#### Environment Variable Injection

Each actor turn receives `TELEGRAM_BOT_TOKEN`, `TELEGRAM_DEFAULT_CHAT_ID`, `TELEGRAM_REPLY_TO_MESSAGE_ID`, enabling agents to proactively reply via the `telegram` skill.

#### SKILL File

Adapter loads `skills/telegram/SKILL.md` for the actor, telling the agent it's running in a Telegram environment, the message payload format, and how to reply.

---
## 13. Agent Skill Definition

One of the deliverables: `skills/agentd/SKILL.md`. Read by agents running on agentd, telling them how to interact with the daemon.

Skill format references each backend's skill specification:
- Codex: https://developers.openai.com/codex/concepts/customization#skills
- Claude Code: https://code.claude.com/docs/en/skills

### Content Requirements

#### Available Commands (condensed, agent perspective)

| Command | Purpose |
|---|---|
| `agentd spawn [--name <name>] --message "..."` | Create actor and send message |
| `agentd emit <actor> --message "..."` | Send message to existing actor |
| `agentd emit <actor> --type <type> --payload '{...}'` | Send typed message |
| `agentd stop <actor>` | Stop current turn |
| `agentd stop <actor> --close` | Close actor and subtree |
| `agentd wait <actor>` | Wait for actor to return to idle/closed |
| `agentd status <actor>` | View actor status and last turn result |
| `agentd ps` | List all actors |

#### Environment Variables

Injected variables available within the agent process:

| Variable | Purpose |
|---|---|
| `AGENTD_ACTOR_ID` | Own actor ID; use for `--parent-actor-id` when spawning child actors |
| `AGENTD_INBOX_URL` | Own inbox HTTP endpoint (for registering webhooks) |

#### Common Patterns

- **Delegate subtask**: `child=$(agentd spawn --name reviewer --message "review PR #42" --parent-actor-id $AGENTD_ACTOR_ID | jq -r '.actor_id')` → `agentd wait "$child"` → `agentd status "$child"` to get result
- **Message existing agent**: `agentd emit bob --message "help me review this"` → `agentd wait bob`
- **Get result**: `agentd status <actor>` returns JSON containing `last_turn.result`
- **Child actor naming**: `name` is optional; when provided, unique under same parent; when omitted, reference via `actor_id`

#### Output Format

In non-TTY environments (agent invocation), all commands output JSON envelope: `{"ok": true, ...}`. Agents should parse JSON to obtain results.

#### Config Preset

Agents choose backend, cwd, and arguments explicitly via CLI flags (`--backend`, `--cwd`, `--args`). Channel adapters may have spawn defaults configured per-channel (§6).

---
## 14. Acceptance Checklist

1. `spawn` with no input → `idle` actor, no turn
2. `name` optional; non-terminal actor names unique under same parent (`null` excluded from check)
3. At most one active turn per actor
4. `emit` persists typed message to mailbox
5. `follow_up` = next-turn input
6. `steer` = current-turn control input; errors if unsupported; auto falls back to `follow_up` when turn is `pending`
7. Mailbox claim in `(created_at, message_id)` FIFO order
8. Explicit `turn.end` is the sole completion signal; synthesized as fallback when missing
9. Soft stop → `interrupted` + actor `idle`
10. Hard close → `canceled` + actor/subtree `closed`; recursively closes all descendants
11. Checkpoint belongs to actor; root default enabled, child default disabled; load failure → fail turn
12. Environment variables two-layer synthesis: actor env + turn overlay; steer prohibits carrying env
13. RPC aligns with JSON-RPC 2.0; `actor.stop` / `actor.close` separated
14. CLI actor-first; status/logs exposed in turn-attached manner
15. Startup reconciliation: after daemon restart, clean up orphan processes, converge stale state
16. Graceful shutdown: SIGTERM/SIGINT triggers orderly shutdown
17. Telegram adapter: long-polling for messages → emit/spawn → real-time progress push → result reply; ProgressState is channel-agnostic and reusable
