# TODO

## Architecture gaps

- **Control lease**: no arbitration when multiple sources emit to same actor. Required for multi-bot / team.
- **Server layer**: UI dashboard needs HTTP/WebSocket beyond localhost. `http_gateway.py` is a starting point.
- **Optimistic concurrency**: actor state has no version number.
- **Mailbox `from` field**: emit currently has no sender identity. Peer messages (actor → actor) and env messages (channel/trigger/webhook) collapse into the same source-less stream. Add optional `from` to `actor.emit` (CLI auto-fills from `AGENTD_ACTOR_ID`) so peer reasoning, commitment chains, and trigger filtering by sender become possible. Trigger when dialectic / debate / red-team patterns or sender-based trigger filters are needed.

Priority order (each layer is prerequisite for the next):
1. Live delivery (Pi RPC) — single-actor experience
2. UI dashboard — observability
3. Orchestration — multi-agent patterns (skills + daemon triggers)
4. Multi-bot / team (lease + concurrency) — multi-actor coordination

## Live delivery

v1 is one-shot (process per turn). Future: long-running backends.

**Pi** (`--mode rpc`, bidirectional stdin/stdout):
- [ ] Steer via stdin, no process kill
- [ ] Follow-up: drain inbox, one process handles multiple messages
- [ ] Fork: expose pi's `fork` through agentd API

**Claude** (`--input-format stream-json --output-format stream-json`):
- [ ] Follow-up via stdin JSON append
- [ ] Session: `--resume <id>` / `--continue`

**Codex**: one-shot only, stays as interrupt-resume.

## UI dashboard

- [ ] HTTP API for actor list, logs, status
- [ ] Token / cost tracking per actor
- [ ] Web frontend: actor list, log streaming, cost breakdown, event timeline

## Orchestration

Ref: Paseo — orchestrate, loop, handoff, chat

- [ ] **Handoff**: delegate to another backend with full context (e.g. plan with Claude → implement with Codex)
- [ ] **Loop**: worker/verifier cycle until exit condition
- [ ] **Orchestrate**: orchestrator spawns implementers/reviewers, coordinates via CLI, does NOT code
- [ ] **Chat room**: shared message log between peer agents

Builds on existing primitives (spawn/emit/wait) + skill files for each pattern.

**Daemon prerequisite — event-driven triggers**:
LLM orchestrators can't block in a turn waiting for siblings. Need
`Trigger.kind = event` (source `turn.end`) so child completion can
re-enter the orchestrator's mailbox and wake the next turn.
Design: [docs/design/event-triggers.md](design/event-triggers.md).

## Multi-bot channels

Multiple agent personas in a single chat. Requires control lease.

- [ ] Adapter config: list of agents per channel/group
- [ ] Relay logic: persona replies → emit to other personas with sender tag
- [ ] Self-filter: prompt engineering, not infrastructure

## Team collaboration

Flat peer collaboration within a parent actor. Requires lease + concurrency.

- [ ] Peer-to-peer messaging (relax emit from parent-child to sibling scope)
- [ ] Shared channel: parent-level append-only log, inject on turn start
- [ ] `agentd team` CLI

## SDK-level provider integration

Ref: Paseo — Claude Agent SDK, Codex app-server protocol, OpenCode SDK

- [ ] Evaluate Claude Agent SDK for richer event stream
- [ ] Evaluate Pi ACP for structured integration
- [ ] Trade-off: richer events vs tighter coupling

## AI memory

- [ ] Memory store: key-value or document-based, scoped per actor name
- [ ] `agentd memory get/set/search` CLI + RPC
- [ ] Automatic context injection on turn start
- [ ] Embedding-based retrieval over past events / memories

## Self-evolving agent

- [ ] Reflection loop: review outcome on turn completion, persist lessons
- [ ] Skill library: agent-authored reusable procedures
- [ ] Prompt evolution: agent proposes and tests modifications to own system prompt
