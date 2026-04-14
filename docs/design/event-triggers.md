# Event-driven triggers

Status: proposed (v1 scope: `turn.end` fan-in primitive)

## Why

`Trigger` in v1 is cron-only (`kind=cron`). Cron covers scheduled wake-ups.
Event triggers cover the common coordination need: **wake an actor when a
child turn completes**.

The primary use case is fan-in for orchestration skills. A parent actor can
spawn several child actors, register `turn.end` triggers for each child, and
receive `env.sibling_done` messages as children finish. This moves rendezvous
handling into the daemon and keeps orchestration skills small.

## Scope (v1 fan-in primitive)

Extend `Trigger.kind` with `event` alongside `cron`. A v1 event trigger is a
small declarative subscription over daemon lifecycle events:

```
trigger.add
  kind=event
  on=turn.end
  filter=<exact field predicate>    # actor=<id/name>, outcome=<value>
  target=<actor>                    # recipient mailbox
  message_type=<type>               # e.g. env.sibling_done
  payload=<json>                    # with {{event.*}} variables
```

Event source for v1:

- `turn.end` — fires on every turn's transition to `ended` (all outcomes).
  Filterable by `actor`, `outcome`.

Payload templating is literal substitution only:

- `{{event.actor_id}}`
- `{{event.turn_id}}`
- `{{event.outcome}}`
- `{{event.result}}` (optional, size-capped or omitted when large)

Deferred event sources:

- `actor.closed` watchdog triggers
- `actor.emit` / message arrival triggers
- `actor.state` transition triggers
- `agentd event emit <type>` user-space semantic events
- cross-tool semantic event bus for jj / pi-control / external hooks

## Concrete case: fan-in barrier

```
for child in [a1, a2, a3]:
    trigger.add kind=event on=turn.end filter=actor=child \
                target=orchestrator message_type=env.sibling_done \
                payload='{"actor_id":"{{event.actor_id}}", "turn_id":"{{event.turn_id}}", "outcome":"{{event.outcome}}"}'
```

The orchestrator wakes on each completion and tracks remaining children in its
own state.

## Implementation sketch

- Protocol: extend `trigger.add` parameters with an event variant:
  `kind=event`, `event_type`, `filter`, `target`, `message_type`, `payload`.
- Persistence: reuse `triggers` table; store `event_type`, `filter_json`, and
  rendered-message config in the existing `spec` JSON column.
- Dispatch: after the scheduler publishes a `turn.end` event, evaluate matching
  event triggers and call `scheduler.emit(target, rendered_message)`.
- Loop guard: include source event metadata in the generated message and skip
  re-firing a trigger for the same `(trigger_id, source_event_seq)` pair.
- RPC / CLI: add `agentd trigger add --event turn.end --filter actor=a1
  --target b --type env.sibling_done`; keep `trigger.ls` / `trigger.rm`
  behavior shared with cron triggers.
- Tests: fan-in across multiple children, actor-name filters, outcome filters,
  closed/removed target handling, trigger removal, loop guard.

## What this unlocks

- `Orchestration skills → Loop / Orchestrate` can coordinate child completion
  via mailbox messages.
- Agent-authored skills can express "wake me when this child finishes" as a
  daemon-level subscription.
- `agentd wait` remains useful for CLI users and simple scripts; long-lived
  workflows use mailbox delivery.

## Deferred design

General semantic events and cross-tool hooks belong in a later design after
`turn.end` fan-in proves the trigger shape.
