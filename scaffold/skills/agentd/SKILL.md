---
name: agentd
description: "Use when the user wants to: spawn, stop, or monitor agents; send messages or follow-ups; run agents in parallel; collect results; schedule recurring work with triggers; or diagnose daemon issues. Trigger on any mention of 'agentd', and also on multi-agent patterns like 'run these in parallel', 'spawn a child agent', 'set a timed wakeup', or 'route a webhook into an agent'. Do NOT trigger for generic subprocess/asyncio, Celery/task-queue, or systemd/launchd questions unrelated to agentd."
---

# agentd

Durable coordination daemon for AI agents.
Run `agentd init` to set up config, skills, and system service (`~/.config/agentd/`).
For architecture and internals, see [docs](https://github.com/tshu-w/agentd/tree/main/docs).

## Concepts

Agents run as **actors** — long-lived containers with a mailbox that persist across runs.
Each actor cycles through **turns** (one execution at a time) and wakes on incoming events.
Actors have three states: `idle`, `active`, `closed` (terminal).
Use `--name` for human-readable identity; otherwise reference by `actor_id`.
Child actors are spawned with `--parent-actor-id $AGENTD_ACTOR_ID`; names are scoped to the parent.
Checkpoint (session continuity) is enabled by default for root actors, disabled for children.

Backends: `pi` (default), `claude`, `codex` — set via `--backend` or config preset.

## CLI

Commands: `spawn`, `emit`, `stop`, `wait`, `ps`, `logs`, `status`, `trigger`, `init`, `serve`, `doctor`, `service`.
Run `agentd <command> --help` for full flags.

## Environment variables

Agent processes receive these injected variables:

| Variable | Purpose |
|----------|---------|
| `AGENTD_ACTOR_ID` | Your actor ID — use for `--parent-actor-id` when spawning children |
| `AGENTD_INBOX_URL` | Your inbox HTTP endpoint (if HTTP gateway enabled) |

## Common patterns

### Delegate async (fire-and-forget with callback)

```bash
# $AGENTD_ACTOR_ID expands to parent's own ID. The daemon delivers
# env.turn_completed to the parent's mailbox each time this child's turn
# settles.
agentd spawn --name researcher \
  --parent-actor-id "$AGENTD_ACTOR_ID" \
  --message "Research X."
# Parent turn ends → idle
# Child finishes → daemon emits env.turn_completed → parent wakes
```

### Delegate and wait

```bash
agentd spawn --name reviewer \
  --parent-actor-id "$AGENTD_ACTOR_ID" \
  --message "Review PR #42"
agentd wait reviewer
result=$(agentd status reviewer --result | jq -r '.last_turn.result')
```

### Implement → review loop

```bash
agentd spawn --name coder \
  --parent-actor-id "$AGENTD_ACTOR_ID" \
  --message "Implement: $TASK"
agentd wait coder
result=$(agentd status coder --result | jq -r '.last_turn.result')

agentd spawn --name reviewer \
  --parent-actor-id "$AGENTD_ACTOR_ID" \
  --message "Review critically. Reply LGTM only if fully acceptable: $result"
agentd wait reviewer
feedback=$(agentd status reviewer --result | jq -r '.last_turn.result')

while ! echo "$feedback" | grep -qi "LGTM"; do
  agentd emit coder \
    --message "Review feedback below. Address valid points, push back if you disagree: $feedback"
  agentd wait coder
  result=$(agentd status coder --result | jq -r '.last_turn.result')

  agentd emit reviewer \
    --message "Revised implementation. LGTM if acceptable, otherwise explain remaining issues: $result"
  agentd wait reviewer
  feedback=$(agentd status reviewer --result | jq -r '.last_turn.result')
done
```

### Timed wakeups (set yourself an alarm)

You can schedule a message to your own mailbox instead of blocking or polling.
Write the payload text as an instruction to your future self — it arrives as a
normal message with no other context.

```bash
# One-shot: wake up in 3 hours (also --at "2026-07-06T09:00", local tz if no offset)
agentd trigger add "$AGENTD_ACTOR_ID" --in 3h \
  --payload '{"text": "Check whether CI for pi-tape finished; report the outcome to the user."}'

# Recurring: cron (local time) or fixed interval
agentd trigger add "$AGENTD_ACTOR_ID" --schedule "0 9 * * *" \
  --payload '{"text": "Morning briefing: summarize overnight notifications."}'
agentd trigger add "$AGENTD_ACTOR_ID" --every 30m \
  --payload '{"text": "Poll the release checklist; stop this trigger (trigger rm) once everything is green."}'
```

One-shot triggers delete themselves after firing; recurring ones persist until
`agentd trigger rm <trigger_id>` (list with `agentd trigger ls`). Use an alarm
for "keep an eye on X until Y" tasks: end your turn instead of waiting.

## Output format

Non-TTY (agent calling agentd): JSON envelope per line.

```json
{"ok": true, "actor_id": "act_7a3f1b9e2d4c", "state": "active", ...}
{"ok": false, "error": {"code": "not_found", "message": "actor not found"}}
```

Streaming commands (`wait --progress`, `logs --follow`) output one JSON object per line.
Bounded streams (`wait`) end with a final result object.
