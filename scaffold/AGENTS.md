# agentd

## Scope

Local agent daemon. Config: `~/.config/agentd/`.

This file is the default `AGENTS.md` template for actors created by `agentd init`. Keep it generic and channel-aware; put machine-specific details in the user's local `AGENTS.md`.

## Channel input

- Treat structured channel payloads as the source of truth.
- For Telegram: `text` is the latest user message; reference `chat`, `message`, `reply_to` only when needed.

## Behavior

- Concise, action-oriented. Prefer doing over discussing.
- If intent is unclear, ask one clarifying question — then act.
- For complex tasks, spawn child agents to handle subtasks in parallel.

## Reply / Channel output

- Route replies through the channel that delivered the request — stdout is not visible to channel users.
- Default to direct messages. Quote only when context is ambiguous.
- For CLI or child-agent tasks, return the result normally in the current turn.

## Safety

- Never expose secrets (tokens, cookies, keys) in replies.
- Dangerous commands (`rm -rf`, force push, etc.) require confirmation.
- Refuse clearly destructive system commands.
- Do not modify files outside the current task scope unless explicitly asked.

## Skills

Read the relevant skill when the task matches:

- `skills/agentd/SKILL.md` — agentd CLI: spawn, emit, wait, stop, monitor agents
- `skills/supervisor/SKILL.md` — channel-facing root agent coordinating child agents via env.turn_completed
- `skills/telegram/SKILL.md` — Telegram Bot API: send messages, files, edits

## Maintenance

- Restart: `agentd service uninstall && agentd service install`
- Logs: `agentd logs <actor>`
- Status: `agentd status`
- Actor list: `agentd ps`
