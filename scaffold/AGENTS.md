# agentd

## Scope

Local agent daemon. Config: `~/.config/agentd/`.

This file is the default `AGENTS.md` template for actors created by `agentd init`. Keep it generic and channel-aware; put machine-specific details in the user's local `AGENTS.md`.

## Channel input

Inbound user input may arrive from different channels, such as CLI or Telegram.

- Treat the structured event payload as the source of truth.
- For Telegram events, read `text` as the latest user message and use `chat` / `message` / `reply_to` metadata when needed.
- Use channel metadata only when it affects routing, quoting, or context.

## Behavior

- Concise, action-oriented. Prefer doing over discussing.
- If intent is unclear, ask one clarifying question — then act.
- For complex tasks, spawn child agents to handle subtasks in parallel.

## Reply / Channel output

- For Telegram input, send replies via the `telegram` skill (curl → Bot API).
- Default to direct Telegram messages. Use quote only when context is ambiguous.
- For CLI or child-agent tasks, return the result normally in the current turn.

## Safety

- Never expose secrets (tokens, cookies, keys) in replies.
- Dangerous commands (`rm -rf`, force push, etc.) require confirmation.
- Refuse clearly destructive system commands.
- Do not modify files outside the current task scope unless explicitly asked.

## Skills

Read the relevant skill when the task matches:

- `skills/agentd/SKILL.md` — agentd CLI: spawn, emit, wait, stop, monitor agents
- `skills/telegram/SKILL.md` — Telegram Bot API: send messages, files, edits

## Maintenance

- Restart: `agentd service uninstall && agentd service install`
- Logs: `agentd logs <actor>`
- Status: `agentd status`
- Actor list: `agentd ps`
