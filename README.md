# agentd

[![python](https://img.shields.io/badge/-Python_3.12+-blue?logo=python&logoColor=white&style=flat-square)](https://python.org)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json&style=flat-square)](https://github.com/astral-sh/ruff)
[![license](https://img.shields.io/badge/License-MIT-green.svg?labelColor=gray&style=flat-square)](LICENSE)

## Why agentd?

agentd is a local daemon that gives AI agents a durable coordination layer
of their own.

- **Agent-native** — agents `spawn`, `emit`, and `wait` via a thin CLI; the daemon stays minimal.
- **Event-driven** — agents persist across runs, waking on events and resuming where they left off.
- **Harness-agnostic** — [Pi](https://github.com/badlogic/pi), [Claude Code](https://docs.anthropic.com/en/docs/claude-code), [Codex](https://github.com/openai/codex), or your own; agents collaborate across backends.

## Quick start

```bash
# Install and start
uv tool install agentd   # or: pipx install agentd
agentd init              # config, skills, system service

# Spawn an agent and wait for it to finish
agentd spawn --name nova --message "Build a REST API ..."
agentd wait nova

# Later — send a follow-up, agent wakes and continues
agentd emit nova --message "Add authentication ..."
agentd wait nova

# Agents collaborate across backends
agentd spawn --name reviewer --backend codex --message "Review nova's code ..."
agentd wait reviewer
```

## Configuration

Loaded from `~/.config/agentd/config.yaml`.
Secrets go in `~/.config/agentd/.env` (loaded automatically).
See [`scaffold/config.yaml`](scaffold/config.yaml) for all options.

```yaml
default_backend: pi          # pi | claude | codex
```

### Channels

Channel adapters bridge messaging platforms to agentd, supervised by the daemon.<br>
Telegram is the built-in channel (`uv tool install 'agentd[telegram]'`);
custom adapters specify a `command:`.

```yaml
channels:
  telegram:                              # built-in, no command needed
    # spawn:                             # optional defaults for actors from this channel
    #   cwd: ~/.config/agentd/agents/telegram
    env:
      TELEGRAM_BOT_TOKEN: ${TELEGRAM_BOT_TOKEN}
      TELEGRAM_ALLOWED_USERS: "123456"
  slack:                                 # custom adapter
    command: ["python", "slack_bot.py"]
    env:
      SLACK_TOKEN: ${SLACK_TOKEN}
```

## Docs

- [Scaffold](scaffold/) — files installed by `agentd init`
- [Docs](docs/) — spec and design notes
