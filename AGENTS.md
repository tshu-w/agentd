# AGENTS.md

[`docs/spec.md`](docs/spec.md) is the authoritative behavior spec. Change the spec before changing semantics. When editing `docs/spec.md`, apply the same change to `docs/spec.zh.md` in the same commit.

Secrets (API tokens) go in `~/.config/agentd/.env`, never in `config.yaml` or launchd plist. The plist only snapshots system paths (`PATH`, `HOME`, `XDG_*`, `*_DIR`).

## Development

```bash
uv sync
uv run ruff check src
uv run pytest -q
```

## Deploy

```bash
uv tool install --force '.[telegram]'
launchctl kickstart -k gui/$(id -u)/dev.agentd.daemon
```
