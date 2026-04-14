"""Tests for agentd.config — file discovery, parsing, env overrides, channels."""

from pathlib import Path

import pytest

from agentd.config import AgentDConfig

CONFIG_YAML = """\
limits:
  max_depth: 3
  max_children_per_parent: 4
  max_total_workers: 64
"""


# ---------------------------------------------------------------------------
# Explicit path loading
# ---------------------------------------------------------------------------


def test_load_explicit_path(tmp_path):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(CONFIG_YAML, encoding="utf-8")

    cfg = AgentDConfig.load(str(cfg_file))
    assert cfg.limits.max_depth == 3
    assert cfg.limits.max_children_per_parent == 4
    assert cfg.limits.max_total_workers == 64


def test_load_explicit_path_with_workspace(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text("workspace: ~/agentd-ws\n", encoding="utf-8")
    monkeypatch.setenv("HOME", str(tmp_path))

    cfg = AgentDConfig.load(str(cfg_file))
    assert cfg.workspace == str((tmp_path / "agentd-ws").resolve())


# ---------------------------------------------------------------------------
# Auto-discovery chain
# ---------------------------------------------------------------------------


def test_load_none_returns_default_when_no_file_exists(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.delenv("AGENTD_CONFIG", raising=False)

    cfg = AgentDConfig.load(None)
    assert cfg.limits.max_depth == 3
    assert cfg.limits.max_children_per_parent == 8
    assert cfg.default_backend == "pi"


def test_load_none_discovers_xdg_config_home(tmp_path, monkeypatch):
    xdg = tmp_path / "xdg"
    cfg_file = xdg / "agentd" / "config.yaml"
    cfg_file.parent.mkdir(parents=True)
    cfg_file.write_text(CONFIG_YAML, encoding="utf-8")

    monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.delenv("AGENTD_CONFIG", raising=False)

    cfg = AgentDConfig.load(None)
    assert cfg.limits.max_depth == 3


def test_load_none_falls_back_to_home_config(tmp_path, monkeypatch):
    cfg_file = tmp_path / ".config" / "agentd" / "config.yaml"
    cfg_file.parent.mkdir(parents=True)
    cfg_file.write_text(CONFIG_YAML, encoding="utf-8")

    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.delenv("AGENTD_CONFIG", raising=False)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)

    cfg = AgentDConfig.load(None)
    assert cfg.limits.max_depth == 3


def test_agentd_config_env_has_priority(tmp_path, monkeypatch):
    env_cfg = tmp_path / "env-config.yaml"
    env_cfg.write_text("limits:\n  max_depth: 7\n", encoding="utf-8")

    xdg_cfg = tmp_path / "xdg" / "agentd" / "config.yaml"
    xdg_cfg.parent.mkdir(parents=True)
    xdg_cfg.write_text("limits:\n  max_depth: 2\n", encoding="utf-8")

    monkeypatch.setenv("AGENTD_CONFIG", str(env_cfg))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)

    cfg = AgentDConfig.load(None)
    assert cfg.limits.max_depth == 7


# ---------------------------------------------------------------------------
# Env overrides
# ---------------------------------------------------------------------------


def test_env_overrides_limits(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(CONFIG_YAML, encoding="utf-8")

    monkeypatch.setenv("AGENTD_LIMITS_MAX_DEPTH", "9")
    monkeypatch.setenv("AGENTD_LIMITS_MAX_CHILDREN_PER_PARENT", "10")
    monkeypatch.setenv("AGENTD_LIMITS_MAX_TOTAL_WORKERS", "999")

    cfg = AgentDConfig.load(str(cfg_file))
    assert cfg.limits.max_depth == 9
    assert cfg.limits.max_children_per_parent == 10
    assert cfg.limits.max_total_workers == 999


def test_invalid_env_override_exits(monkeypatch):
    monkeypatch.setenv("AGENTD_LIMITS_MAX_DEPTH", "abc")
    monkeypatch.delenv("AGENTD_CONFIG", raising=False)

    with pytest.raises(SystemExit) as exc_info:
        AgentDConfig.load(None)
    assert "AGENTD_LIMITS_MAX_DEPTH" in str(exc_info.value)


# ---------------------------------------------------------------------------
# default_backend
# ---------------------------------------------------------------------------


def test_default_backend_from_config(tmp_path):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text("default_backend: claude\n", encoding="utf-8")

    cfg = AgentDConfig.load(str(cfg_file))
    assert cfg.default_backend == "claude"


def test_default_backend_builtin():
    """When not specified, default_backend is pi."""
    cfg = AgentDConfig()
    assert cfg.default_backend == "pi"


# ---------------------------------------------------------------------------
# inbox_gateway
# ---------------------------------------------------------------------------


def test_inbox_gateway(tmp_path):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(
        "inbox_gateway:\n"
        "  enabled: true\n"
        "  host: 0.0.0.0\n"
        "  port: 9988\n"
        "  public_base_url: https://relay.example.test/agentd\n",
        encoding="utf-8",
    )

    cfg = AgentDConfig.load(str(cfg_file))
    assert cfg.inbox_gateway.enabled is True
    assert cfg.inbox_gateway.host == "0.0.0.0"
    assert cfg.inbox_gateway.port == 9988
    assert cfg.inbox_gateway.public_base_url == "https://relay.example.test/agentd"


# ---------------------------------------------------------------------------
# Channel spawn defaults
# ---------------------------------------------------------------------------


def test_channel_spawn_defaults(tmp_path):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(
        "channels:\n"
        "  telegram:\n"
        "    spawn:\n"
        "      backend: claude\n"
        f"      cwd: {tmp_path / 'tg'}\n"
        '      args: ["--model", "haiku"]\n',
        encoding="utf-8",
    )

    cfg = AgentDConfig.load(str(cfg_file))
    sp = cfg.channels["telegram"].spawn
    assert sp.backend == "claude"
    assert sp.cwd == str((tmp_path / "tg").resolve())
    assert sp.args == ["--model", "haiku"]


def test_channel_spawn_defaults_empty(tmp_path):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(
        "channels:\n  telegram:\n    env:\n      TOKEN: abc\n",
        encoding="utf-8",
    )

    cfg = AgentDConfig.load(str(cfg_file))
    sp = cfg.channels["telegram"].spawn
    assert sp.backend is None
    assert sp.cwd is None
    assert sp.args is None


# ---------------------------------------------------------------------------
# Workspace resolution
# ---------------------------------------------------------------------------


def test_workspace_env(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENTD_WORKSPACE", str(tmp_path / "ws"))

    cfg = AgentDConfig.load(None)
    assert cfg.workspace == str((tmp_path / "ws").resolve())


def test_resolve_workspace_creates_dir(tmp_path):
    cfg = AgentDConfig(workspace=str(tmp_path / "new_ws"))
    ws = cfg.resolve_workspace()
    assert ws.exists()
    assert ws == tmp_path / "new_ws"


def test_socket_and_db_paths(tmp_path):
    cfg = AgentDConfig(workspace=str(tmp_path))
    assert cfg.socket_path == tmp_path / "agentd.sock"
    assert cfg.db_path == tmp_path / "agentd.db"
    assert cfg.pid_path == tmp_path / "agentd.pid"


# ---------------------------------------------------------------------------
# Channels
# ---------------------------------------------------------------------------


def test_channels_parsing(tmp_path, monkeypatch):
    monkeypatch.setenv("MY_TOKEN", "secret123")
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(
        """\
channels:
  telegram:
    env:
      BOT_TOKEN: ${MY_TOKEN}
      USERS: "42"
  custom:
    command: ["python", "/path/to/bot.py"]
  disabled:
    command: ["echo", "nope"]
    enabled: false
""",
        encoding="utf-8",
    )

    cfg = AgentDConfig.load(str(cfg_file))
    assert "telegram" in cfg.channels
    assert cfg.channels["telegram"].command is None  # built-in
    assert cfg.channels["telegram"].env["BOT_TOKEN"] == "secret123"
    assert cfg.channels["telegram"].env["USERS"] == "42"
    assert cfg.channels["telegram"].enabled is True
    assert cfg.channels["custom"].command == ["python", "/path/to/bot.py"]
    assert cfg.channels["disabled"].enabled is False


def test_channels_env_var_missing(tmp_path, monkeypatch):
    monkeypatch.delenv("NONEXISTENT_VAR", raising=False)
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(
        """\
channels:
  test:
    command: ["echo"]
    env:
      VAR: ${NONEXISTENT_VAR}
""",
        encoding="utf-8",
    )

    cfg = AgentDConfig.load(str(cfg_file))
    assert cfg.channels["test"].env["VAR"] == ""
