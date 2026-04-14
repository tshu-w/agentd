"""Configuration loading for agentd.

Resolves YAML config (explicit path → AGENTD_CONFIG → XDG chain → ~/.agentd.yaml),
applies env overrides, and resolves workspace directory.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(slots=True)
class LimitsConfig:
    max_depth: int = 3
    max_children_per_parent: int = 8
    max_total_workers: int = 64


@dataclass(slots=True)
class InboxGatewayConfig:
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 8765
    public_base_url: str | None = None


@dataclass(slots=True)
class SpawnDefaults:
    backend: str | None = None
    cwd: str | None = None
    args: list[str] | None = None


@dataclass(slots=True)
class ChannelConfig:
    name: str
    command: list[str] | None = None  # None = built-in channel
    env: dict[str, str] = field(default_factory=dict)
    enabled: bool = True
    spawn: SpawnDefaults = field(default_factory=SpawnDefaults)


@dataclass(slots=True)
class AgentDConfig:
    limits: LimitsConfig = field(default_factory=LimitsConfig)
    default_backend: str = "pi"
    channels: dict[str, ChannelConfig] = field(default_factory=dict)
    inbox_gateway: InboxGatewayConfig = field(default_factory=InboxGatewayConfig)
    source: str = "default"
    path: str | None = None
    workspace: str | None = None

    @classmethod
    def load(cls, explicit_path: str | None = None) -> AgentDConfig:
        """Load config from explicit path or discovery chain."""
        path, source = _resolve_config_path(explicit_path)

        if path is None or not path.exists():
            cfg = cls(source=source)
            _apply_env_overrides(cfg.limits)
            cfg.workspace = _resolve_workspace(None)
            return cfg

        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(raw, dict):
            raw = {}

        limits = _parse_limits(raw.get("limits"))
        _apply_env_overrides(limits)

        return cls(
            limits=limits,
            default_backend=_str_or(raw.get("default_backend"), "pi"),
            channels=_parse_channels(raw.get("channels")),
            inbox_gateway=_parse_inbox(raw.get("inbox_gateway")),
            source=source,
            path=str(path),
            workspace=_resolve_workspace(raw.get("workspace")),
        )

    def resolve_workspace(self) -> Path:
        """Return the resolved workspace directory (creates if needed)."""
        if self.workspace:
            p = Path(self.workspace)
        else:
            xdg = os.environ.get("XDG_STATE_HOME", str(Path.home() / ".local" / "state"))
            p = Path(xdg) / "agentd"
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def socket_path(self) -> Path:
        return self.resolve_workspace() / "agentd.sock"

    @property
    def db_path(self) -> Path:
        return self.resolve_workspace() / "agentd.db"

    @property
    def pid_path(self) -> Path:
        return self.resolve_workspace() / "agentd.pid"


# ---------------------------------------------------------------------------
# Internal parsing helpers
# ---------------------------------------------------------------------------


def _resolve_config_path(explicit: str | None) -> tuple[Path | None, str]:
    if explicit is not None:
        return Path(explicit).expanduser(), "explicit"

    env_path = os.environ.get("AGENTD_CONFIG")
    if env_path:
        p = Path(env_path).expanduser()
        if p.exists():
            return p, "explicit"

    xdg = os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))
    candidates = [
        Path(xdg) / "agentd" / "config.yaml",
        Path.home() / ".config" / "agentd" / "config.yaml",
        Path.home() / ".agentd.yaml",
    ]
    for c in candidates:
        if c.exists():
            return c, "discovered"

    return None, "default"


def _str_or(val: Any, default: str) -> str:
    if isinstance(val, str) and val.strip():
        return val.strip()
    return default


def _parse_limits(raw: Any) -> LimitsConfig:
    if not isinstance(raw, dict):
        return LimitsConfig()
    return LimitsConfig(
        max_depth=int(raw.get("max_depth", 3)),
        max_children_per_parent=int(raw.get("max_children_per_parent", 8)),
        max_total_workers=int(raw.get("max_total_workers", 64)),
    )


def _resolve_env_ref(value: str) -> str:
    """Resolve ${VAR} references from os.environ."""
    return re.sub(r"\$\{(\w+)\}", lambda m: os.environ.get(m.group(1), ""), value)


def _parse_spawn_defaults(raw: Any) -> SpawnDefaults:
    if not isinstance(raw, dict):
        return SpawnDefaults()
    backend = raw.get("backend")
    cwd_raw = raw.get("cwd")
    cwd = (
        str(Path(cwd_raw).expanduser().resolve(strict=False))
        if isinstance(cwd_raw, str) and cwd_raw.strip()
        else None
    )
    args = [str(a) for a in raw["args"]] if isinstance(raw.get("args"), list) else None
    return SpawnDefaults(
        backend=str(backend) if isinstance(backend, str) else None,
        cwd=cwd,
        args=args,
    )


def _parse_channels(raw: Any) -> dict[str, ChannelConfig]:
    if not isinstance(raw, dict):
        return {}
    channels: dict[str, ChannelConfig] = {}
    for name, val in raw.items():
        if not isinstance(val, dict):
            val = {}
        cmd_raw = val.get("command")
        cmd = [str(c) for c in cmd_raw] if isinstance(cmd_raw, list) and cmd_raw else None
        env_raw = val.get("env", {})
        env = (
            {str(k): _resolve_env_ref(str(v)) for k, v in env_raw.items()}
            if isinstance(env_raw, dict)
            else {}
        )
        enabled = val.get("enabled", True)
        channels[str(name)] = ChannelConfig(
            name=str(name),
            command=cmd,
            env=env,
            enabled=bool(enabled),
            spawn=_parse_spawn_defaults(val.get("spawn")),
        )
    return channels


def _parse_inbox(raw: Any) -> InboxGatewayConfig:
    cfg = InboxGatewayConfig()
    if not isinstance(raw, dict):
        return cfg
    if raw.get("enabled") is not None:
        cfg.enabled = bool(raw["enabled"])
    if isinstance(raw.get("host"), str):
        cfg.host = raw["host"].strip()
    if raw.get("port") is not None:
        cfg.port = int(raw["port"])
    url = raw.get("public_base_url")
    if isinstance(url, str) and url.strip():
        cfg.public_base_url = url.strip().rstrip("/")
    return cfg


def _apply_env_overrides(limits: LimitsConfig) -> None:
    for attr, var in [
        ("max_depth", "AGENTD_LIMITS_MAX_DEPTH"),
        ("max_children_per_parent", "AGENTD_LIMITS_MAX_CHILDREN_PER_PARENT"),
        ("max_total_workers", "AGENTD_LIMITS_MAX_TOTAL_WORKERS"),
    ]:
        raw = os.environ.get(var)
        if raw is not None:
            try:
                setattr(limits, attr, int(raw.strip()))
            except ValueError:
                raise SystemExit(f"error: invalid {var}: {raw!r}") from None


def _resolve_workspace(raw: Any) -> str | None:
    env = os.environ.get("AGENTD_WORKSPACE")
    if env:
        return str(Path(env).expanduser().resolve(strict=False))
    if isinstance(raw, str) and raw.strip():
        return str(Path(raw).expanduser().resolve(strict=False))
    return None
