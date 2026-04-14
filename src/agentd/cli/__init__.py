"""agentd CLI — actor-first command-line interface."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
from typing import Any

from agentd.config import AgentDConfig

from .client import RpcClient


def main() -> None:
    parser = argparse.ArgumentParser(prog="agentd", description="Agent Daemon CLI")
    parser.add_argument("-c", "--config", help="Config file path")
    sub = parser.add_subparsers(dest="command")

    # -- spawn --
    sp = sub.add_parser("spawn", help="Create a new actor")
    sp.add_argument("--name", "-n", help="Actor name (optional)")
    sp.add_argument("--backend", "-b", help="Backend adapter")
    sp.add_argument("--parent-actor-id", help="Parent actor ID (child actor)")
    sp.add_argument("--message", "-m", help="Initial message text")
    sp.add_argument("--type", "-t", dest="msg_type", help="Message type")
    sp.add_argument("--payload", help="Message payload (JSON)")
    sp.add_argument("--env", "-e", action="append", default=[], help="Env var (KEY=VALUE)")
    sp.add_argument("--cwd", help="Working directory")
    sp.add_argument("--checkpoint", type=_parse_bool, default=None, help="Enable checkpoint")
    sp.add_argument("backend_args", nargs="*", help="Backend-specific args (after --)")

    # -- emit --
    em = sub.add_parser("emit", help="Send message to an actor")
    em.add_argument("actor", help="Actor reference (id or name)")
    em.add_argument("--message", "-m", help="Message text")
    em.add_argument("--type", "-t", dest="msg_type", help="Message type")
    em.add_argument("--payload", help="Message payload (JSON)")
    em.add_argument("--env", "-e", action="append", default=[], help="Env var (KEY=VALUE)")
    em.add_argument(
        "--deliver-as",
        choices=["auto", "steer", "follow_up"],
        default="auto",
        help="Delivery mode (default: auto)",
    )

    # -- stop --
    st = sub.add_parser("stop", help="Stop or close an actor")
    st.add_argument("actor", help="Actor reference (id or name)")
    st.add_argument("--close", action="store_true", help="Hard close actor + subtree")

    # -- wait --
    wa = sub.add_parser("wait", help="Wait for actor to reach idle/closed")
    wa.add_argument("actor", help="Actor reference (id or name)")
    wa.add_argument("--timeout", type=float, help="Timeout in seconds")
    wa.add_argument("--progress", action="store_true", help="Stream progress events")
    wa.add_argument("--since-seq", type=int, default=0, help="Start from event seq")

    # -- ps --
    ps = sub.add_parser("ps", help="List actors")
    ps.add_argument("--all", action="store_true", help="Include closed actors")
    ps.add_argument("--watch", action="store_true", help="Watch for changes")
    ps.add_argument("--limit", type=int, default=200, help="Max actors to show")

    # -- logs --
    lo = sub.add_parser("logs", help="View actor logs")
    lo.add_argument("actor", help="Actor reference (id or name)")
    lo.add_argument("--follow", "-f", action="store_true", help="Follow log stream")
    lo.add_argument("--since-seq", type=int, default=0, help="Start from event seq")
    lo.add_argument("--limit", type=int, default=200, help="Max events to show")

    # -- status --
    sta = sub.add_parser("status", help="Actor or daemon status")
    sta.add_argument("actor", nargs="?", help="Actor reference (omit for daemon status)")
    sta.add_argument("--events", action="store_true", help="Include events")
    sta.add_argument("--result", action="store_true", help="Include last result")

    # -- trigger --
    tr = sub.add_parser("trigger", help="Manage triggers")
    tr_sub = tr.add_subparsers(dest="trigger_cmd")
    tr_add = tr_sub.add_parser("add", help="Add a cron trigger")
    tr_add.add_argument("actor", help="Actor reference (id or name)")
    tr_add.add_argument("--schedule", required=True, help="Cron expression (5-field)")
    tr_add.add_argument("--type", "-t", dest="msg_type", required=True)
    tr_add.add_argument("--payload", default="{}")
    tr_ls = tr_sub.add_parser("ls", help="List triggers")
    tr_ls.add_argument("actor", nargs="?", help="Actor reference (optional filter)")
    tr_rm = tr_sub.add_parser("rm", help="Remove a trigger")
    tr_rm.add_argument("trigger_id")

    # -- init --
    sub.add_parser("init", help="Initialize config, skills, and AGENTS.md")

    # -- serve --
    sv = sub.add_parser("serve", help="Run daemon in foreground")
    sv.add_argument("--verbose", "-v", action="store_true", help="Debug logging")

    # -- doctor --
    doc = sub.add_parser("doctor", help="Diagnose and fix issues")
    doc.add_argument("--fix", action="store_true", help="Auto-fix issues")

    # -- service --
    svc = sub.add_parser("service", help="Manage system service")
    svc_sub = svc.add_subparsers(dest="service_cmd")
    svc_sub.add_parser("install", help="Install as system service")
    svc_sub.add_parser("uninstall", help="Uninstall system service")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    try:
        asyncio.run(_dispatch(args))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        _output_error(str(e))
        sys.exit(1)


async def _dispatch(args: argparse.Namespace) -> None:
    cmd = args.command
    config = AgentDConfig.load(getattr(args, "config", None))

    if cmd == "init":
        _cmd_init(config)
        return

    if cmd == "serve":
        await _cmd_serve(args, config)
        return

    if cmd == "service":
        _cmd_service(args, config)
        return

    # All other commands need RPC client
    client = RpcClient(config.socket_path)

    if cmd == "spawn":
        await _cmd_spawn(args, client)
    elif cmd == "emit":
        await _cmd_emit(args, client)
    elif cmd == "stop":
        await _cmd_stop(args, client)
    elif cmd == "wait":
        await _cmd_wait(args, client)
    elif cmd == "ps":
        await _cmd_ps(args, client)
    elif cmd == "logs":
        await _cmd_logs(args, client)
    elif cmd == "status":
        await _cmd_status(args, client)
    elif cmd == "trigger":
        await _cmd_trigger(args, client)
    elif cmd == "doctor":
        await _cmd_doctor(args, client)


# ------------------------------------------------------------------
# Command implementations
# ------------------------------------------------------------------


def _cmd_init(config: AgentDConfig) -> None:
    """Initialize ~/.config/agentd/ with config, skills, and AGENTS.md."""
    import shutil
    from importlib.resources import files
    from pathlib import Path

    config_dir = Path(os.environ.get("XDG_CONFIG_HOME", "~/.config")).expanduser() / "agentd"
    config_dir.mkdir(parents=True, exist_ok=True)

    data_root = files("agentd._data")
    created: list[str] = []

    # 1. config.yaml
    config_dest = config_dir / "config.yaml"
    if not config_dest.exists():
        src = data_root.joinpath("config.example.yaml")
        if src.is_file():
            config_dest.write_text(src.read_text())
        else:
            # Fallback: try repo-level config.example.yaml via package
            example = Path(__file__).resolve().parents[3] / "config.example.yaml"
            if example.exists():
                shutil.copy2(example, config_dest)
        if config_dest.exists():
            created.append("  config.yaml")
    else:
        print("  config.yaml (exists, skipped)")

    # 2. AGENTS.md
    agents_dest = config_dir / "AGENTS.md"
    if not agents_dest.exists():
        src = data_root.joinpath("agents.md")
        agents_dest.write_text(src.read_text())
        created.append("  AGENTS.md")
    else:
        print("  AGENTS.md (exists, skipped)")

    # 3. .env
    env_dest = config_dir / ".env"
    if not env_dest.exists():
        env_dest.write_text(
            "# agentd secrets\n"
            "# Loaded automatically by agentd serve.\n"
            "# Shell environment takes precedence over values here.\n"
            "\n"
            "# TELEGRAM_BOT_TOKEN=\n"
            "# TELEGRAM_ALLOWED_USERS=\n"
        )
        created.append("  .env")
    else:
        print("  .env (exists, skipped)")

    # 4. Skills
    skills_dest = config_dir / "skills"
    skills_src = data_root.joinpath("skills")
    for skill_dir in sorted(skills_src.iterdir()):
        if not skill_dir.is_dir() or skill_dir.name.startswith("_"):
            continue
        skill_md = skill_dir.joinpath("SKILL.md")
        if not skill_md.is_file():
            continue
        dest_dir = skills_dest / skill_dir.name
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_file = dest_dir / "SKILL.md"
        dest_file.write_text(skill_md.read_text())
        created.append(f"  skills/{skill_dir.name}/SKILL.md")

    # 5. agents/ dir
    agents_dir = config_dir / "agents"
    agents_dir.mkdir(parents=True, exist_ok=True)

    if created:
        print(f"Initialized {config_dir}/")
        for item in created:
            print(item)
    else:
        print(f"Already initialized: {config_dir}/")

    # Install system service
    import platform

    system = platform.system()
    if system == "Darwin":
        _install_launchd(config)
    elif system == "Linux":
        _install_systemd(config)

    # Hint for Pi integration
    if shutil.which("pi"):
        print()
        print("To register agentd skills with Pi:")
        print("  pi install git:github.com/tshu-w/agentd")


async def _cmd_serve(args: argparse.Namespace, config: AgentDConfig) -> None:
    import logging

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    # Load .env before re-resolving config so ${VAR} picks up secrets.
    _load_dotenv(config)
    config = AgentDConfig.load(config.path)

    from agentd.api.server import Daemon

    daemon = Daemon(config)
    await daemon.run()


def _load_dotenv(config: AgentDConfig) -> None:
    """Load ~/.config/agentd/.env into os.environ (before config ${VAR} resolution)."""
    from pathlib import Path

    if config.path:
        env_file = Path(config.path).parent / ".env"
    else:
        xdg = os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))
        env_file = Path(xdg) / "agentd" / ".env"
    if not env_file.exists():
        return
    with open(env_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            # Strip surrounding quotes
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            if key:
                os.environ.setdefault(key, value)


async def _cmd_spawn(args: argparse.Namespace, client: RpcClient) -> None:
    params: dict[str, Any] = {}
    if args.name:
        params["name"] = args.name
    if args.backend:
        params["backend"] = args.backend
    if args.parent_actor_id:
        params["parent_actor_id"] = args.parent_actor_id
    if args.message:
        params["message"] = args.message
    elif args.msg_type:
        params["type"] = args.msg_type
        params["payload"] = json.loads(args.payload) if args.payload else {}
    if args.env:
        params["env"] = _parse_env_list(args.env)
    if args.cwd:
        params["cwd"] = args.cwd
    elif sys.stdout.isatty():
        # Auto-fill cwd from caller's PWD (interactive use only)
        params["cwd"] = os.getcwd()
    if args.checkpoint is not None:
        params["checkpoint"] = args.checkpoint
    if args.backend_args:
        params["backend_args"] = args.backend_args

    result = await client.call("actor.spawn", params)
    _output(result)


async def _cmd_emit(args: argparse.Namespace, client: RpcClient) -> None:
    params: dict[str, Any] = {"actor": args.actor}
    if args.message:
        params["message"] = args.message
    elif args.msg_type:
        params["type"] = args.msg_type
        params["payload"] = json.loads(args.payload) if args.payload else {}
    if args.env:
        params["env"] = _parse_env_list(args.env)
    params["deliver_as"] = args.deliver_as

    result = await client.call("actor.emit", params)
    _output(result)


async def _cmd_stop(args: argparse.Namespace, client: RpcClient) -> None:
    if args.close:
        result = await client.call("actor.close", {"actor": args.actor})
    else:
        result = await client.call("actor.stop", {"actor": args.actor})
    _output(result)


async def _cmd_wait(args: argparse.Namespace, client: RpcClient) -> None:
    params: dict[str, Any] = {
        "actor": args.actor,
        "progress": args.progress,
        "since_seq": args.since_seq,
    }
    if args.timeout:
        params["timeout"] = args.timeout

    if args.progress:
        # Streaming mode
        last_result = None
        async for frame in client.stream("actor.wait", params):
            if "event" in frame:
                _output_line(frame["event"])
            elif "result" in frame:
                last_result = frame["result"]
        if last_result is not None:
            _output(last_result)
    else:
        result = await client.call("actor.wait", params)
        _output(result)


async def _cmd_ps(args: argparse.Namespace, client: RpcClient) -> None:
    params: dict[str, Any] = {
        "include_terminal": args.all,
        "limit": args.limit,
    }
    if args.watch:
        params["watch"] = True
        async for frame in client.stream("actor.list", params):
            if "event" in frame:
                _output_ps_snapshot(frame["event"])
    else:
        result = await client.call("actor.list", params)
        if _is_tty():
            _output_ps_table(result.get("actors", []))
        else:
            _output(result)


async def _cmd_logs(args: argparse.Namespace, client: RpcClient) -> None:
    params: dict[str, Any] = {
        "actor": args.actor,
        "since_seq": args.since_seq,
        "limit": args.limit,
    }
    if args.follow:
        params["follow"] = True
        async for frame in client.stream("actor.logs", params):
            if "event" in frame:
                _output_line(frame["event"])
    else:
        result = await client.call("actor.logs", params)
        if _is_tty():
            for ev in result.get("events", []):
                _output_event_tty(ev)
        else:
            _output(result)


async def _cmd_status(args: argparse.Namespace, client: RpcClient) -> None:
    if args.actor:
        params: dict[str, Any] = {
            "actor": args.actor,
            "include_events": args.events,
            "include_result": args.result,
        }
        result = await client.call("actor.status", params)
    else:
        result = await client.call("daemon.status", {})
    _output(result)


async def _cmd_trigger(args: argparse.Namespace, client: RpcClient) -> None:
    if args.trigger_cmd == "add":
        result = await client.call(
            "trigger.add",
            {
                "actor": args.actor,
                "schedule": args.schedule,
                "type": args.msg_type,
                "payload": json.loads(args.payload),
            },
        )
    elif args.trigger_cmd == "ls":
        result = await client.call(
            "trigger.ls",
            {
                "actor": args.actor,
            },
        )
    elif args.trigger_cmd == "rm":
        result = await client.call(
            "trigger.rm",
            {
                "trigger_id": args.trigger_id,
            },
        )
    else:
        print("Usage: agentd trigger {add|ls|rm}", file=sys.stderr)
        sys.exit(1)
        return
    _output(result)


async def _cmd_doctor(args: argparse.Namespace, client: RpcClient) -> None:
    # Local checks first (don't need daemon running)
    config = AgentDConfig.load(None)
    local_issues = _doctor_local_checks(config, fix=args.fix)
    for issue in local_issues:
        print(issue)

    # Remote checks (need daemon)
    try:
        result = await client.call("daemon.doctor", {"fix": args.fix})
        _output(result)
    except ConnectionError as e:
        if local_issues:
            print(f"\ndaemon not running ({e})")
            print("Local issues reported above. Run with --fix to repair.")
        else:
            print(f"daemon not running ({e})")
            print("No local issues found.")


def _doctor_local_checks(config: AgentDConfig, *, fix: bool) -> list[str]:
    """Check for issues that don't require a running daemon."""
    issues: list[str] = []
    sock = config.socket_path
    pid_file = config.pid_path

    if sock.exists():
        # Socket exists — check if daemon is actually alive
        alive = False
        if pid_file.exists():
            try:
                pid = int(pid_file.read_text().strip())
                os.kill(pid, 0)
                alive = True
            except (ValueError, OSError):
                pass

        if not alive:
            issues.append(f"stale socket: {sock}")
            if pid_file.exists():
                issues.append(f"stale pid file: {pid_file}")
            if fix:
                sock.unlink(missing_ok=True)
                pid_file.unlink(missing_ok=True)
                issues.append("  → cleaned up stale files")

    return issues


def _cmd_service(args: argparse.Namespace, config: AgentDConfig) -> None:
    import platform

    system = platform.system()
    if args.service_cmd == "install":
        if system == "Darwin":
            _install_launchd(config)
        elif system == "Linux":
            _install_systemd(config)
        else:
            print(f"Service install not supported on {system}", file=sys.stderr)
            sys.exit(1)
    elif args.service_cmd == "uninstall":
        if system == "Darwin":
            _uninstall_launchd()
        elif system == "Linux":
            _uninstall_systemd()
        else:
            print(f"Service uninstall not supported on {system}", file=sys.stderr)
            sys.exit(1)
    else:
        print("Usage: agentd service {install|uninstall}", file=sys.stderr)
        sys.exit(1)


# ------------------------------------------------------------------
# Output helpers
# ------------------------------------------------------------------


def _is_tty() -> bool:
    return sys.stdout.isatty()


def _output(data: dict[str, Any]) -> None:
    if _is_tty():
        _output_tty(data)
    else:
        print(json.dumps({"ok": True, **data}, ensure_ascii=False))


def _output_error(msg: str, code: str = "error") -> None:
    if _is_tty():
        print(f"error: {msg}", file=sys.stderr)
    else:
        err = {"ok": False, "error": {"code": code, "message": msg}}
        print(json.dumps(err, ensure_ascii=False))


def _output_line(data: dict[str, Any]) -> None:
    """Output a single streaming line."""
    print(json.dumps(data, ensure_ascii=False), flush=True)


def _output_tty(data: dict[str, Any]) -> None:
    """Pretty-print for TTY."""
    # Simple key=value format for now
    for k, v in data.items():
        if isinstance(v, dict):
            print(f"{k}:")
            for k2, v2 in v.items():
                print(f"  {k2}: {v2}")
        elif isinstance(v, list):
            print(f"{k}: [{len(v)} items]")
        else:
            print(f"{k}: {v}")


def _output_ps_table(actors: list[dict]) -> None:
    if not actors:
        print("No actors.")
        return
    # Simple table
    print(f"{'ACTOR_ID':<20} {'NAME':<20} {'STATE':<8} {'BACKEND':<10}")
    print("-" * 60)
    for a in actors:
        name = a.get("name") or "-"
        print(f"{a['actor_id']:<20} {name:<20} {a['state']:<8} {a['backend']:<10}")


def _output_ps_snapshot(data: dict) -> None:
    """Output ps --watch snapshot."""
    actors = data.get("actors", [])
    # Clear and reprint
    print("\033[2J\033[H", end="")
    _output_ps_table(actors)


def _output_event_tty(ev: dict) -> None:
    ts = (ev.get("created_at") or "")[:19]
    etype = ev.get("event_type", "?")
    payload = ev.get("payload", {})

    if etype == "turn.progress":
        ptype = payload.get("type", "")
        if ptype == "text":
            print(f"{ts} {payload.get('content', '')}", end="")
        elif ptype == "thinking":
            print(f"{ts} [thinking] {payload.get('content', '')[:80]}")
        elif ptype == "tool_call":
            status = payload.get("status", "")
            name = payload.get("name", "")
            print(f"{ts} [{status}] {name}")
    elif etype == "turn.end":
        outcome = payload.get("outcome", "")
        result = payload.get("result", "")
        print(f"\n{ts} [{etype}] {outcome}")
        if result:
            print(result)
    else:
        print(f"{ts} [{etype}]")


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _parse_env_list(env_args: list[str]) -> dict[str, str]:
    env = {}
    for item in env_args:
        key, _, val = item.partition("=")
        if not key:
            continue
        env[key] = val
    return env


def _parse_bool(val: str) -> bool:
    return val.lower() in ("true", "1", "yes")


def _install_launchd(config: AgentDConfig) -> None:
    import shutil

    agentd_bin = shutil.which("agentd")
    if not agentd_bin:
        print("agentd not found in PATH", file=sys.stderr)
        sys.exit(1)

    label = "dev.agentd.daemon"
    plist_path = os.path.expanduser(f"~/Library/LaunchAgents/{label}.plist")
    log_dir = config.resolve_workspace()

    # Snapshot system env vars so launchd inherits them.
    # Secrets (API keys) belong in ~/.config/agentd/.env, not here.
    _ENV_SNAPSHOT_KEYS = [
        "PATH",
        "HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_STATE_HOME",
        "PI_CODING_AGENT_DIR",
        "CLAUDE_CODE_CONFIG_DIR",
        "CODEX_HOME",
        "AGENTD_CONFIG",
        "AGENTD_WORKSPACE",
    ]
    env_entries = ""
    for key in _ENV_SNAPSHOT_KEYS:
        val = os.environ.get(key)
        if val:
            safe = val.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            env_entries += f"        <key>{key}</key>\n        <string>{safe}</string>\n"
    env_block = ""
    if env_entries:
        env_block = f"    <key>EnvironmentVariables</key>\n    <dict>\n{env_entries}    </dict>\n"

    plist = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{agentd_bin}</string>
        <string>serve</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
{env_block}    <key>StandardOutPath</key>
    <string>{log_dir}/agentd.log</string>
    <key>StandardErrorPath</key>
    <string>{log_dir}/agentd.log</string>
</dict>
</plist>"""
    os.makedirs(os.path.dirname(plist_path), exist_ok=True)
    with open(plist_path, "w") as f:
        f.write(plist)
    subprocess.run(["launchctl", "load", str(plist_path)], check=False)
    print(f"Service installed: {plist_path}")
    print(f"Loaded: launchctl load {plist_path}")


def _uninstall_launchd() -> None:
    label = "dev.agentd.daemon"
    plist_path = os.path.expanduser(f"~/Library/LaunchAgents/{label}.plist")
    if os.path.exists(plist_path):
        subprocess.run(["launchctl", "unload", str(plist_path)], check=False)
        os.remove(plist_path)
        print(f"Service uninstalled: {plist_path}")
    else:
        print("Service not installed.", file=sys.stderr)


def _install_systemd(config: AgentDConfig) -> None:
    import shutil

    agentd_bin = shutil.which("agentd")
    if not agentd_bin:
        print("agentd not found in PATH", file=sys.stderr)
        sys.exit(1)

    unit_dir = os.path.expanduser("~/.config/systemd/user")
    unit_path = os.path.join(unit_dir, "agentd.service")

    # Collect env vars for the unit (same keys as launchd).
    _ENV_SNAPSHOT_KEYS = [
        "PATH",
        "HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_STATE_HOME",
        "PI_CODING_AGENT_DIR",
        "CLAUDE_CODE_CONFIG_DIR",
        "CODEX_HOME",
        "AGENTD_CONFIG",
        "AGENTD_WORKSPACE",
    ]
    env_lines = ""
    for key in _ENV_SNAPSHOT_KEYS:
        val = os.environ.get(key)
        if val:
            env_lines += f"Environment={key}={val}\n"

    unit = f"""\
[Unit]
Description=agentd daemon
After=network.target

[Service]
Type=simple
ExecStart={agentd_bin} serve
Restart=on-failure
RestartSec=5
{env_lines}
[Install]
WantedBy=default.target
"""
    os.makedirs(unit_dir, exist_ok=True)
    with open(unit_path, "w") as f:
        f.write(unit)
    subprocess.run(["systemctl", "--user", "daemon-reload"], check=False)
    subprocess.run(["systemctl", "--user", "enable", "--now", "agentd"], check=False)
    print(f"Service installed: {unit_path}")
    print("Enabled and started: systemctl --user enable --now agentd")


def _uninstall_systemd() -> None:
    unit_path = os.path.expanduser("~/.config/systemd/user/agentd.service")
    if os.path.exists(unit_path):
        subprocess.run(["systemctl", "--user", "disable", "--now", "agentd"], check=False)
        os.remove(unit_path)
        subprocess.run(["systemctl", "--user", "daemon-reload"], check=False)
        print(f"Service uninstalled: {unit_path}")
    else:
        print("Service not installed.", file=sys.stderr)
