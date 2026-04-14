"""Channel supervisor — launches and restarts channel adapter processes."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import sys
from pathlib import Path

from agentd.channels import BUILTIN_CHANNELS
from agentd.config import ChannelConfig

logger = logging.getLogger("agentd.channels")

HEALTHY_THRESHOLD_S = 30  # reset backoff after running this long
MAX_BACKOFF_S = 60


class ChannelSupervisor:
    """Supervise channel adapter subprocesses with restart-on-crash."""

    def __init__(self, channels: dict[str, ChannelConfig]):
        self._channels = {n: c for n, c in channels.items() if c.enabled}
        self._tasks: dict[str, asyncio.Task] = {}

    async def start(self) -> None:
        for name, cfg in self._channels.items():
            cmd = self._resolve_command(name, cfg)
            self._tasks[name] = asyncio.create_task(
                self._supervise(name, cfg), name=f"channel:{name}"
            )
            logger.info("channel %s started: %s", name, " ".join(cmd))

    async def stop(self) -> None:
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()

    @staticmethod
    def _resolve_command(name: str, cfg: ChannelConfig) -> list[str]:
        if cfg.command:
            return cfg.command
        if name in BUILTIN_CHANNELS:
            return [sys.executable, "-m", f"agentd.channels.{name}"]
        msg = f"unknown built-in channel: {name!r} (use 'command' for custom channels)"
        raise ValueError(msg)

    async def _supervise(self, name: str, cfg: ChannelConfig) -> None:
        cmd = self._resolve_command(name, cfg)
        backoff = 1
        while True:
            proc: asyncio.subprocess.Process | None = None
            try:
                env = {**os.environ, **cfg.env}
                # Ensure channel subprocess can find the agentd CLI.
                # agentd is installed next to sys.executable (same venv bin/).
                if "AGENTD_BIN" not in env:
                    candidate = Path(sys.executable).parent / "agentd"
                    if candidate.exists():
                        env["AGENTD_BIN"] = str(candidate)
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    env=env,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )
                # Stream output with channel prefix
                log_task = asyncio.create_task(self._pipe_output(name, proc))
                start = asyncio.get_event_loop().time()
                await proc.wait()
                log_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await log_task

                elapsed = asyncio.get_event_loop().time() - start

                if proc.returncode == 0:
                    logger.info("channel %s exited with 0, restarting in 1s", name)
                    await asyncio.sleep(1)
                    backoff = 1
                    continue

                if elapsed > HEALTHY_THRESHOLD_S:
                    backoff = 1

                logger.warning(
                    "channel %s exited with %d, restarting in %ds",
                    name,
                    proc.returncode,
                    backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_S)

            except asyncio.CancelledError:
                if proc and proc.returncode is None:
                    proc.send_signal(signal.SIGTERM)
                    try:
                        await asyncio.wait_for(proc.wait(), timeout=5)
                    except TimeoutError:
                        proc.kill()
                        await proc.wait()
                logger.info("channel %s stopped", name)
                return
            except Exception:
                logger.exception("channel %s supervisor error", name)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_S)

    @staticmethod
    async def _pipe_output(name: str, proc: asyncio.subprocess.Process) -> None:
        assert proc.stdout is not None
        prefix = f"[{name}] "
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").rstrip()
            if not text:
                continue
            if text.startswith(prefix):
                logger.info("%s", text)
            else:
                logger.info("[%s] %s", name, text)
