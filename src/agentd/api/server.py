"""agentd daemon — Unix socket JSON-RPC 2.0 server.

Wires together Store, Scheduler, Runtime, and serves
RPC requests over a Unix domain socket.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal

from agentd.config import AgentDConfig
from agentd.protocol import (
    AGENTD_FRAME_MAX,
    INTERNAL_ERROR,
    INVALID_REQUEST,
    PARSE_ERROR,
    RpcResponse,
    make_error,
)
from agentd.runtime.backends import ClaudeAdapter, CodexAdapter, PiAdapter
from agentd.runtime.runner import Runtime
from agentd.scheduler.cron import run_cron_loop
from agentd.scheduler.scheduler import Scheduler
from agentd.store import Store
from agentd.store.db import Database

from .methods import MethodDispatcher

logger = logging.getLogger(__name__)


class Daemon:
    """Main daemon process that owns all components."""

    def __init__(self, config: AgentDConfig):
        self.config = config
        self._server: asyncio.AbstractServer | None = None
        self._cron_task: asyncio.Task | None = None
        self._http_task: asyncio.Task | None = None
        self._channel_supervisor = None
        self._shutdown_event = asyncio.Event()

        # Wire components
        config.resolve_workspace()  # Ensure workspace dir exists
        db = Database(config.db_path)
        self.store = Store(db)
        self.scheduler = Scheduler(self.store, config)
        self.runtime = Runtime(self.store, config, self.scheduler)
        self.scheduler.set_runtime(self.runtime)

        # Register backend adapters
        self.runtime.register_backend(PiAdapter())
        self.runtime.register_backend(ClaudeAdapter())
        self.runtime.register_backend(CodexAdapter())

        self.dispatcher = MethodDispatcher(self.scheduler, self.store, config)

    async def run(self) -> None:
        """Start daemon and run until shutdown signal."""
        self.store.initialize()
        logger.info("database initialized at %s", self.config.db_path)

        # Reconcile stale state
        await self.scheduler.reconcile()

        # Write PID file
        pid_path = self.config.pid_path
        pid_path.write_text(str(os.getpid()))

        # Start Unix socket server
        sock_path = self.config.socket_path
        if sock_path.exists():
            sock_path.unlink()

        self._server = await asyncio.start_unix_server(
            self._handle_connection,
            path=str(sock_path),
            limit=AGENTD_FRAME_MAX,
        )
        os.chmod(str(sock_path), 0o600)
        logger.info("listening on %s", sock_path)

        # Start cron loop
        self._cron_task = asyncio.create_task(run_cron_loop(self.store, self.scheduler))

        # Start HTTP inbox if enabled
        if self.config.inbox_gateway.enabled:
            from .http_gateway import run_http_gateway

            self._http_task = asyncio.create_task(run_http_gateway(self.scheduler, self.config))

        # Start channel adapters
        if self.config.channels:
            from agentd.channels.supervisor import ChannelSupervisor

            self._channel_supervisor = ChannelSupervisor(self.config.channels)
            await self._channel_supervisor.start()

        # Install signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._signal_shutdown)

        logger.info("daemon ready (pid=%d)", os.getpid())
        await self._shutdown_event.wait()
        await self._shutdown()

    def _signal_shutdown(self) -> None:
        logger.info("shutdown signal received")
        self._shutdown_event.set()

    async def _shutdown(self) -> None:
        """Graceful shutdown sequence."""
        logger.info("shutting down...")

        # 1. Stop accepting new connections
        if self._server:
            self._server.close()
            await self._server.wait_closed()

        # 2. Stop cron
        if self._cron_task:
            self._cron_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cron_task

        # 3. Stop channel adapters
        if self._channel_supervisor:
            await self._channel_supervisor.stop()

        # 4. Stop HTTP gateway
        if self._http_task:
            self._http_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._http_task

        # 5. Stop all running turns
        await self.runtime.stop_all()

        # 6. Close store
        self.store.db.close()

        # 7. Clean up files
        try:
            self.config.socket_path.unlink(missing_ok=True)
            self.config.pid_path.unlink(missing_ok=True)
        except OSError:
            pass

        logger.info("shutdown complete")

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle one RPC client connection (NDJSON protocol)."""
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").strip()
                if not text:
                    continue

                # Parse request
                try:
                    raw = json.loads(text)
                except json.JSONDecodeError:
                    resp = make_error("0", PARSE_ERROR, "invalid JSON")
                    await _write_response(writer, resp)
                    continue

                if not isinstance(raw, dict):
                    resp = make_error("0", INVALID_REQUEST, "request must be object")
                    await _write_response(writer, resp)
                    continue

                req_id = str(raw.get("id", "0"))
                method = raw.get("method")
                if not isinstance(method, str):
                    resp = make_error(req_id, INVALID_REQUEST, "missing method")
                    await _write_response(writer, resp)
                    continue

                params = raw.get("params", {})
                if not isinstance(params, dict):
                    params = {}

                # Dispatch
                try:
                    await self.dispatcher.dispatch(req_id, method, params, writer)
                except Exception:
                    logger.exception("error handling %s", method)
                    resp = make_error(req_id, INTERNAL_ERROR, "internal error")
                    await _write_response(writer, resp)
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception:
            logger.exception("connection error")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass


async def _write_response(writer: asyncio.StreamWriter, resp: RpcResponse) -> None:
    data = resp.model_dump(exclude_none=True)
    line = json.dumps(data, ensure_ascii=False) + "\n"
    writer.write(line.encode("utf-8"))
    await writer.drain()
