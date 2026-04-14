"""RPC client for communicating with the agentd daemon over Unix socket."""

from __future__ import annotations

import asyncio
import contextlib
import json
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any


class RpcError(Exception):
    """Error returned by the daemon."""

    def __init__(self, code: int, message: str, data: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.data = data or {}
        self.error_type = self.data.get("type", "unknown")


class RpcClient:
    """JSON-RPC 2.0 client over Unix domain socket."""

    def __init__(self, socket_path: Path):
        self.socket_path = socket_path

    async def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """Make a single RPC call and return the result."""
        reader, writer = await self._connect()
        try:
            req_id = uuid.uuid4().hex[:8]
            request = {
                "jsonrpc": "2.0",
                "id": req_id,
                "method": method,
                "params": params,
            }
            line = json.dumps(request, ensure_ascii=False) + "\n"
            writer.write(line.encode("utf-8"))
            await writer.drain()

            # Read response
            resp_line = await reader.readline()
            if not resp_line:
                raise ConnectionError("no response from daemon")

            resp = json.loads(resp_line.decode("utf-8"))
            if "error" in resp and resp["error"]:
                err = resp["error"]
                raise RpcError(
                    err.get("code", -1),
                    err.get("message", "unknown"),
                    err.get("data"),
                )
            return resp.get("result", {})
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Connect to daemon, with friendly errors for common failures."""
        if not self.socket_path.exists():
            raise ConnectionError(
                f"daemon not running (socket not found: {self.socket_path})\n"
                "Start with: agentd serve"
            )
        try:
            return await asyncio.open_unix_connection(
                str(self.socket_path),
                limit=4 * 1024 * 1024,  # 4MB line limit
            )
        except ConnectionRefusedError:
            raise ConnectionError(
                f"daemon not responding (stale socket: {self.socket_path})\n"
                f"Try: agentd doctor --fix  or  rm {self.socket_path} && agentd serve"
            ) from None
        except OSError as e:
            raise ConnectionError(
                f"cannot connect to daemon: {e}\nStart with: agentd serve"
            ) from None

    async def stream(
        self,
        method: str,
        params: dict[str, Any],
    ) -> AsyncIterator[dict[str, Any]]:
        """Make a streaming RPC call, yielding frames until done."""
        reader, writer = await self._connect()
        try:
            req_id = uuid.uuid4().hex[:8]
            request = {
                "jsonrpc": "2.0",
                "id": req_id,
                "method": method,
                "params": params,
            }
            line = json.dumps(request, ensure_ascii=False) + "\n"
            writer.write(line.encode("utf-8"))
            await writer.drain()

            while True:
                resp_line = await reader.readline()
                if not resp_line:
                    break

                frame = json.loads(resp_line.decode("utf-8"))

                if "error" in frame and frame["error"]:
                    err = frame["error"]
                    raise RpcError(
                        err.get("code", -1),
                        err.get("message", "unknown"),
                        err.get("data"),
                    )

                yield frame

                # Check if this is the final frame
                if frame.get("done", False):
                    break
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
