"""Optional HTTP inbox bridge.

POST /v1/actors/{actor_id}/inbox → actor.emit

Provider-agnostic. Supports Idempotency-Key header for best-effort in-memory webhook dedup.
"""

from __future__ import annotations

import contextlib
import json
import logging
from typing import TYPE_CHECKING

from agentd.config import AgentDConfig
from agentd.scheduler.scheduler import Scheduler, SchedulerError

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Dedup cache (simple in-memory, bounded)
_seen_keys: dict[str, bool] = {}
_MAX_SEEN = 10000


async def run_http_gateway(scheduler: Scheduler, config: AgentDConfig) -> None:
    """Run the HTTP inbox bridge using asyncio HTTP server."""
    import asyncio

    gw = config.inbox_gateway
    host = gw.host
    port = gw.port

    async def handle_request(reader, writer):
        try:
            # Read HTTP request line
            request_line = await reader.readline()
            if not request_line:
                writer.close()
                return

            parts = request_line.decode().strip().split()
            if len(parts) < 3:
                _send_response(writer, 400, {"error": "bad request"})
                return

            method_str, path, _ = parts[0], parts[1], parts[2]

            # Read headers
            headers = {}
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
                key, _, val = line.decode().partition(":")
                headers[key.strip().lower()] = val.strip()

            # Read body
            content_length = int(headers.get("content-length", "0"))
            body = b""
            if content_length > 0:
                body = await reader.readexactly(content_length)

            # Route
            if method_str == "POST" and path.startswith("/v1/actors/") and path.endswith("/inbox"):
                # Extract actor_id from path
                segments = path.split("/")
                # /v1/actors/{actor_id}/inbox → segments = ['', 'v1', 'actors', '{id}', 'inbox']
                if len(segments) >= 5:
                    actor_id = segments[3]
                    idempotency_key = headers.get("idempotency-key")

                    # Parse body BEFORE recording idempotency key
                    try:
                        payload = json.loads(body) if body else {}
                    except json.JSONDecodeError:
                        _send_response(writer, 400, {"error": "invalid JSON"})
                        return

                    # Dedup check (after body validation so bad requests don't consume keys)
                    if idempotency_key:
                        dedup_key = f"{actor_id}:{idempotency_key}"
                        if dedup_key in _seen_keys:
                            _send_response(writer, 200, {"ok": True, "deduplicated": True})
                            return

                    msg_type = payload.get("type", "message")
                    msg_payload = payload.get("payload", payload)

                    try:
                        result = await scheduler.emit(
                            actor_id=actor_id,
                            msg_type=msg_type,
                            msg_payload=msg_payload,
                        )
                        # Record dedup key only after successful emit
                        if idempotency_key:
                            _seen_keys[dedup_key] = True
                            if len(_seen_keys) > _MAX_SEEN:
                                # Evict oldest half
                                keys = list(_seen_keys.keys())
                                for k in keys[: len(keys) // 2]:
                                    _seen_keys.pop(k, None)
                        _send_response(writer, 200, result)
                    except SchedulerError as e:
                        status = 404 if e.error_type == "not_found" else 409
                        _send_response(
                            writer,
                            status,
                            {
                                "error": {"code": e.error_type, "message": str(e)},
                            },
                        )
                else:
                    _send_response(writer, 404, {"error": "not found"})
            elif method_str == "GET" and path == "/health":
                _send_response(writer, 200, {"ok": True})
            else:
                _send_response(writer, 404, {"error": "not found"})

        except Exception:
            logger.exception("HTTP gateway error")
            with contextlib.suppress(Exception):
                _send_response(writer, 500, {"error": "internal error"})
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    server = await asyncio.start_server(handle_request, host, port)
    logger.info("HTTP inbox gateway listening on %s:%d", host, port)
    async with server:
        await server.serve_forever()


def _send_response(writer, status: int, body: dict) -> None:
    import json as _json

    body_bytes = _json.dumps(body, ensure_ascii=False).encode("utf-8")
    from http import HTTPStatus

    reason = HTTPStatus(status).phrase
    header = (
        f"HTTP/1.1 {status} {reason}\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(body_bytes)}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    )
    writer.write(header.encode("utf-8") + body_bytes)
