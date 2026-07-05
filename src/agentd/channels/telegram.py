"""Telegram channel adapter for agentd.

Polls Telegram for updates, emits events to agentd, and streams progress
back as Telegram messages.

    python -m agentd.channels.telegram

Requires env vars:
    TELEGRAM_BOT_TOKEN
    TELEGRAM_ALLOWED_USERS
"""

import asyncio
import contextlib
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import aiohttp

from agentd.channels.lib import (
    WaitResult,
    agentd_exec,
    agentd_spawn,
    agentd_status,
    agentd_stop,
    notify,
    wait_for_actor,
)

logging.basicConfig(level=logging.INFO, format="[%(name)s] %(message)s")
logger = logging.getLogger("telegram")

# File handler for /logs command
_LOG_DIR = Path(os.environ.get("XDG_STATE_HOME", str(Path.home() / ".local" / "state"))) / "agentd"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_file_handler = logging.FileHandler(_LOG_DIR / "agentd-telegram.log", encoding="utf-8")
_file_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
logger.addHandler(_file_handler)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
ALLOWED_USERS: set[int] = set()
_raw = os.environ.get("TELEGRAM_ALLOWED_USERS", "").strip()
for part in _raw.split(","):
    part = part.strip()
    if part.isdigit():
        ALLOWED_USERS.add(int(part))

PI_CODING_AGENT_DIR = os.environ.get("PI_CODING_AGENT_DIR", str(Path.home() / ".config" / "pi"))
_xdg_data = os.environ.get("XDG_DATA_HOME") or str(Path.home() / ".local" / "share")
DATA_DIR = Path(
    os.environ.get("TELEGRAM_ADAPTER_DATA", str(Path(_xdg_data) / "agentd" / "telegram"))
)
OFFSET_FILE = DATA_DIR / "offset"
AUTO_NEW_SESSION_IDLE_SEC = int(os.environ.get("AUTO_NEW_SESSION_IDLE_SEC", "7200"))
POLL_TIMEOUT = 60
PROGRESS_DELAY_MS = 1000
GENERIC_FAILURE_PREFIX = "🔴 Failed"


def _short_turn_id(turn_id: str) -> str:
    return turn_id.removeprefix("turn_") if turn_id else "unknown"


def format_failure_message(error_code: str, turn_id: str) -> str:
    """Return a user-safe failure message without leaking low-level errors."""
    code = error_code or "UNKNOWN_ERROR"
    return f"{GENERIC_FAILURE_PREFIX}: {code} (turn: {_short_turn_id(turn_id)})"


# ---------------------------------------------------------------------------
# Telegram API
# ---------------------------------------------------------------------------

_BASE_URL = f"https://api.telegram.org/bot{TOKEN}"

_session: Any = None


def _get_session() -> Any:
    import aiohttp

    global _session  # noqa: PLW0603
    if _session is None or _session.closed:
        import socket

        connector = aiohttp.TCPConnector(family=socket.AF_INET)  # IPv4 only
        _session = aiohttp.ClientSession(connector=connector)
    return _session


async def close_session() -> None:
    global _session  # noqa: PLW0603
    if _session is not None and not _session.closed:
        await _session.close()
        _session = None


async def tg_api(method: str, payload: dict[str, Any] | None = None) -> Any:
    session = _get_session()
    url = f"{_BASE_URL}/{method}"
    if payload:
        async with session.post(url, json=payload) as resp:
            data = await resp.json()
    else:
        async with session.get(url) as resp:
            data = await resp.json()
    if not data.get("ok"):
        raise RuntimeError(data.get("description", "Telegram API error"))
    return data.get("result")


async def send_text(chat_id: str, text: str, reply_to: int | None = None) -> dict:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text.strip() or "✅ Done.",
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    if reply_to:
        payload["reply_to_message_id"] = reply_to
    try:
        return await tg_api("sendMessage", payload)
    except RuntimeError as e:
        if "can't parse entities" in str(e):
            payload.pop("parse_mode", None)
            return await tg_api("sendMessage", payload)
        raise


async def send_typing(chat_id: str) -> None:
    with contextlib.suppress(Exception):
        await tg_api("sendChatAction", {"chat_id": chat_id, "action": "typing"})


# ---------------------------------------------------------------------------
# Progress reporter (Telegram-specific)
# ---------------------------------------------------------------------------


class ProgressReporter:
    """Manages a single editable progress message in a Telegram chat.

    - Delays initial creation to avoid flicker on fast tasks.
    - Batches edits to avoid Telegram rate limits.
    - Deletes the message on finish.
    """

    def __init__(
        self,
        chat_id: str,
        tg_api_fn: Any,
        *,
        delay_ms: int = 1000,
        initial_text: str = "✨ Please wait…",
    ) -> None:
        self._chat_id = chat_id
        self._tg_api = tg_api_fn
        self._desired_text = initial_text
        self._last_sent_text = ""
        self._message_id: int | None = None
        self._edit_queue: asyncio.Lock = asyncio.Lock()
        self._created = asyncio.Event()
        self._create_task = asyncio.create_task(self._create_after_delay(delay_ms / 1000.0))

    async def _create_after_delay(self, delay_s: float) -> None:
        await asyncio.sleep(delay_s)
        try:
            result = await self._tg_api(
                "sendMessage",
                {
                    "chat_id": self._chat_id,
                    "text": self._desired_text,
                    "disable_web_page_preview": True,
                    "disable_notification": True,
                },
            )
            self._message_id = result.get("message_id") if isinstance(result, dict) else None
            self._last_sent_text = self._desired_text
        except Exception:
            logger.exception("failed creating progress message")
        finally:
            self._created.set()

    async def update(self, text: str) -> None:
        self._desired_text = text
        if self._message_id is None:
            return
        if text == self._last_sent_text:
            return
        async with self._edit_queue:
            if self._desired_text == self._last_sent_text:
                return
            try:
                await self._tg_api(
                    "editMessageText",
                    {
                        "chat_id": self._chat_id,
                        "message_id": self._message_id,
                        "text": self._desired_text,
                        "disable_web_page_preview": True,
                    },
                )
                self._last_sent_text = self._desired_text
            except Exception as e:
                if "message is not modified" not in str(e):
                    logger.warning("failed editing progress: %s", e)

    async def finish(self) -> None:
        if self._create_task is not None:
            self._create_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._create_task
        self._created.set()
        if self._message_id is None:
            return
        try:
            await self._tg_api(
                "deleteMessage",
                {"chat_id": self._chat_id, "message_id": self._message_id},
            )
        except Exception:
            logger.warning("failed deleting progress message")


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

chat_queues: dict[str, asyncio.Future] = {}
last_inbound_at: dict[str, float] = {}
pending_new: set[str] = set()


def load_offset() -> int:
    if OFFSET_FILE.exists():
        try:
            return int(OFFSET_FILE.read_text().strip())
        except (ValueError, OSError):
            pass
    return 0


def save_offset(offset: int) -> None:
    OFFSET_FILE.parent.mkdir(parents=True, exist_ok=True)
    OFFSET_FILE.write_text(str(offset))


# ---------------------------------------------------------------------------
# FIFO queue per chat
# ---------------------------------------------------------------------------


async def with_chat_queue(chat_id: str, fn):
    loop = asyncio.get_event_loop()
    my_turn = loop.create_future()
    prev = chat_queues.get(chat_id)
    chat_queues[chat_id] = my_turn
    if prev:
        with contextlib.suppress(Exception):
            await prev
    try:
        return await fn()
    finally:
        if not my_turn.done():
            my_turn.set_result(None)
        if chat_queues.get(chat_id) is my_turn:
            del chat_queues[chat_id]


# ---------------------------------------------------------------------------
# Actor name & env
# ---------------------------------------------------------------------------


def root_actor_name(chat_id: str) -> str:
    return f"telegram:{chat_id}"


def agent_env(chat_id: str, reply_to: int | None = None) -> dict[str, str]:
    env: dict[str, str] = {
        "PI_CODING_AGENT_DIR": PI_CODING_AGENT_DIR,
        "TELEGRAM_BOT_TOKEN": TOKEN,
        "TELEGRAM_DEFAULT_CHAT_ID": chat_id,
    }
    if reply_to:
        env["TELEGRAM_REPLY_TO_MESSAGE_ID"] = str(reply_to)
    return env


# ---------------------------------------------------------------------------
# Telegram event payload
# ---------------------------------------------------------------------------


def build_event_payload(message: dict[str, Any], text: str) -> dict[str, Any]:
    chat = message.get("chat", {})
    from_user = message.get("from", {})

    payload: dict[str, Any] = {
        "channel": "telegram",
        "text": text,
        "chat": {
            "id": str(chat.get("id", "")),
            "type": chat.get("type", "private"),
        },
        "message": {
            "id": message.get("message_id"),
            "date": message.get("date"),
            "text": text,
            "from": {
                "id": from_user.get("id"),
                "username": from_user.get("username"),
                "first_name": from_user.get("first_name"),
                "is_bot": from_user.get("is_bot", False),
            },
        },
    }

    reply = message.get("reply_to_message")
    if reply:
        reply_from = reply.get("from", {})
        payload["reply_to"] = {
            "id": reply.get("message_id"),
            "text": reply.get("text", ""),
            "from": {
                "id": reply_from.get("id"),
                "username": reply_from.get("username"),
            },
        }

    return payload


# ---------------------------------------------------------------------------
# Root actor execution (notify: emit-or-spawn)
# ---------------------------------------------------------------------------


async def run_via_agentd(
    chat_id: str, message: dict, text: str, *, is_new: bool = False
) -> WaitResult:
    event_type = "env.telegram.message"
    payload = build_event_payload(message, text)
    env = agent_env(chat_id, message.get("message_id"))
    name = root_actor_name(chat_id)

    try:
        if is_new:
            with contextlib.suppress(Exception):
                agentd_stop(name)
            logger.info("spawn name=%s (new session)", name)
            result = agentd_spawn(
                name,
                event_type=event_type,
                payload=payload,
                env_vars=env,
            )
            is_fresh = True
        else:
            logger.info("notify name=%s", name)
            result = notify(
                name,
                event_type=event_type,
                payload=payload,
                env_vars=env,
            )
            is_fresh = "current_turn" in result

        actor_id = result["actor_id"]
        since_seq = result.get("event_seq", 0)
        logger.info("result actor=%s fresh=%s since_seq=%d", actor_id, is_fresh, since_seq)

        pr = ProgressReporter(
            chat_id,
            tg_api,
            delay_ms=PROGRESS_DELAY_MS,
            initial_text="✨ New session…" if is_fresh else "✨ Please wait…",
        )

        async def on_progress(text: str) -> None:
            await pr.update(text)

        try:
            wait = await wait_for_actor(actor_id, on_progress=on_progress, since_seq=since_seq)
        finally:
            await pr.finish()

        return wait
    except Exception as e:
        return WaitResult(ok=False, error=str(e), error_code="UNKNOWN_ERROR")


# ---------------------------------------------------------------------------
# /logs command
# ---------------------------------------------------------------------------

_ADAPTER_LOG = Path(
    os.environ.get(
        "AGENTD_TELEGRAM_LOG",
        str(
            Path(os.environ.get("XDG_STATE_HOME", str(Path.home() / ".local" / "state")))
            / "agentd"
            / "agentd-telegram.log"
        ),
    )
)


def _tail(path: Path, lines: int = 20) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
        return "\n".join(text.splitlines()[-lines:])
    except FileNotFoundError:
        return ""


async def _handle_logs_command(chat_id: str, mid: int | None) -> None:
    parts: list[str] = []

    name = root_actor_name(chat_id)
    try:
        result = agentd_exec(["ps", "--all"])
        actors = result.get("actors", [])
        actor = next((a for a in actors if a.get("name") == name), None)
        if actor:
            actor_id = actor["actor_id"]
            state = actor["state"]
            logs = agentd_exec(["logs", actor_id, "--limit", "10"])
            events = logs.get("events", [])
            summary_lines = []
            for e in events[-10:]:
                etype = e.get("event_type", "?")
                ts = (e.get("created_at") or "")[:19]
                if etype in {"actor.spawned", "turn.started", "turn.end", "actor.closed"}:
                    summary_lines.append(f"{ts} [{etype}]")
                elif etype == "turn.progress":
                    p = e.get("payload") or {}
                    ptype = p.get("type", "")
                    if ptype == "tool_call":
                        tname = p.get("name", "")
                        summary_lines.append(f"{ts} [tool] {tname}")
                    elif ptype == "text":
                        content = p.get("content", "")[:60]
                        summary_lines.append(f"{ts} {content}")
            if summary_lines:
                parts.append(
                    f"📋 *Actor* `{actor_id}` ({state})\n```\n" + "\n".join(summary_lines) + "\n```"
                )
            else:
                parts.append(f"📋 *Actor* `{actor_id}` ({state}) — no recent events")
        else:
            parts.append("📋 No actor found for this chat.")
    except Exception as e:
        parts.append(f"📋 Actor logs unavailable: {e}")

    adapter_tail = _tail(_ADAPTER_LOG, 15)
    if adapter_tail:
        if len(adapter_tail) > 2000:
            adapter_tail = adapter_tail[-2000:]
        parts.append(f"📝 *Adapter log*\n```\n{adapter_tail}\n```")

    if not parts:
        await send_text(chat_id, "📭 No logs available.", mid)
    else:
        await send_text(chat_id, "\n\n".join(parts), mid)


# ---------------------------------------------------------------------------
# Command handling
# ---------------------------------------------------------------------------


async def handle_command(chat_id: str, text: str, message: dict) -> bool:
    cmd = text.split()[0].lower()
    mid = message.get("message_id")

    if cmd == "/ping":
        await send_text(chat_id, "🏓 Pong!", mid)
        return True
    if cmd == "/help":
        await send_text(
            chat_id,
            "🤖 *Commands*\n\n"
            "/ping — check status\n"
            "/new — start new session\n"
            "/stop — stop current actor\n"
            "/status — actor info\n"
            "/logs — recent actor activity\n"
            "/restart — restart Telegram channel\n"
            "/help — this message",
            mid,
        )
        return True
    if cmd == "/status":
        name = root_actor_name(chat_id)
        try:
            result = agentd_status()
            active = result.get("status", {}).get("active_actors", "?")
            await send_text(chat_id, f"🏷 Name: `{name}`\n🤖 Daemon: running ({active} active)", mid)
        except Exception:
            await send_text(chat_id, f"🏷 Name: `{name}`\n🤖 Daemon: unreachable", mid)
        return True
    if cmd == "/logs":
        await _handle_logs_command(chat_id, mid)
        return True
    if cmd == "/new":
        pending_new.add(chat_id)
        await send_text(chat_id, "🆕 Next message starts a new session.", mid)
        return True
    if cmd == "/stop":
        name = root_actor_name(chat_id)
        try:
            agentd_stop(name)
            await send_text(chat_id, "🛑 Actor stopped.", mid)
        except Exception as e:
            await send_text(chat_id, f"❌ Stop failed: {e}", mid)
        return True
    if cmd == "/restart":
        await send_text(chat_id, "♻️ Restarting Telegram channel…", mid)
        await asyncio.sleep(0.5)
        os._exit(0)

    return False


# ---------------------------------------------------------------------------
# Update handler
# ---------------------------------------------------------------------------


async def handle_update(update: dict) -> None:
    message = update.get("message")
    if not message:
        return

    from_user = message.get("from", {})
    user_id = from_user.get("id")
    if not isinstance(user_id, int):
        return

    chat_id = str(message.get("chat", {}).get("id", ""))
    if not chat_id:
        return

    if ALLOWED_USERS and user_id not in ALLOWED_USERS:
        await send_text(chat_id, "⛔ Not authorized.", message.get("message_id"))
        return

    text = (message.get("text") or message.get("caption") or "").strip() or "[non-text message]"
    logger.debug("msg from=%s chat=%s text=%r", user_id, chat_id, text[:80])

    if text.startswith("/") and await handle_command(chat_id, text, message):
        return

    async def _process():
        force_new = chat_id in pending_new
        pending_new.discard(chat_id)

        prev_ts = last_inbound_at.get(chat_id)
        now_ts = message.get("date") or int(time.time())
        idle = prev_ts is not None and (now_ts - prev_ts) >= AUTO_NEW_SESSION_IDLE_SEC
        is_new = force_new or (idle and not message.get("reply_to_message"))

        last_inbound_at[chat_id] = now_ts

        typing_task = asyncio.create_task(_typing_loop(chat_id))

        try:
            result = await run_via_agentd(chat_id, message, text, is_new=is_new)

            if result.stopped:
                await send_text(chat_id, "🛑 Actor stopped.", message.get("message_id"))
            elif not result.ok:
                logger.error(
                    "actor turn failed code=%s turn=%s: %s",
                    result.error_code or "UNKNOWN_ERROR",
                    result.turn_id or "unknown",
                    result.error,
                )
                await send_text(
                    chat_id,
                    format_failure_message(result.error_code, result.turn_id),
                    message.get("message_id"),
                )
            elif result.result_text:
                logger.info("result: %s", result.result_text[:200])
        finally:
            typing_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await typing_task

    await with_chat_queue(chat_id, _process)


async def _typing_loop(chat_id: str) -> None:
    while True:
        await send_typing(chat_id)
        await asyncio.sleep(4.5)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


async def main() -> None:
    if not TOKEN:
        print("TELEGRAM_BOT_TOKEN not set", file=sys.stderr)
        sys.exit(1)
    if not ALLOWED_USERS:
        print("TELEGRAM_ALLOWED_USERS not set", file=sys.stderr)
        sys.exit(1)

    try:
        agentd_status()
    except Exception as e:
        print(f"agentd not ready: {e}", file=sys.stderr)
        sys.exit(1)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    offset = load_offset()
    logger.info("starting, offset=%d", offset)

    default_chat = os.environ.get("TELEGRAM_DEFAULT_CHAT_ID", "").strip()
    if default_chat:
        with contextlib.suppress(Exception):
            await send_text(default_chat, "🟢 Online")

    while True:
        try:
            params: dict[str, Any] = {
                "limit": 50,
                "timeout": POLL_TIMEOUT,
                "allowed_updates": ["message"],
            }
            if offset > 0:
                params["offset"] = offset + 1

            updates = await tg_api("getUpdates", params)
            if not isinstance(updates, list) or not updates:
                continue

            for update in updates:
                uid = update.get("update_id", 0)
                if isinstance(uid, int) and uid > offset:
                    offset = uid
                    save_offset(offset)
                asyncio.create_task(handle_update(update))

        except asyncio.CancelledError:
            break
        except (OSError, aiohttp.ClientError) as e:
            logger.warning("polling: %s", e)
            await asyncio.sleep(2)
        except RuntimeError as e:
            if "Conflict" in str(e):
                logger.warning("polling conflict, waiting for old session to expire...")
                await close_session()
                await asyncio.sleep(POLL_TIMEOUT + 5)
            else:
                logger.exception("polling error")
                await close_session()
                await asyncio.sleep(5)

    await close_session()
    logger.info("stopped")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(main())
