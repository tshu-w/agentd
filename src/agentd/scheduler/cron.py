"""Timed trigger scheduler.

Periodically checks for due triggers and fires them as actor.emit.
Kinds: `cron` (recurring, cron expression), `every` (recurring, fixed
interval), `at` (one-shot — deletes itself after firing).
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from croniter import croniter

from agentd.store import Store
from agentd.store.db import utc_now

if TYPE_CHECKING:
    from .scheduler import Scheduler

logger = logging.getLogger(__name__)

CRON_CHECK_INTERVAL = 10  # seconds

_DURATION_PART = re.compile(r"(\d+)([smhd])")
_DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(spec: str) -> int:
    """Parse a duration like ``90s``, ``15m``, ``3h``, ``1d``, ``1h30m`` into seconds."""
    s = spec.strip().lower()
    parts = _DURATION_PART.findall(s)
    if not parts or "".join(f"{n}{u}" for n, u in parts) != s:
        raise ValueError(f"invalid duration: {spec!r} (use e.g. 90s, 15m, 3h, 1d, 1h30m)")
    total = sum(int(n) * _DURATION_UNITS[u] for n, u in parts)
    if total <= 0:
        raise ValueError(f"duration must be positive: {spec!r}")
    return total


def parse_at(spec: str) -> datetime:
    """Parse an ISO 8601 timestamp into an aware datetime.

    Naive timestamps are interpreted in the daemon's local timezone
    (consistent with cron expressions).
    """
    dt = datetime.fromisoformat(spec)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
    return dt


def to_utc_iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def compute_next_fire(cron_expr: str, after: datetime | None = None) -> str:
    """Compute the next fire time for a cron expression (UTC ISO string).

    The cron expression is interpreted in the daemon's local timezone
    (consistent with system cron behavior). The returned ISO string is UTC.
    """
    local_tz = datetime.now().astimezone().tzinfo
    if after is not None:
        base = after.astimezone(local_tz)
    else:
        base = datetime.now(local_tz)
    it = croniter(cron_expr, base)
    next_dt = it.get_next(datetime)
    if next_dt.tzinfo is None:
        next_dt = next_dt.replace(tzinfo=local_tz)
    next_utc = next_dt.astimezone(UTC)
    return next_utc.isoformat().replace("+00:00", "Z")


async def fire_due_triggers(store: Store, scheduler: Scheduler) -> None:
    """Fire all due triggers once, then reschedule (recurring) or delete (one-shot)."""
    now = utc_now()
    due = store.list_due_triggers(now)
    for trigger in due:
        try:
            actor_id = trigger["target_actor_id"]
            actor = store.get_actor(actor_id)
            if actor is None or actor["state"] == "closed":
                store.delete_trigger(trigger["trigger_id"])
                continue

            # Fire trigger as emit
            await scheduler.emit(
                actor_id=actor_id,
                msg_type=trigger["message_type"],
                msg_payload=trigger["payload"],
            )
            logger.info(
                "fired trigger=%s actor=%s",
                trigger["trigger_id"],
                actor_id,
            )

            kind = trigger.get("kind")
            spec = trigger.get("spec", {})
            if kind == "cron":
                next_fire = compute_next_fire(spec["cron"])
                store.update_trigger_next_fire(trigger["trigger_id"], next_fire)
            elif kind == "every":
                next_dt = datetime.now(UTC) + timedelta(seconds=int(spec["every_seconds"]))
                store.update_trigger_next_fire(trigger["trigger_id"], to_utc_iso(next_dt))
            else:
                # One-shot ("at"). Unknown kinds land here too — deleting is
                # safer than the alternative (refiring every tick forever).
                store.delete_trigger(trigger["trigger_id"])
        except Exception:
            logger.exception("error firing trigger=%s", trigger["trigger_id"])


async def run_cron_loop(store: Store, scheduler: Scheduler) -> None:
    """Background loop that fires due triggers."""
    while True:
        try:
            await asyncio.sleep(CRON_CHECK_INTERVAL)
            await fire_due_triggers(store, scheduler)
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("cron loop error")
