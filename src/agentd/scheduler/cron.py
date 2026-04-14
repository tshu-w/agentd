"""Cron trigger scheduler.

Periodically checks for due triggers and fires them as actor.emit.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from croniter import croniter

from agentd.store import Store
from agentd.store.db import utc_now

if TYPE_CHECKING:
    from .scheduler import Scheduler

logger = logging.getLogger(__name__)

CRON_CHECK_INTERVAL = 10  # seconds


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


async def run_cron_loop(store: Store, scheduler: Scheduler) -> None:
    """Background loop that fires due cron triggers."""
    while True:
        try:
            await asyncio.sleep(CRON_CHECK_INTERVAL)
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

                    # Update next fire time
                    spec = trigger.get("spec", {})
                    cron_expr = spec.get("cron", "")
                    if cron_expr:
                        next_fire = compute_next_fire(cron_expr)
                        store.update_trigger_next_fire(trigger["trigger_id"], next_fire)
                except Exception:
                    logger.exception("error firing trigger=%s", trigger["trigger_id"])
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("cron loop error")
