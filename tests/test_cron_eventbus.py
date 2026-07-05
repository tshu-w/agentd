"""Tests for cron timezone and event_bus correctness."""

from datetime import UTC, datetime

import pytest

from agentd.scheduler.cron import compute_next_fire
from agentd.scheduler.event_bus import EventBus, SlowConsumerError

# ---------------------------------------------------------------------------
# Cron: local timezone interpretation
# ---------------------------------------------------------------------------


class TestCronTimezone:
    def test_result_is_utc_iso(self):
        result = compute_next_fire("0 9 * * *")
        assert result.endswith("Z")

    def test_interprets_in_local_timezone(self):
        """Cron '0 9 * * *' means 9am local, not 9am UTC."""
        local_tz = datetime.now().astimezone().tzinfo
        local_offset = datetime.now(local_tz).utcoffset()
        assert local_offset is not None

        result = compute_next_fire("0 9 * * *")
        next_utc = datetime.fromisoformat(result.replace("Z", "+00:00"))

        # The fire time in local tz should have hour=9
        next_local = next_utc.astimezone(local_tz)
        assert next_local.hour == 9

    def test_explicit_after_is_converted_to_local(self):
        """Even if `after` is in UTC, cron is still interpreted in local tz."""
        # Use a UTC time that's clearly a different hour in local tz
        after_utc = datetime(2025, 6, 15, 0, 0, 0, tzinfo=UTC)
        result = compute_next_fire("30 14 * * *", after=after_utc)
        next_utc = datetime.fromisoformat(result.replace("Z", "+00:00"))

        local_tz = datetime.now().astimezone().tzinfo
        next_local = next_utc.astimezone(local_tz)
        assert next_local.hour == 14
        assert next_local.minute == 30


# ---------------------------------------------------------------------------
# EventBus: __aiter__ protocol
# ---------------------------------------------------------------------------


class TestSubscriptionAiter:
    @pytest.mark.asyncio
    async def test_async_for_works(self):
        """Subscription must work with 'async for'."""
        bus = EventBus()
        sub = await bus.subscribe(actor_id="act_test")

        # Publish two events then close
        bus.publish({"actor_id": "act_test", "seq": 1, "type": "t", "payload": {}})
        bus.publish({"actor_id": "act_test", "seq": 2, "type": "t", "payload": {}})
        sub.close()

        collected = []
        async for event in sub:
            collected.append(event)
        assert len(collected) == 2

    @pytest.mark.asyncio
    async def test_async_for_filters_by_actor(self):
        bus = EventBus()
        sub = await bus.subscribe(actor_id="act_a")

        bus.publish({"actor_id": "act_a", "seq": 1, "type": "t", "payload": {}})
        bus.publish({"actor_id": "act_b", "seq": 2, "type": "t", "payload": {}})
        sub.close()

        collected = []
        async for event in sub:
            collected.append(event)
        assert len(collected) == 1
        assert collected[0]["actor_id"] == "act_a"

    @pytest.mark.asyncio
    async def test_overflow_delivers_sentinel(self):
        """When queue fills up, consumer must get SlowConsumerError, not hang."""
        bus = EventBus()
        sub = await bus.subscribe(actor_id="act_x", since_seq=0)

        # Fill queue beyond capacity
        for i in range(300):
            bus.publish({"actor_id": "act_x", "seq": i + 1, "type": "t", "payload": {}})

        collected = []
        with pytest.raises(SlowConsumerError):
            async for event in sub:
                collected.append(event)

        # Should have received events up to capacity, then SlowConsumerError
        assert len(collected) > 0
        assert len(collected) < 300


# ---------------------------------------------------------------------------
# Duration / one-shot time parsing
# ---------------------------------------------------------------------------


class TestParseDuration:
    def test_units(self):
        from agentd.scheduler.cron import parse_duration

        assert parse_duration("90s") == 90
        assert parse_duration("15m") == 900
        assert parse_duration("3h") == 10800
        assert parse_duration("1d") == 86400
        assert parse_duration("1h30m") == 5400

    def test_invalid(self):
        from agentd.scheduler.cron import parse_duration

        for bad in ("", "3x", "h3", "1.5h", "3h junk", "-5m"):
            with pytest.raises(ValueError):
                parse_duration(bad)


class TestParseAt:
    def test_naive_is_local(self):
        from agentd.scheduler.cron import parse_at

        local_tz = datetime.now().astimezone().tzinfo
        dt = parse_at("2030-06-15T09:00")
        assert dt.tzinfo is not None
        assert dt == datetime(2030, 6, 15, 9, 0, tzinfo=local_tz)

    def test_explicit_offset_preserved(self):
        from agentd.scheduler.cron import parse_at

        dt = parse_at("2030-06-15T09:00+00:00")
        assert dt == datetime(2030, 6, 15, 9, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# fire_due_triggers: kind-specific reschedule/retire
# ---------------------------------------------------------------------------


def _sched_env():
    import tempfile
    from pathlib import Path

    from agentd.config import AgentDConfig
    from agentd.scheduler.scheduler import Scheduler
    from agentd.store import Store
    from agentd.store.db import Database

    p = Path(tempfile.mkdtemp()) / "t.db"
    db = Database(p)
    db.initialize()
    store = Store(db)
    sch = Scheduler(store, EventBus(), AgentDConfig())
    return store, sch


def _queued_types(store, actor_id):
    rows = (
        store.db.connect()
        .execute("SELECT message_type FROM mailbox WHERE actor_id = ?", (actor_id,))
        .fetchall()
    )
    return [r["message_type"] for r in rows]


PAST = "2020-01-01T00:00:00Z"


class TestFireDueTriggers:
    @pytest.mark.asyncio
    async def test_one_shot_fires_and_deletes_itself(self):
        from agentd.protocol import ROOT_SCOPE
        from agentd.scheduler.cron import fire_due_triggers

        store, sch = _sched_env()
        a = store.create_actor(name="a", scope_id=ROOT_SCOPE, backend="pi")
        trig = store.add_trigger(
            target_actor_id=a["actor_id"],
            kind="at",
            spec={"at": PAST},
            message_type="message",
            payload={"text": "wake up"},
            next_fire_at=PAST,
        )

        await fire_due_triggers(store, sch)

        assert "message" in _queued_types(store, a["actor_id"])
        assert store.get_trigger(trig["trigger_id"]) is None

    @pytest.mark.asyncio
    async def test_interval_fires_and_reschedules(self):
        from agentd.protocol import ROOT_SCOPE
        from agentd.scheduler.cron import fire_due_triggers
        from agentd.store.db import utc_now

        store, sch = _sched_env()
        a = store.create_actor(name="a", scope_id=ROOT_SCOPE, backend="pi")
        trig = store.add_trigger(
            target_actor_id=a["actor_id"],
            kind="every",
            spec={"every_seconds": 3600},
            message_type="message",
            payload={"text": "poll"},
            next_fire_at=PAST,
        )

        await fire_due_triggers(store, sch)

        after = store.get_trigger(trig["trigger_id"])
        assert after is not None
        assert after["next_fire_at"] > utc_now()

    @pytest.mark.asyncio
    async def test_cron_fires_and_reschedules(self):
        from agentd.protocol import ROOT_SCOPE
        from agentd.scheduler.cron import fire_due_triggers
        from agentd.store.db import utc_now

        store, sch = _sched_env()
        a = store.create_actor(name="a", scope_id=ROOT_SCOPE, backend="pi")
        trig = store.add_trigger(
            target_actor_id=a["actor_id"],
            kind="cron",
            spec={"cron": "0 9 * * *"},
            message_type="message",
            payload={},
            next_fire_at=PAST,
        )

        await fire_due_triggers(store, sch)

        after = store.get_trigger(trig["trigger_id"])
        assert after is not None
        assert after["next_fire_at"] > utc_now()

    @pytest.mark.asyncio
    async def test_unknown_kind_is_deleted_not_refired_forever(self):
        from agentd.protocol import ROOT_SCOPE
        from agentd.scheduler.cron import fire_due_triggers

        store, sch = _sched_env()
        a = store.create_actor(name="a", scope_id=ROOT_SCOPE, backend="pi")
        trig = store.add_trigger(
            target_actor_id=a["actor_id"],
            kind="webhook",
            spec={},
            message_type="message",
            payload={},
            next_fire_at=PAST,
        )

        await fire_due_triggers(store, sch)

        assert store.get_trigger(trig["trigger_id"]) is None

    @pytest.mark.asyncio
    async def test_closed_actor_trigger_deleted_without_firing(self):
        from agentd.protocol import ROOT_SCOPE, ActorState
        from agentd.scheduler.cron import fire_due_triggers

        store, sch = _sched_env()
        a = store.create_actor(name="a", scope_id=ROOT_SCOPE, backend="pi")
        store.transition_actor(a["actor_id"], ActorState.CLOSED)
        trig = store.add_trigger(
            target_actor_id=a["actor_id"],
            kind="at",
            spec={"at": PAST},
            message_type="message",
            payload={},
            next_fire_at=PAST,
        )

        await fire_due_triggers(store, sch)

        assert store.get_trigger(trig["trigger_id"]) is None
        assert _queued_types(store, a["actor_id"]) == []
