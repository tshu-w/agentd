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
