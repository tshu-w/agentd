"""Tests for scheduler — concurrency, reconcile, wakeup chain."""

import asyncio
import tempfile
from pathlib import Path

import pytest

from agentd.config import AgentDConfig
from agentd.protocol import ROOT_SCOPE, TurnOutcome, TurnState
from agentd.scheduler.event_bus import EventBus
from agentd.scheduler.scheduler import Scheduler
from agentd.store.db import Database
from agentd.store.store import Store


@pytest.fixture
def env():
    """Provide (store, bus, scheduler) with max_total_workers=2."""
    p = Path(tempfile.mkdtemp()) / "t.db"
    db = Database(p)
    db.initialize()
    store = Store(db)
    bus = EventBus()
    cfg = AgentDConfig()
    cfg.limits.max_total_workers = 2
    sch = Scheduler(store, bus, cfg)
    return store, bus, sch


# ---------------------------------------------------------------------------
# Concurrency limit: pending turn creation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_capacity_full_creates_pending_turn(env):
    store, _bus, sch = env
    sch._running_count = 2  # full

    a = store.create_actor(name="a", scope_id=ROOT_SCOPE, backend="pi")
    result = await sch.emit(actor_id=a["actor_id"], msg_type="message", msg_payload={"text": "hi"})

    assert result["woke"] is True
    actor = store.get_actor(a["actor_id"])
    assert actor["state"] == "active"

    turn = store.get_active_turn(a["actor_id"])
    assert turn is not None
    assert turn["state"] == "pending"

    # Message claimed, not queued
    assert store.count_queued(a["actor_id"]) == 0
    assert store.get_claimed_message(a["actor_id"]) is not None


@pytest.mark.asyncio
async def test_capacity_release_preserves_pending_turn_without_runtime(env):
    store, _bus, sch = env

    # Actor A occupies a slot
    a = store.create_actor(name="a", scope_id=ROOT_SCOPE, backend="pi")
    store.add_message(a["actor_id"], "message", {"text": "a"})
    turn_a, msg_a = store.open_turn_atomic(a["actor_id"])
    store.transition_turn(turn_a["turn_id"], TurnState.RUNNING)
    sch._turn_message[turn_a["turn_id"]] = msg_a["message_id"]
    sch._running_count = 2  # both slots full

    # Actor B gets a pending turn (capacity full)
    b = store.create_actor(name="b", scope_id=ROOT_SCOPE, backend="pi")
    await sch.emit(actor_id=b["actor_id"], msg_type="message", msg_payload={"text": "b"})
    assert store.get_active_turn(b["actor_id"])["state"] == "pending"

    # A completes → frees a slot → run the scheduling path for B
    sch._running_count = 2
    await sch.on_turn_completed(turn_a["turn_id"], outcome=TurnOutcome.SUCCEEDED)

    # Without a runtime, B should remain a valid pending turn
    b_turn = store.get_active_turn(b["actor_id"])
    assert b_turn is not None
    assert b_turn["state"] == "pending"
    actor_b = store.get_actor(b["actor_id"])
    assert actor_b["state"] == "active"


# ---------------------------------------------------------------------------
# Wakeup chain
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_same_actor_wakeup_chain(env):
    store, _bus, sch = env

    a = store.create_actor(name="a", scope_id=ROOT_SCOPE, backend="pi")
    # First message → turn
    await sch.emit(actor_id=a["actor_id"], msg_type="message", msg_payload={"text": "m1"})
    turn1 = store.get_active_turn(a["actor_id"])
    assert turn1 is not None
    sch._turn_message[turn1["turn_id"]] = store.get_claimed_message(a["actor_id"])["message_id"]

    # Second message → queued (actor already active)
    await sch.emit(actor_id=a["actor_id"], msg_type="message", msg_payload={"text": "m2"})
    assert store.count_queued(a["actor_id"]) == 1

    # Complete turn1 → should open turn for m2
    store.transition_turn(turn1["turn_id"], TurnState.RUNNING)
    await sch.on_turn_completed(turn1["turn_id"], outcome=TurnOutcome.SUCCEEDED)

    turn2 = store.get_active_turn(a["actor_id"])
    assert turn2 is not None
    assert turn2["turn_id"] != turn1["turn_id"]
    assert store.count_queued(a["actor_id"]) == 0


# ---------------------------------------------------------------------------
# Reconcile
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reconcile_running_turn_acks_message(env):
    store, _bus, sch = env

    a = store.create_actor(name="a", scope_id=ROOT_SCOPE, backend="pi")
    store.add_message(a["actor_id"], "message", {"text": "hi"})
    turn, msg = store.open_turn_atomic(a["actor_id"])
    store.transition_turn(turn["turn_id"], TurnState.RUNNING, exec_pid=999999)

    await sch.reconcile()

    # Turn failed, message acked, no duplicate
    actor = store.get_actor(a["actor_id"])
    assert actor["state"] == "idle"
    assert store.count_queued(a["actor_id"]) == 0
    assert store.get_claimed_message(a["actor_id"]) is None
    assert store.get_active_turn(a["actor_id"]) is None
    assert store.list_idle_actors_with_queued() == []


@pytest.mark.asyncio
async def test_reconcile_pending_turn_respects_capacity(env):
    store, _bus, sch = env
    sch._running_count = 2  # full

    a = store.create_actor(name="a", scope_id=ROOT_SCOPE, backend="pi")
    store.add_message(a["actor_id"], "message", {"text": "hi"})
    store.open_turn_atomic(a["actor_id"])  # actor → active, msg → claimed

    await sch.reconcile()

    # Turn stays pending (no dispatch at full capacity)
    turn = store.get_active_turn(a["actor_id"])
    assert turn is not None
    assert turn["state"] == "pending"


# ---------------------------------------------------------------------------
# Close subtree: running_count correctness
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_pending_turn_does_not_decrement_running_count(env):
    store, _bus, sch = env
    sch._running_count = 2  # full

    a = store.create_actor(name="a", scope_id=ROOT_SCOPE, backend="pi")
    await sch.emit(actor_id=a["actor_id"], msg_type="message", msg_payload={"text": "hi"})
    turn = store.get_active_turn(a["actor_id"])
    assert turn["state"] == "pending"

    before = sch._running_count
    await sch.close(a["actor_id"])
    # Closing a pending (never-dispatched) turn must NOT decrement _running_count
    assert sch._running_count == before


# ---------------------------------------------------------------------------
# No double dispatch
# ---------------------------------------------------------------------------


class _FakeRuntime:
    def __init__(self):
        self.calls: list[dict] = []

    def prepare_turn(self, turn_id):
        pass

    async def execute_turn(self, *, turn_id, actor, input_messages, env=None):
        self.calls.append({"turn_id": turn_id, "env": env})

    async def stop_turn(self, turn_id):
        pass

    async def cancel_turn(self, turn_id):
        pass


@pytest.mark.asyncio
async def test_no_double_dispatch_on_turn_complete(env):
    """Same-actor wakeup + _try_schedule_waiting must not dispatch same turn twice."""
    store, _bus, sch = env
    rt = _FakeRuntime()
    sch.set_runtime(rt)

    a = store.create_actor(name="a", scope_id=ROOT_SCOPE, backend="pi")
    store.add_message(a["actor_id"], "message", {"text": "m1"})
    t1, msg1 = store.open_turn_atomic(a["actor_id"])
    store.transition_turn(t1["turn_id"], TurnState.RUNNING)
    sch._turn_message[t1["turn_id"]] = msg1["message_id"]
    sch._running_count = 1

    # m2 queued while a is active
    await sch.emit(actor_id=a["actor_id"], msg_type="message", msg_payload={"text": "m2"})

    await sch.on_turn_completed(t1["turn_id"], outcome=TurnOutcome.SUCCEEDED)
    await asyncio.sleep(0)

    turn_ids = [c["turn_id"] for c in rt.calls]
    assert len(turn_ids) == len(set(turn_ids)), f"duplicate dispatch: {turn_ids}"


@pytest.mark.asyncio
async def test_live_event_envelope_uses_event_type_only(env):
    _store, bus, sch = env
    sub = await bus.subscribe(actor_id="act_test")

    sch._publish_event("act_test", "turn_test", "turn.progress", {"type": "thinking"}, 1)

    event = await sub.__anext__()
    assert event["event_type"] == "turn.progress"
    assert "type" not in event

    await bus.unsubscribe(sub)


# ---------------------------------------------------------------------------
# Reconcile env recovery
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reconcile_recovers_env_overlay(env):
    """Pending turn's emit.env should survive daemon restart via turn.opened snapshot."""
    store, _bus, sch = env
    sch.config.limits.max_total_workers = 0  # force all turns pending
    rt1 = _FakeRuntime()
    sch.set_runtime(rt1)

    a = store.create_actor(name="a", scope_id=ROOT_SCOPE, backend="pi")
    await sch.emit(
        actor_id=a["actor_id"],
        msg_type="message",
        msg_payload={"text": "hi"},
        env={"SECRET": "42"},
    )
    assert len(rt1.calls) == 0  # not dispatched (capacity=0)

    # Simulate restart: new scheduler, same DB
    sch2 = Scheduler(store, EventBus(), sch.config)
    sch2.config.limits.max_total_workers = 2
    rt2 = _FakeRuntime()
    sch2.set_runtime(rt2)  # ty: ignore[invalid-argument-type]
    await sch2.reconcile()
    await asyncio.sleep(0)

    assert len(rt2.calls) == 1
    assert rt2.calls[0]["env"]["SECRET"] == "42"


# ---------------------------------------------------------------------------
# Duplicate turn.end
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_duplicate_turn_end_event(env):
    store, _bus, sch = env
    from agentd.protocol import EventType, ParsedLine
    from agentd.runtime.runner import Runtime

    rt = Runtime(store, _bus, sch.config, sch)
    sch.set_runtime(rt)

    a = store.create_actor(name="a", scope_id=ROOT_SCOPE, backend="pi")
    store.add_message(a["actor_id"], "message", {"text": "hi"})
    turn, msg = store.open_turn_atomic(a["actor_id"])
    sch._turn_message[turn["turn_id"]] = msg["message_id"]
    store.transition_turn(turn["turn_id"], TurnState.RUNNING)

    # Simulate backend raw turn_end → _handle_parsed
    await rt._handle_parsed(
        turn["turn_id"],
        a["actor_id"],
        ParsedLine(event_type=EventType.TURN_END, payload={"raw": "x"}, result="ok"),
    )

    # Scheduler canonical turn.end
    await sch.on_turn_completed(turn["turn_id"], outcome=TurnOutcome.SUCCEEDED, result="ok")

    events = store.list_events(a["actor_id"])
    turn_ends = [e for e in events if e["event_type"] == "turn.end"]
    assert len(turn_ends) == 1
    assert turn_ends[0]["payload"]["outcome"] == "succeeded"


@pytest.mark.asyncio
async def test_stop_pending_turn(env):
    """actor.stop must end a capacity-blocked pending turn and transition actor → idle."""
    store, _bus, sch = env
    sch.config.limits.max_total_workers = 0  # force all turns to stay pending

    rt = _FakeRuntime()
    sch.set_runtime(rt)

    a = store.create_actor(name="a", scope_id=ROOT_SCOPE, backend="pi")
    await sch.emit(actor_id=a["actor_id"], msg_type="message", msg_payload={"text": "hi"})

    # Verify pending state
    actor = store.get_actor(a["actor_id"])
    turn = store.get_active_turn(a["actor_id"])
    assert actor["state"] == "active"
    assert turn["state"] == "pending"

    result = await sch.stop(a["actor_id"])

    # Actor should be idle, turn should be ended
    assert result["state"] == "idle"
    assert result["changed_count"] == 1
    actor = store.get_actor(a["actor_id"])
    assert actor["state"] == "idle"
    assert store.get_active_turn(a["actor_id"]) is None

    # Should have emitted turn.end event with interrupted outcome
    events = store.list_events(a["actor_id"])
    turn_ends = [e for e in events if e["event_type"] == "turn.end"]
    assert len(turn_ends) == 1
    assert turn_ends[0]["payload"]["outcome"] == "interrupted"
