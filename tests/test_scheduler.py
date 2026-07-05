"""Tests for scheduler — concurrency, reconcile, wakeup chain."""

import asyncio
import tempfile
from pathlib import Path

import pytest

from agentd.config import AgentDConfig
from agentd.protocol import ROOT_SCOPE, EventType, TurnOutcome, TurnState
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


# ---------------------------------------------------------------------------
# Child → parent notification (env.turn_completed convention)
# ---------------------------------------------------------------------------


def _all_messages(store, actor_id: str) -> list[dict]:
    """Return every mailbox message for an actor regardless of state."""
    rows = (
        store.db.connect()
        .execute(
            "SELECT * FROM mailbox WHERE actor_id = ? ORDER BY created_at, message_id",
            (actor_id,),
        )
        .fetchall()
    )
    return [store._msg_dict(r) for r in rows]


@pytest.mark.asyncio
async def test_notify_parent_on_success(env):
    """Successful child turn.end auto-emits env.turn_completed and wakes the parent."""
    store, _bus, sch = env

    parent = store.create_actor(name="sup", scope_id=ROOT_SCOPE, backend="pi")
    child = store.create_actor(
        name="worker",
        scope_id=parent["actor_id"],
        backend="pi",
        parent_actor_id=parent["actor_id"],
    )

    # Open + end a child turn directly (no runtime needed)
    await sch.emit(actor_id=child["actor_id"], msg_type="message", msg_payload={"text": "go"})
    turn = store.get_active_turn(child["actor_id"])
    await sch.on_turn_completed(turn["turn_id"], outcome=TurnOutcome.SUCCEEDED, result="done.")

    # Parent should have received env.turn_completed in its mailbox (any state).
    msgs = _all_messages(store, parent["actor_id"])
    turn_ends = [m for m in msgs if m["message_type"] == "env.turn_completed"]
    assert len(turn_ends) == 1
    payload = turn_ends[0]["payload"]
    assert payload["actor_id"] == child["actor_id"]
    assert payload["actor_name"] == "worker"
    assert payload["outcome"] == "succeeded"
    assert payload["result"] == "done."
    assert "error" not in payload
    # Parent must have actually woken: emit on IDLE → _try_open_turn → ACTIVE.
    parent_after = store.get_actor(parent["actor_id"])
    assert parent_after["state"] == "active"
    assert store.get_active_turn(parent["actor_id"]) is not None


@pytest.mark.asyncio
async def test_notify_parent_on_failure(env):
    """Failed child turn still notifies parent so supervisor can react."""
    store, _bus, sch = env

    parent = store.create_actor(name="sup", scope_id=ROOT_SCOPE, backend="pi")
    child = store.create_actor(
        name="worker",
        scope_id=parent["actor_id"],
        backend="pi",
        parent_actor_id=parent["actor_id"],
    )
    await sch.emit(actor_id=child["actor_id"], msg_type="message", msg_payload={"text": "go"})
    turn = store.get_active_turn(child["actor_id"])
    await sch.on_turn_completed(turn["turn_id"], outcome=TurnOutcome.FAILED, error="boom")

    msgs = _all_messages(store, parent["actor_id"])
    turn_ends = [m for m in msgs if m["message_type"] == "env.turn_completed"]
    assert len(turn_ends) == 1
    payload = turn_ends[0]["payload"]
    assert payload["outcome"] == "failed"
    assert payload["error"] == "boom"
    assert "result" not in payload


@pytest.mark.asyncio
async def test_notify_suppressed_on_user_termination(env):
    """User-initiated stop/cancel should not generate turn_completed noise."""
    store, _bus, sch = env

    parent = store.create_actor(name="sup", scope_id=ROOT_SCOPE, backend="pi")
    child = store.create_actor(
        name="worker",
        scope_id=parent["actor_id"],
        backend="pi",
        parent_actor_id=parent["actor_id"],
    )
    await sch.emit(actor_id=child["actor_id"], msg_type="message", msg_payload={"text": "go"})
    turn = store.get_active_turn(child["actor_id"])
    await sch.on_turn_completed(turn["turn_id"], outcome=TurnOutcome.INTERRUPTED)

    msgs = _all_messages(store, parent["actor_id"])
    assert [m for m in msgs if m["message_type"] == "env.turn_completed"] == []


@pytest.mark.asyncio
async def test_notify_skipped_when_parent_closed(env):
    """Closed parent must not receive notifications."""
    store, _bus, sch = env

    parent = store.create_actor(name="sup", scope_id=ROOT_SCOPE, backend="pi")
    child = store.create_actor(
        name="worker",
        scope_id=parent["actor_id"],
        backend="pi",
        parent_actor_id=parent["actor_id"],
    )

    await sch.emit(actor_id=child["actor_id"], msg_type="message", msg_payload={"text": "go"})
    turn = store.get_active_turn(child["actor_id"])

    # Race: parent gets closed (without cascade) between child turn opening and
    # turn.end firing. The notification path must silently skip.
    store.db.connect().execute(
        "UPDATE actors SET state = 'closed' WHERE actor_id = ?",
        (parent["actor_id"],),
    )
    store.db.connect().commit()

    await sch.on_turn_completed(turn["turn_id"], outcome=TurnOutcome.SUCCEEDED, result="done")

    msgs = _all_messages(store, parent["actor_id"])
    assert [m for m in msgs if m["message_type"] == "env.turn_completed"] == []


@pytest.mark.asyncio
async def test_notify_delivers_final_text_result_to_parent(env):
    """Child final text result is delivered with the completion notification."""
    store, _bus, sch = env
    parent = store.create_actor(name="sup", scope_id=ROOT_SCOPE, backend="pi")
    child = store.create_actor(
        name="worker",
        scope_id=parent["actor_id"],
        backend="pi",
        parent_actor_id=parent["actor_id"],
    )
    await sch.emit(actor_id=child["actor_id"], msg_type="message", msg_payload={"text": "go"})
    turn = store.get_active_turn(child["actor_id"])

    result = "child finished with findings"
    await sch.on_turn_completed(turn["turn_id"], outcome=TurnOutcome.SUCCEEDED, result=result)

    msgs = _all_messages(store, parent["actor_id"])
    payload = next(m["payload"] for m in msgs if m["message_type"] == "env.turn_completed")
    assert payload["result"] == result


@pytest.mark.asyncio
async def test_notify_queues_when_parent_active(env):
    """If parent is mid-turn, env.turn_completed queues for the next turn."""
    store, _bus, sch = env

    parent = store.create_actor(name="sup", scope_id=ROOT_SCOPE, backend="pi")
    child = store.create_actor(
        name="worker",
        scope_id=parent["actor_id"],
        backend="pi",
        parent_actor_id=parent["actor_id"],
    )

    # Force parent ACTIVE: emit drives IDLE → ACTIVE with a turn pending.
    await sch.emit(actor_id=parent["actor_id"], msg_type="message", msg_payload={"text": "hi"})
    parent_state = store.get_actor(parent["actor_id"])
    assert parent_state["state"] == "active"

    # Run a child turn to completion.
    await sch.emit(actor_id=child["actor_id"], msg_type="message", msg_payload={"text": "go"})
    turn = store.get_active_turn(child["actor_id"])
    await sch.on_turn_completed(turn["turn_id"], outcome=TurnOutcome.SUCCEEDED, result="r")

    msgs = _all_messages(store, parent["actor_id"])
    turn_ends = [m for m in msgs if m["message_type"] == "env.turn_completed"]
    assert len(turn_ends) == 1
    # Parent stays ACTIVE (its earlier turn is still open).
    assert store.get_actor(parent["actor_id"])["state"] == "active"


@pytest.mark.asyncio
async def test_notify_does_not_propagate_to_grandparent(env):
    """Notifications target the immediate parent only — no transitive forwarding."""
    store, _bus, sch = env

    gp = store.create_actor(name="gp", scope_id=ROOT_SCOPE, backend="pi")
    parent = store.create_actor(
        name="parent",
        scope_id=gp["actor_id"],
        backend="pi",
        parent_actor_id=gp["actor_id"],
    )
    child = store.create_actor(
        name="child",
        scope_id=parent["actor_id"],
        backend="pi",
        parent_actor_id=parent["actor_id"],
    )

    await sch.emit(actor_id=child["actor_id"], msg_type="message", msg_payload={"text": "go"})
    turn = store.get_active_turn(child["actor_id"])
    await sch.on_turn_completed(turn["turn_id"], outcome=TurnOutcome.SUCCEEDED, result="ok")

    parent_msgs = _all_messages(store, parent["actor_id"])
    gp_msgs = _all_messages(store, gp["actor_id"])
    assert any(m["message_type"] == "env.turn_completed" for m in parent_msgs)
    assert all(m["message_type"] != "env.turn_completed" for m in gp_msgs)


@pytest.mark.asyncio
async def test_root_actor_does_not_self_emit(env):
    """Actors without a parent never trigger env.turn_completed."""
    store, _bus, sch = env

    a = store.create_actor(name="a", scope_id=ROOT_SCOPE, backend="pi")
    await sch.emit(actor_id=a["actor_id"], msg_type="message", msg_payload={"text": "go"})
    turn = store.get_active_turn(a["actor_id"])
    await sch.on_turn_completed(turn["turn_id"], outcome=TurnOutcome.SUCCEEDED, result="hi")

    msgs = _all_messages(store, a["actor_id"])
    assert [m for m in msgs if m["message_type"] == "env.turn_completed"] == []


@pytest.mark.asyncio
async def test_notify_survives_capacity_pressure(env):
    """Notification path must work even when global worker capacity is saturated.

    The parent's wakeup turn may stay pending (no dispatch slot), but the
    mailbox message must still land and the parent must transition to ACTIVE.
    """
    store, _bus, sch = env
    sch.config.limits.max_total_workers = 1
    rt = _FakeRuntime()
    sch.set_runtime(rt)

    parent = store.create_actor(name="sup", scope_id=ROOT_SCOPE, backend="pi")
    child = store.create_actor(
        name="worker",
        scope_id=parent["actor_id"],
        backend="pi",
        parent_actor_id=parent["actor_id"],
    )

    # Saturate the single dispatch slot with an unrelated running turn.
    blocker = store.create_actor(name="blocker", scope_id=ROOT_SCOPE, backend="pi")
    await sch.emit(actor_id=blocker["actor_id"], msg_type="message", msg_payload={"text": "hold"})
    assert sch._running_count == 1

    # Run the child turn to completion (manually, since we're not exercising
    # runtime here — the open path counts toward _running_count, then
    # on_turn_completed releases it before notify runs).
    await sch.emit(actor_id=child["actor_id"], msg_type="message", msg_payload={"text": "go"})
    turn = store.get_active_turn(child["actor_id"])
    await sch.on_turn_completed(turn["turn_id"], outcome=TurnOutcome.SUCCEEDED, result="ok")

    # Mailbox got the notification.
    msgs = _all_messages(store, parent["actor_id"])
    assert any(m["message_type"] == "env.turn_completed" for m in msgs)
    # Parent woke up to ACTIVE; its turn may be running (slot was released by
    # the child completing) or pending depending on scheduling order, but the
    # actor itself must not be stuck IDLE with a queued message.
    assert store.get_actor(parent["actor_id"])["state"] == "active"
    assert store.get_active_turn(parent["actor_id"]) is not None
    assert sch._running_count == 1


@pytest.mark.asyncio
async def test_notify_exception_does_not_block_scheduler(env, monkeypatch):
    """A failed parent notification must not break on_turn_completed.

    The child's turn.end is already persisted; swallowing the notification
    exception preserves the global liveness invariant that
    `_try_schedule_waiting` always runs.
    """
    store, _bus, sch = env

    parent = store.create_actor(name="sup", scope_id=ROOT_SCOPE, backend="pi")
    child = store.create_actor(
        name="worker",
        scope_id=parent["actor_id"],
        backend="pi",
        parent_actor_id=parent["actor_id"],
    )
    await sch.emit(actor_id=child["actor_id"], msg_type="message", msg_payload={"text": "go"})
    turn = store.get_active_turn(child["actor_id"])

    # Patch only the parent-notify call; let other emits work normally.
    original_emit = sch.emit

    async def emit_router(**kwargs):
        if kwargs.get("msg_type") == "env.turn_completed":
            raise RuntimeError("simulated notify failure")
        return await original_emit(**kwargs)

    monkeypatch.setattr(sch, "emit", emit_router)

    schedule_calls: list[bool] = []
    original_try_schedule = sch._try_schedule_waiting

    async def tracking_try_schedule():
        schedule_calls.append(True)
        return await original_try_schedule()

    monkeypatch.setattr(sch, "_try_schedule_waiting", tracking_try_schedule)

    # Must not raise — exception swallowed inside _notify_parent_turn_completed.
    await sch.on_turn_completed(turn["turn_id"], outcome=TurnOutcome.SUCCEEDED, result="ok")
    assert schedule_calls

    # Child completed cleanly: turn ended, actor idle, no env.turn_completed
    # in parent (because emit was the failing call).
    child_after = store.get_actor(child["actor_id"])
    assert child_after["state"] == "idle"
    assert store.get_active_turn(child["actor_id"]) is None
    parent_msgs = _all_messages(store, parent["actor_id"])
    assert all(m["message_type"] != "env.turn_completed" for m in parent_msgs)


@pytest.mark.asyncio
async def test_reconcile_notifies_parent_of_restart_failure(env):
    """Turns force-failed by daemon-restart reconcile must still notify the parent."""
    store, _bus, sch = env

    parent = store.create_actor(name="sup", scope_id=ROOT_SCOPE, backend="pi")
    child = store.create_actor(
        name="worker",
        scope_id=parent["actor_id"],
        backend="pi",
        parent_actor_id=parent["actor_id"],
    )
    store.add_message(child["actor_id"], "message", {"text": "go"})
    turn, _msg = store.open_turn_atomic(child["actor_id"])
    store.transition_turn(turn["turn_id"], TurnState.RUNNING, exec_pid=999999)

    await sch.reconcile()

    msgs = _all_messages(store, parent["actor_id"])
    turn_ends = [m for m in msgs if m["message_type"] == "env.turn_completed"]
    assert len(turn_ends) == 1
    payload = turn_ends[0]["payload"]
    assert payload["actor_id"] == child["actor_id"]
    assert payload["outcome"] == "failed"
    assert payload["error"] == "daemon restarted"
    # Child settled cleanly.
    assert store.get_actor(child["actor_id"])["state"] == "idle"
    # Reconcile step 4 (idle wakeup) opened a turn for the queued notification.
    assert store.get_actor(parent["actor_id"])["state"] == "active"
    assert store.get_active_turn(parent["actor_id"]) is not None


@pytest.mark.asyncio
async def test_reconcile_notify_skips_closed_parent(env):
    """Enqueue-only notify path must tolerate a closed parent."""
    store, _bus, sch = env

    parent = store.create_actor(name="sup", scope_id=ROOT_SCOPE, backend="pi")
    child = store.create_actor(
        name="worker",
        scope_id=parent["actor_id"],
        backend="pi",
        parent_actor_id=parent["actor_id"],
    )
    store.add_message(child["actor_id"], "message", {"text": "go"})
    turn, _msg = store.open_turn_atomic(child["actor_id"])
    store.transition_turn(turn["turn_id"], TurnState.RUNNING, exec_pid=999999)

    store.db.connect().execute(
        "UPDATE actors SET state = 'closed' WHERE actor_id = ?",
        (parent["actor_id"],),
    )
    store.db.connect().commit()

    await sch.reconcile()

    msgs = _all_messages(store, parent["actor_id"])
    assert [m for m in msgs if m["message_type"] == "env.turn_completed"] == []


@pytest.mark.asyncio
async def test_emit_event_seq_anchors_before_own_events(env):
    """Replaying events > event_seq must include this emit's own turn.opened."""
    store, _bus, sch = env

    a = store.create_actor(name="a", scope_id=ROOT_SCOPE, backend="pi")
    # Unrelated actor appends an event first so max_seq > 0.
    other = store.create_actor(name="other", scope_id=ROOT_SCOPE, backend="pi")
    store.append_event(other["actor_id"], EventType.ACTOR_SPAWNED, {})

    res = await sch.emit(actor_id=a["actor_id"], msg_type="message", msg_payload={"text": "hi"})

    events = store.list_events(a["actor_id"], since_seq=res["event_seq"])
    assert any(e["event_type"] == "turn.opened" for e in events)


# ---------------------------------------------------------------------------
# Env persistence (actor env + message overlay live in the store)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_env_survives_daemon_restart(env):
    """Actor env and queued message env must survive a scheduler restart."""
    store, bus, sch = env

    r = await sch.spawn(name="worker", backend="pi", env={"SECRET": "s3cr3t"})
    actor_id = r["actor_id"]
    await sch.emit(
        actor_id=actor_id,
        msg_type="message",
        msg_payload={"text": "go"},
        env={"OVERLAY": "v1"},
    )

    # Fresh scheduler over the same store = daemon restart
    cfg = AgentDConfig()
    cfg.limits.max_total_workers = 2
    sch2 = Scheduler(store, bus, cfg)
    rt = _FakeRuntime()
    sch2.set_runtime(rt)  # ty: ignore[invalid-argument-type]
    await sch2.reconcile()
    await asyncio.sleep(0)

    assert len(rt.calls) == 1
    dispatched_env = rt.calls[0]["env"]
    assert dispatched_env["SECRET"] == "s3cr3t"
    assert dispatched_env["OVERLAY"] == "v1"


@pytest.mark.asyncio
async def test_turn_opened_snapshot_has_env_keys_not_values(env):
    """turn.opened input snapshots must never persist env values."""
    store, _bus, sch = env
    rt = _FakeRuntime()
    sch.set_runtime(rt)

    r = await sch.spawn(
        name="worker",
        backend="pi",
        msg_type="message",
        msg_payload={"text": "go"},
        env={"TOKEN": "supersecret"},
    )
    actor_id = r["actor_id"]
    await asyncio.sleep(0)

    # Emit with a turn-level env overlay while the first turn is active,
    # then finish the first turn so the overlay message opens its own turn.
    await sch.emit(
        actor_id=actor_id,
        msg_type="message",
        msg_payload={"text": "more"},
        env={"OVERLAY_TOKEN": "ovval456"},
    )
    first_turn = store.get_active_turn(actor_id)
    await sch.on_turn_completed(first_turn["turn_id"], outcome=TurnOutcome.SUCCEEDED)
    await asyncio.sleep(0)

    events = store.list_events(actor_id, limit=50)
    opened = [e for e in events if e["event_type"] == EventType.TURN_OPENED]
    assert len(opened) == 2
    # Spawn env is actor-level: not part of the turn input at all
    assert opened[0]["payload"]["input"]["env_keys"] == []
    # Emit env overlay: keys recorded, values never
    assert opened[1]["payload"]["input"]["env_keys"] == ["OVERLAY_TOKEN"]
    import json as _json

    raw = _json.dumps(opened)
    assert "supersecret" not in raw
    assert "ovval456" not in raw
