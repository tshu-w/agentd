"""Tests for agentd.store — CRUD, atomic operations, invariants, queries."""

import sqlite3

import pytest

from agentd.protocol import ROOT_SCOPE, ActorState, TurnOutcome, TurnState
from agentd.store.db import SCHEMA_VERSION, Database
from agentd.store.store import Store


@pytest.fixture()
def store(tmp_path):
    db = Database(tmp_path / "test.db")
    db.initialize()
    s = Store(db)
    yield s
    db.close()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def test_schema_creates_all_tables(store):
    conn = store.db.connect()
    tables = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    assert {"actors", "turns", "mailbox", "events", "triggers"}.issubset(tables)


def test_schema_version(store):
    version = store.db.connect().execute("PRAGMA user_version").fetchone()[0]
    assert version == SCHEMA_VERSION


def test_reinitialize_is_idempotent(store):
    store.db.initialize()  # second call should not raise
    version = store.db.connect().execute("PRAGMA user_version").fetchone()[0]
    assert version == SCHEMA_VERSION


# ---------------------------------------------------------------------------
# Actor CRUD
# ---------------------------------------------------------------------------


def test_create_and_get_actor(store):
    result = store.create_actor(name="test", scope_id=ROOT_SCOPE, backend="pi", cwd="/tmp")
    actor_id = result["actor_id"]
    assert actor_id.startswith("act_")
    assert result["state"] == "idle"
    assert result["name"] == "test"

    fetched = store.get_actor(actor_id)
    assert fetched is not None
    assert fetched["actor_id"] == actor_id
    assert fetched["backend"] == "pi"


def test_create_actor_with_checkpoint(store):
    result = store.create_actor(
        name="ckpt",
        scope_id=ROOT_SCOPE,
        backend="pi",
        checkpoint={},
    )
    fetched = store.get_actor(result["actor_id"])
    assert fetched["checkpoint"] == {}


def test_create_actor_checkpoint_disabled(store):
    result = store.create_actor(
        name="nockpt",
        scope_id=ROOT_SCOPE,
        backend="pi",
    )
    fetched = store.get_actor(result["actor_id"])
    assert fetched["checkpoint"] is None


def test_resolve_actor_by_id(store):
    result = store.create_actor(name="r", scope_id=ROOT_SCOPE, backend="pi")
    assert store.resolve_actor(result["actor_id"]) is not None


def test_resolve_actor_by_name(store):
    store.create_actor(name="myname", scope_id=ROOT_SCOPE, backend="pi")
    assert store.resolve_actor("myname") is not None


def test_resolve_actor_not_found(store):
    assert store.resolve_actor("nonexistent") is None


def test_find_actor_excludes_closed(store):
    result = store.create_actor(name="closeme", scope_id=ROOT_SCOPE, backend="pi")
    store.transition_actor(result["actor_id"], ActorState.CLOSED)
    assert store.find_actor_by_name("closeme") is None


# ---------------------------------------------------------------------------
# Actor state transitions
# ---------------------------------------------------------------------------


def test_actor_transition_idle_to_active(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    store.transition_actor(a["actor_id"], ActorState.ACTIVE)
    assert store.get_actor(a["actor_id"])["state"] == "active"


def test_actor_transition_active_to_closed(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    store.transition_actor(a["actor_id"], ActorState.ACTIVE)
    store.transition_actor(a["actor_id"], ActorState.CLOSED)
    fetched = store.get_actor(a["actor_id"])
    assert fetched["state"] == "closed"
    assert fetched["closed_at"] is not None


def test_actor_invalid_transition_raises(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    store.transition_actor(a["actor_id"], ActorState.CLOSED)
    with pytest.raises(ValueError):
        store.transition_actor(a["actor_id"], ActorState.IDLE)


# ---------------------------------------------------------------------------
# Name uniqueness invariant
# ---------------------------------------------------------------------------


def test_duplicate_name_same_scope_raises(store):
    store.create_actor(name="dup", scope_id=ROOT_SCOPE, backend="pi")
    with pytest.raises(sqlite3.IntegrityError):
        store.create_actor(name="dup", scope_id=ROOT_SCOPE, backend="pi")


def test_null_names_are_allowed_multiple(store):
    a1 = store.create_actor(name=None, scope_id=ROOT_SCOPE, backend="pi")
    a2 = store.create_actor(name=None, scope_id=ROOT_SCOPE, backend="pi")
    assert a1["actor_id"] != a2["actor_id"]


def test_same_name_different_scope_ok(store):
    a1 = store.create_actor(name="same", scope_id=ROOT_SCOPE, backend="pi")
    a2 = store.create_actor(name="same", scope_id="other_scope", backend="pi")
    assert a1["actor_id"] != a2["actor_id"]


def test_closed_name_can_be_reused(store):
    a1 = store.create_actor(name="reuse", scope_id=ROOT_SCOPE, backend="pi")
    store.transition_actor(a1["actor_id"], ActorState.CLOSED)
    a2 = store.create_actor(name="reuse", scope_id=ROOT_SCOPE, backend="pi")
    assert a2["actor_id"] != a1["actor_id"]


# ---------------------------------------------------------------------------
# Turn CRUD
# ---------------------------------------------------------------------------


def test_create_and_get_turn(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    turn = store.create_turn(a["actor_id"])
    assert turn["turn_id"].startswith("turn_")
    assert turn["state"] == "pending"

    fetched = store.get_turn(turn["turn_id"])
    assert fetched is not None
    assert fetched["actor_id"] == a["actor_id"]


def test_turn_transition_pending_to_running(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    turn = store.create_turn(a["actor_id"])
    store.transition_turn(turn["turn_id"], TurnState.RUNNING, exec_pid=12345)
    fetched = store.get_turn(turn["turn_id"])
    assert fetched["state"] == "running"
    assert fetched["exec_pid"] == 12345
    assert fetched["started_at"] is not None


def test_turn_transition_running_to_ended(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    turn = store.create_turn(a["actor_id"])
    store.transition_turn(turn["turn_id"], TurnState.RUNNING)
    store.transition_turn(
        turn["turn_id"],
        TurnState.ENDED,
        outcome=TurnOutcome.SUCCEEDED,
        result="done",
    )
    fetched = store.get_turn(turn["turn_id"])
    assert fetched["state"] == "ended"
    assert fetched["outcome"] == "succeeded"
    assert fetched["result"] == "done"
    assert fetched["ended_at"] is not None


def test_turn_invalid_transition_raises(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    turn = store.create_turn(a["actor_id"])
    store.transition_turn(turn["turn_id"], TurnState.ENDED, outcome=TurnOutcome.CANCELED)
    with pytest.raises(ValueError):
        store.transition_turn(turn["turn_id"], TurnState.RUNNING)


def test_get_active_turn(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    assert store.get_active_turn(a["actor_id"]) is None
    turn = store.create_turn(a["actor_id"])
    assert store.get_active_turn(a["actor_id"])["turn_id"] == turn["turn_id"]


# ---------------------------------------------------------------------------
# One active turn per actor invariant
# ---------------------------------------------------------------------------


def test_one_active_turn_per_actor(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    store.create_turn(a["actor_id"])
    with pytest.raises(sqlite3.IntegrityError):
        store.create_turn(a["actor_id"])


# ---------------------------------------------------------------------------
# Mailbox
# ---------------------------------------------------------------------------


def test_add_and_claim_message(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    msg = store.add_message(a["actor_id"], "message", {"text": "hello"})
    assert msg["message_id"].startswith("msg_")
    assert store.count_queued(a["actor_id"]) == 1

    claimed = store.claim_oldest_message(a["actor_id"])
    assert claimed["message_id"] == msg["message_id"]
    assert claimed["payload"] == {"text": "hello"}
    assert claimed["state"] == "claimed"
    # Claimed message no longer counts as queued
    assert store.count_queued(a["actor_id"]) == 0


def test_get_claimed_message(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    assert store.get_claimed_message(a["actor_id"]) is None

    msg = store.add_message(a["actor_id"], "message", {"text": "hello"})
    assert store.get_claimed_message(a["actor_id"]) is None  # still queued

    store.claim_oldest_message(a["actor_id"])
    claimed = store.get_claimed_message(a["actor_id"])
    assert claimed is not None
    assert claimed["message_id"] == msg["message_id"]

    # After ack, no longer claimed / queued
    store.ack_message(msg["message_id"])
    assert store.get_claimed_message(a["actor_id"]) is None
    assert store.count_queued(a["actor_id"]) == 0


def test_ack_message(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    msg = store.add_message(a["actor_id"], "message", {"text": "hello"})
    store.ack_message(msg["message_id"])
    assert store.count_queued(a["actor_id"]) == 0


def test_fifo_order(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    m1 = store.add_message(a["actor_id"], "message", {"text": "first"})
    store.add_message(a["actor_id"], "message", {"text": "second"})

    claimed = store.claim_oldest_message(a["actor_id"])
    assert claimed["message_id"] == m1["message_id"]


def test_claim_empty_returns_none(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    assert store.claim_oldest_message(a["actor_id"]) is None


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------


def test_append_and_list_events(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    seq1 = store.append_event(a["actor_id"], "actor.spawned", {"name": "t"})
    seq2 = store.append_event(a["actor_id"], "turn.opened", {"turn_id": "t1"})
    assert seq2 > seq1

    events = store.list_events(a["actor_id"])
    assert len(events) == 2
    assert events[0]["event_type"] == "actor.spawned"


def test_list_events_since_seq(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    seq1 = store.append_event(a["actor_id"], "e1", {})
    seq2 = store.append_event(a["actor_id"], "e2", {})

    events = store.list_events(a["actor_id"], since_seq=seq1)
    assert len(events) == 1
    assert events[0]["seq"] == seq2


def test_get_max_seq(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    assert store.get_max_seq() == 0
    store.append_event(a["actor_id"], "e1", {})
    assert store.get_max_seq() > 0


# ---------------------------------------------------------------------------
# Triggers
# ---------------------------------------------------------------------------


def test_add_and_list_triggers(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    t = store.add_trigger(
        a["actor_id"],
        "cron",
        {"schedule": "* * * * *"},
        "message",
        {"text": "tick"},
    )
    assert t["trigger_id"].startswith("trig_")

    triggers = store.list_triggers(a["actor_id"])
    assert len(triggers) == 1
    assert triggers[0]["spec"]["schedule"] == "* * * * *"


def test_delete_trigger(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    t = store.add_trigger(a["actor_id"], "cron", {}, "message", {})
    assert store.delete_trigger(t["trigger_id"]) is True
    assert store.list_triggers(a["actor_id"]) == []


def test_delete_triggers_for_actor(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    store.add_trigger(a["actor_id"], "cron", {}, "message", {})
    store.add_trigger(a["actor_id"], "cron", {}, "message", {})
    deleted = store.delete_triggers_for_actor(a["actor_id"])
    assert deleted == 2


# ---------------------------------------------------------------------------
# Atomic operations
# ---------------------------------------------------------------------------


def test_open_turn_atomic(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    store.add_message(a["actor_id"], "message", {"text": "go"})

    result = store.open_turn_atomic(a["actor_id"])
    assert result is not None
    turn, msg = result
    assert turn["state"] == "pending"
    assert msg["payload"] == {"text": "go"}
    assert msg["state"] == "claimed"

    actor = store.get_actor(a["actor_id"])
    assert actor["state"] == "active"
    # Message is claimed, not queued
    assert store.count_queued(a["actor_id"]) == 0
    assert store.get_claimed_message(a["actor_id"]) is not None


def test_open_turn_atomic_no_message(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    assert store.open_turn_atomic(a["actor_id"]) is None


def test_end_turn_atomic(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    store.add_message(a["actor_id"], "message", {"text": "go"})
    turn, msg = store.open_turn_atomic(a["actor_id"])

    store.transition_turn(turn["turn_id"], TurnState.RUNNING)
    store.end_turn_atomic(
        turn["turn_id"],
        a["actor_id"],
        outcome=TurnOutcome.SUCCEEDED,
        result="done",
    )

    actor = store.get_actor(a["actor_id"])
    assert actor["state"] == "idle"

    fetched_turn = store.get_turn(turn["turn_id"])
    assert fetched_turn["state"] == "ended"
    assert fetched_turn["outcome"] == "succeeded"

    assert store.count_queued(a["actor_id"]) == 0


def test_end_turn_atomic_with_close(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    store.add_message(a["actor_id"], "message", {"text": "go"})
    turn, msg = store.open_turn_atomic(a["actor_id"])

    store.end_turn_atomic(
        turn["turn_id"],
        a["actor_id"],
        outcome=TurnOutcome.CANCELED,
        close_actor=True,
    )

    actor = store.get_actor(a["actor_id"])
    assert actor["state"] == "closed"


# ---------------------------------------------------------------------------
# Subtree
# ---------------------------------------------------------------------------


def test_list_subtree(store):
    parent = store.create_actor(name="parent", scope_id=ROOT_SCOPE, backend="pi")
    child = store.create_actor(
        name="child",
        scope_id=parent["actor_id"],
        parent_actor_id=parent["actor_id"],
        backend="pi",
    )
    grandchild = store.create_actor(
        name="gc",
        scope_id=child["actor_id"],
        parent_actor_id=child["actor_id"],
        backend="pi",
    )

    tree = store.list_subtree(parent["actor_id"])
    ids = [a["actor_id"] for a in tree]
    assert len(ids) == 3
    # Deepest first
    assert ids[0] == grandchild["actor_id"]
    assert ids[-1] == parent["actor_id"]


def test_count_children(store):
    parent = store.create_actor(name="p", scope_id=ROOT_SCOPE, backend="pi")
    store.create_actor(
        name="c1",
        scope_id=parent["actor_id"],
        parent_actor_id=parent["actor_id"],
        backend="pi",
    )
    store.create_actor(
        name="c2",
        scope_id=parent["actor_id"],
        parent_actor_id=parent["actor_id"],
        backend="pi",
    )
    assert store.count_children(parent["actor_id"]) == 2


def test_actor_depth(store):
    p = store.create_actor(name="p", scope_id=ROOT_SCOPE, backend="pi")
    c = store.create_actor(
        name="c",
        scope_id=p["actor_id"],
        parent_actor_id=p["actor_id"],
        backend="pi",
    )
    assert store.actor_depth(p["actor_id"]) == 0
    assert store.actor_depth(c["actor_id"]) == 1


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------


def test_update_checkpoint(store):
    a = store.create_actor(
        name="t",
        scope_id=ROOT_SCOPE,
        backend="pi",
        checkpoint={},
    )
    store.update_checkpoint(a["actor_id"], {"session_id": "abc123"})
    fetched = store.get_actor(a["actor_id"])
    assert fetched["checkpoint"] == {"session_id": "abc123"}


# ---------------------------------------------------------------------------
# Reconciliation helpers
# ---------------------------------------------------------------------------


def test_list_running_turns(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    turn = store.create_turn(a["actor_id"])
    store.transition_turn(turn["turn_id"], TurnState.RUNNING)

    running = store.list_running_turns()
    assert len(running) == 1
    assert running[0]["turn_id"] == turn["turn_id"]


def test_list_idle_actors_with_queued(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    assert store.list_idle_actors_with_queued() == []

    store.add_message(a["actor_id"], "message", {"text": "wake"})
    wakeup = store.list_idle_actors_with_queued()
    assert len(wakeup) == 1
    assert wakeup[0]["actor_id"] == a["actor_id"]


def test_claimed_message_not_in_idle_with_queued(store):
    """Claimed messages don't trigger idle-with-queued wakeup."""
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    store.add_message(a["actor_id"], "message", {"text": "hello"})
    store.claim_oldest_message(a["actor_id"])
    # Message is claimed, not queued → no wakeup
    assert store.list_idle_actors_with_queued() == []


def test_end_turn_atomic_acks_claimed_message(store):
    """end_turn_atomic properly acks a claimed message."""
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    store.add_message(a["actor_id"], "message", {"text": "go"})
    turn, msg = store.open_turn_atomic(a["actor_id"])
    store.transition_turn(turn["turn_id"], TurnState.RUNNING)

    # Verify claimed state
    assert store.get_claimed_message(a["actor_id"]) is not None

    store.end_turn_atomic(
        turn["turn_id"],
        a["actor_id"],
        outcome=TurnOutcome.SUCCEEDED,
    )

    # After end, message is acked (not claimed, not queued)
    assert store.get_claimed_message(a["actor_id"]) is None
    assert store.count_queued(a["actor_id"]) == 0


# ---------------------------------------------------------------------------
# Env persistence
# ---------------------------------------------------------------------------


def test_actor_env_round_trip(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi", env={"ACTOR_VAR": "a1"})
    got = store.get_actor(a["actor_id"])
    assert got["env"] == {"ACTOR_VAR": "a1"}

    plain = store.create_actor(name="p", scope_id=ROOT_SCOPE, backend="pi")
    assert store.get_actor(plain["actor_id"])["env"] == {}


def test_message_env_cleared_on_ack(store):
    a = store.create_actor(name="t", scope_id=ROOT_SCOPE, backend="pi")
    msg = store.add_message(a["actor_id"], "message", {"text": "go"}, env={"OVERLAY": "v1"})
    turn, claimed = store.open_turn_atomic(a["actor_id"])
    assert claimed["env"] == {"OVERLAY": "v1"}

    store.end_turn_atomic(turn["turn_id"], a["actor_id"], outcome=TurnOutcome.SUCCEEDED)

    row = (
        store.db.connect()
        .execute("SELECT env, state FROM mailbox WHERE message_id = ?", (msg["message_id"],))
        .fetchone()
    )
    assert row["state"] == "acked"
    assert row["env"] is None


def test_migration_v1_to_v2(tmp_path):
    """A v1 database is upgraded in place: env columns added, event env scrubbed."""
    import json

    p = tmp_path / "old.db"
    conn = sqlite3.connect(p)
    conn.executescript(
        """
        CREATE TABLE actors(
            actor_id TEXT PRIMARY KEY, name TEXT, scope_id TEXT NOT NULL,
            parent_actor_id TEXT, backend TEXT NOT NULL,
            backend_args TEXT NOT NULL DEFAULT '[]', cwd TEXT,
            state TEXT NOT NULL DEFAULT 'idle', checkpoint TEXT,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE mailbox(
            message_id TEXT PRIMARY KEY, actor_id TEXT NOT NULL,
            message_type TEXT NOT NULL, payload TEXT NOT NULL DEFAULT '{}',
            state TEXT NOT NULL DEFAULT 'queued', created_at TEXT NOT NULL,
            acked_at TEXT
        );
        CREATE TABLE turns(
            turn_id TEXT PRIMARY KEY, actor_id TEXT NOT NULL,
            state TEXT NOT NULL, outcome TEXT, result TEXT, error TEXT,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE events(
            seq INTEGER PRIMARY KEY AUTOINCREMENT, actor_id TEXT NOT NULL,
            turn_id TEXT, event_type TEXT NOT NULL,
            payload TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL
        );
        CREATE TABLE triggers(
            trigger_id TEXT PRIMARY KEY, actor_id TEXT NOT NULL,
            kind TEXT NOT NULL, spec TEXT NOT NULL DEFAULT '{}',
            payload TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL
        );
        PRAGMA user_version = 1;
        """
    )
    leaked = json.dumps(
        {"turn_id": "turn_x", "input": {"messages": [], "env": {"TOKEN": "tokval123"}}}
    )
    conn.execute(
        "INSERT INTO events(actor_id, turn_id, event_type, payload, created_at)"
        " VALUES ('act_x', 'turn_x', 'turn.opened', ?, '2026-01-01T00:00:00Z')",
        (leaked,),
    )
    conn.commit()
    conn.close()

    db = Database(p)
    db.initialize()
    c = db.connect()
    assert c.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    actor_cols = {r[1] for r in c.execute("PRAGMA table_info(actors)")}
    mailbox_cols = {r[1] for r in c.execute("PRAGMA table_info(mailbox)")}
    assert "env" in actor_cols
    assert "env" in mailbox_cols

    payload = c.execute("SELECT payload FROM events WHERE turn_id = 'turn_x'").fetchone()[0]
    assert "tokval123" not in payload
