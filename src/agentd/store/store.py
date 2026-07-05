"""SQLite persistence layer for agentd.

Single-writer model: only the daemon process should call write methods.
Read methods are safe for concurrent readers (WAL mode).
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from typing import Any, overload

from agentd.protocol import (
    ROOT_SCOPE,
    ActorState,
    TurnOutcome,
    TurnState,
    gen_id,
    is_actor_ref_by_id,
    validate_actor_transition,
    validate_turn_transition,
)

from .db import Database, utc_now


class Store:
    def __init__(self, db: Database):
        self.db = db

    def initialize(self) -> None:
        self.db.initialize()

    @contextmanager
    def transaction(self):
        with self.db.transaction() as conn:
            yield conn

    # -- helpers ------------------------------------------------------------

    @staticmethod
    def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return dict(row)

    @staticmethod
    def _parse_json(val: str | None, default: Any = None) -> Any:
        if val is None:
            return default
        return json.loads(val)

    # Row-to-dict helpers. Overloads let the type checker narrow the result
    # based on whether the input row is None: callers passing a non-None row
    # (e.g. inside list comprehensions over fetchall()) get a non-optional dict
    # without sprinkling asserts/casts.

    @overload
    def _actor_dict(self, row: sqlite3.Row) -> dict[str, Any]: ...
    @overload
    def _actor_dict(self, row: None) -> None: ...
    @overload
    def _actor_dict(self, row: sqlite3.Row | None) -> dict[str, Any] | None: ...
    def _actor_dict(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        d = dict(row)
        d["backend_args"] = self._parse_json(d.pop("backend_args", "[]"), [])
        d["checkpoint"] = self._parse_json(d.get("checkpoint"), None)
        d["env"] = self._parse_json(d.get("env", "{}"), {})
        return d

    @overload
    def _turn_dict(self, row: sqlite3.Row) -> dict[str, Any]: ...
    @overload
    def _turn_dict(self, row: None) -> None: ...
    @overload
    def _turn_dict(self, row: sqlite3.Row | None) -> dict[str, Any] | None: ...
    def _turn_dict(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return dict(row)

    @overload
    def _msg_dict(self, row: sqlite3.Row) -> dict[str, Any]: ...
    @overload
    def _msg_dict(self, row: None) -> None: ...
    @overload
    def _msg_dict(self, row: sqlite3.Row | None) -> dict[str, Any] | None: ...
    def _msg_dict(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        d = dict(row)
        d["payload"] = self._parse_json(d.get("payload", "{}"), {})
        d["env"] = self._parse_json(d["env"], None) if d.get("env") else None
        return d

    @overload
    def _event_dict(self, row: sqlite3.Row) -> dict[str, Any]: ...
    @overload
    def _event_dict(self, row: None) -> None: ...
    @overload
    def _event_dict(self, row: sqlite3.Row | None) -> dict[str, Any] | None: ...
    def _event_dict(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        d = dict(row)
        d["payload"] = self._parse_json(d.get("payload", "{}"), {})
        return d

    @overload
    def _trigger_dict(self, row: sqlite3.Row) -> dict[str, Any]: ...
    @overload
    def _trigger_dict(self, row: None) -> None: ...
    @overload
    def _trigger_dict(self, row: sqlite3.Row | None) -> dict[str, Any] | None: ...
    def _trigger_dict(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        d = dict(row)
        d["spec"] = self._parse_json(d.get("spec", "{}"), {})
        d["payload"] = self._parse_json(d.get("payload", "{}"), {})
        return d

    # ======================================================================
    # Actor CRUD
    # ======================================================================

    def create_actor(
        self,
        *,
        name: str | None,
        scope_id: str,
        backend: str,
        parent_actor_id: str | None = None,
        backend_args: list[str] | None = None,
        cwd: str | None = None,
        state: ActorState = ActorState.IDLE,
        checkpoint: dict | None = None,
        env: dict[str, str] | None = None,
        conn: sqlite3.Connection | None = None,
    ) -> dict[str, Any]:
        actor_id = gen_id("act")
        now = utc_now()
        c = conn or self.db.connect()
        c.execute(
            """INSERT INTO actors(
                actor_id, name, scope_id, parent_actor_id,
                backend, backend_args, cwd, state, checkpoint, env,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                actor_id,
                name,
                scope_id,
                parent_actor_id,
                backend,
                json.dumps(backend_args or []),
                cwd,
                state.value,
                json.dumps(checkpoint) if checkpoint is not None else None,
                json.dumps(env or {}),
                now,
                now,
            ),
        )
        return {
            "actor_id": actor_id,
            "name": name,
            "scope_id": scope_id,
            "parent_actor_id": parent_actor_id,
            "backend": backend,
            "backend_args": backend_args or [],
            "cwd": cwd,
            "state": state.value,
            "checkpoint": checkpoint,
            "env": env or {},
            "created_at": now,
            "updated_at": now,
            "closed_at": None,
        }

    def get_actor(self, actor_id: str) -> dict[str, Any] | None:
        row = (
            self.db.connect()
            .execute("SELECT * FROM actors WHERE actor_id = ?", (actor_id,))
            .fetchone()
        )
        return self._actor_dict(row)

    def resolve_actor(self, ref: str) -> dict[str, Any] | None:
        """Resolve actor by id (act_ prefix) or by name in root scope."""
        if is_actor_ref_by_id(ref):
            return self.get_actor(ref)
        return self.find_actor_by_name(ref)

    def find_actor_by_name(self, name: str, scope_id: str = ROOT_SCOPE) -> dict[str, Any] | None:
        row = (
            self.db.connect()
            .execute(
                "SELECT * FROM actors WHERE name = ? AND scope_id = ? AND state != 'closed'",
                (name, scope_id),
            )
            .fetchone()
        )
        return self._actor_dict(row)

    def count_children(self, parent_actor_id: str) -> int:
        row = (
            self.db.connect()
            .execute(
                "SELECT COUNT(*) AS c FROM actors WHERE parent_actor_id = ? AND state != 'closed'",
                (parent_actor_id,),
            )
            .fetchone()
        )
        return int(row["c"]) if row else 0

    def actor_depth(self, actor_id: str) -> int:
        """Compute depth of actor in the tree (root = 0)."""
        depth = 0
        current = self.get_actor(actor_id)
        while current and current.get("parent_actor_id"):
            depth += 1
            current = self.get_actor(current["parent_actor_id"])
        return depth

    def list_actors(
        self,
        *,
        include_terminal: bool = False,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM actors"
        params: list[Any] = []
        if not include_terminal:
            sql += " WHERE state != 'closed'"
        sql += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)
        rows = self.db.connect().execute(sql, params).fetchall()
        return [self._actor_dict(r) for r in rows]

    def list_subtree(self, actor_id: str, include_root: bool = True) -> list[dict[str, Any]]:
        """List all descendants of actor_id, deepest first."""
        rows = (
            self.db.connect()
            .execute(
                """
            WITH RECURSIVE subtree(aid, depth) AS (
                SELECT actor_id, 0 FROM actors WHERE actor_id = ?
                UNION ALL
                SELECT a.actor_id, s.depth + 1
                FROM actors a JOIN subtree s ON a.parent_actor_id = s.aid
            )
            SELECT a.* FROM actors a
            JOIN subtree s ON s.aid = a.actor_id
            ORDER BY s.depth DESC, a.created_at DESC
            """,
                (actor_id,),
            )
            .fetchall()
        )
        result = [self._actor_dict(r) for r in rows]
        if not include_root:
            result = [a for a in result if a["actor_id"] != actor_id]
        return result

    def transition_actor(
        self,
        actor_id: str,
        new_state: ActorState,
        *,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        c = conn or self.db.connect()
        row = c.execute("SELECT state FROM actors WHERE actor_id = ?", (actor_id,)).fetchone()
        if row is None:
            raise ValueError(f"actor not found: {actor_id}")
        current = ActorState(row["state"])
        validate_actor_transition(current, new_state)
        now = utc_now()
        updates = "state = ?, updated_at = ?"
        params: list[Any] = [new_state.value, now]
        if new_state == ActorState.CLOSED:
            updates += ", closed_at = ?"
            params.append(now)
        params.append(actor_id)
        c.execute(f"UPDATE actors SET {updates} WHERE actor_id = ?", params)

    def update_checkpoint(
        self,
        actor_id: str,
        data: dict[str, Any],
        *,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        c = conn or self.db.connect()
        c.execute(
            "UPDATE actors SET checkpoint = ?, updated_at = ? WHERE actor_id = ?",
            (json.dumps(data), utc_now(), actor_id),
        )

    # ======================================================================
    # Turn CRUD
    # ======================================================================

    def create_turn(
        self,
        actor_id: str,
        *,
        conn: sqlite3.Connection | None = None,
    ) -> dict[str, Any]:
        turn_id = gen_id("turn")
        now = utc_now()
        c = conn or self.db.connect()
        c.execute(
            "INSERT INTO turns(turn_id, actor_id, state, created_at) VALUES (?, ?, ?, ?)",
            (turn_id, actor_id, TurnState.PENDING.value, now),
        )
        return {
            "turn_id": turn_id,
            "actor_id": actor_id,
            "state": TurnState.PENDING.value,
            "exec_pid": None,
            "result": None,
            "outcome": None,
            "error": None,
            "created_at": now,
            "started_at": None,
            "ended_at": None,
        }

    def get_turn(self, turn_id: str) -> dict[str, Any] | None:
        row = (
            self.db.connect()
            .execute("SELECT * FROM turns WHERE turn_id = ?", (turn_id,))
            .fetchone()
        )
        return self._turn_dict(row)

    def get_active_turn(self, actor_id: str) -> dict[str, Any] | None:
        row = (
            self.db.connect()
            .execute(
                "SELECT * FROM turns WHERE actor_id = ? AND state IN ('pending', 'running')",
                (actor_id,),
            )
            .fetchone()
        )
        return self._turn_dict(row)

    def get_last_turn(self, actor_id: str) -> dict[str, Any] | None:
        row = (
            self.db.connect()
            .execute(
                "SELECT * FROM turns WHERE actor_id = ? ORDER BY created_at DESC LIMIT 1",
                (actor_id,),
            )
            .fetchone()
        )
        return self._turn_dict(row)

    def transition_turn(
        self,
        turn_id: str,
        new_state: TurnState,
        *,
        outcome: TurnOutcome | None = None,
        result: str | None = None,
        error: str | None = None,
        exec_pid: int | None = None,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        c = conn or self.db.connect()
        row = c.execute("SELECT state FROM turns WHERE turn_id = ?", (turn_id,)).fetchone()
        if row is None:
            raise ValueError(f"turn not found: {turn_id}")
        current = TurnState(row["state"])
        validate_turn_transition(current, new_state)
        now = utc_now()
        sets: list[str] = ["state = ?"]
        params: list[Any] = [new_state.value]
        if new_state == TurnState.RUNNING:
            sets.append("started_at = ?")
            params.append(now)
            if exec_pid is not None:
                sets.append("exec_pid = ?")
                params.append(exec_pid)
        if new_state == TurnState.ENDED:
            sets.append("ended_at = ?")
            params.append(now)
            if outcome is not None:
                sets.append("outcome = ?")
                params.append(outcome.value)
            if result is not None:
                sets.append("result = ?")
                params.append(result)
            if error is not None:
                sets.append("error = ?")
                params.append(error)
        params.append(turn_id)
        c.execute(f"UPDATE turns SET {', '.join(sets)} WHERE turn_id = ?", params)

    # ======================================================================
    # Mailbox
    # ======================================================================

    def add_message(
        self,
        actor_id: str,
        message_type: str,
        payload: dict[str, Any],
        *,
        env: dict[str, str] | None = None,
        conn: sqlite3.Connection | None = None,
    ) -> dict[str, Any]:
        message_id = gen_id("msg")
        now = utc_now()
        c = conn or self.db.connect()
        c.execute(
            """INSERT INTO mailbox(message_id, actor_id, message_type, payload, env,
                                   state, created_at)
               VALUES (?, ?, ?, ?, ?, 'queued', ?)""",
            (
                message_id,
                actor_id,
                message_type,
                json.dumps(payload),
                json.dumps(env) if env else None,
                now,
            ),
        )
        return {
            "message_id": message_id,
            "actor_id": actor_id,
            "message_type": message_type,
            "payload": payload,
            "env": env,
            "state": "queued",
            "created_at": now,
            "acked_at": None,
        }

    def claim_oldest_message(
        self,
        actor_id: str,
        *,
        conn: sqlite3.Connection | None = None,
    ) -> dict[str, Any] | None:
        """Claim the oldest queued message for an actor (FIFO).

        Sets state from 'queued' to 'claimed'. The message stays 'claimed'
        until acked by end_turn_atomic.
        """
        c = conn or self.db.connect()
        row = c.execute(
            """SELECT * FROM mailbox
               WHERE actor_id = ? AND state = 'queued'
               ORDER BY created_at, message_id LIMIT 1""",
            (actor_id,),
        ).fetchone()
        if row is None:
            return None
        msg = self._msg_dict(row)
        assert msg is not None  # row is non-None, so msg is non-None
        c.execute(
            "UPDATE mailbox SET state = 'claimed' WHERE message_id = ?",
            (msg["message_id"],),
        )
        msg["state"] = "claimed"
        return msg

    def get_claimed_message(
        self,
        actor_id: str,
        *,
        conn: sqlite3.Connection | None = None,
    ) -> dict[str, Any] | None:
        """Find the message currently claimed (but not yet acked) for an actor."""
        c = conn or self.db.connect()
        row = c.execute(
            """SELECT * FROM mailbox
               WHERE actor_id = ? AND state = 'claimed'
               ORDER BY created_at, message_id LIMIT 1""",
            (actor_id,),
        ).fetchone()
        return self._msg_dict(row)

    def ack_message(
        self,
        message_id: str,
        *,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        """Ack a message (claimed → acked, or queued → acked).

        Clears env so secrets only rest in the DB while a message is
        queued/claimed — acked history keeps type and payload only.
        """
        c = conn or self.db.connect()
        c.execute(
            "UPDATE mailbox SET state = 'acked', acked_at = ?, env = NULL"
            " WHERE message_id = ? AND state != 'acked'",
            (utc_now(), message_id),
        )

    def count_queued(self, actor_id: str) -> int:
        row = (
            self.db.connect()
            .execute(
                "SELECT COUNT(*) AS c FROM mailbox WHERE actor_id = ? AND state = 'queued'",
                (actor_id,),
            )
            .fetchone()
        )
        return int(row["c"]) if row else 0

    def get_queued_messages(
        self,
        actor_id: str,
        *,
        conn: sqlite3.Connection | None = None,
    ) -> list[dict[str, Any]]:
        c = conn or self.db.connect()
        rows = c.execute(
            """SELECT * FROM mailbox
               WHERE actor_id = ? AND state = 'queued'
               ORDER BY created_at, message_id""",
            (actor_id,),
        ).fetchall()
        return [self._msg_dict(r) for r in rows]

    # ======================================================================
    # Events (append-only log)
    # ======================================================================

    def append_event(
        self,
        actor_id: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        turn_id: str | None = None,
        conn: sqlite3.Connection | None = None,
    ) -> int:
        """Append event and return its seq number."""
        now = utc_now()
        c = conn or self.db.connect()
        cur = c.execute(
            """INSERT INTO events(actor_id, turn_id, event_type, payload, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (actor_id, turn_id, event_type, json.dumps(payload), now),
        )
        last_id = cur.lastrowid
        assert last_id is not None  # INSERT always assigns a rowid
        return last_id

    def list_events(
        self,
        actor_id: str,
        *,
        since_seq: int = 0,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        rows = (
            self.db.connect()
            .execute(
                "SELECT * FROM events WHERE actor_id = ? AND seq > ? ORDER BY seq LIMIT ?",
                (actor_id, since_seq, limit),
            )
            .fetchall()
        )
        return [self._event_dict(r) for r in rows]

    def list_events_by_turn(
        self,
        turn_id: str,
        *,
        since_seq: int = 0,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        rows = (
            self.db.connect()
            .execute(
                "SELECT * FROM events WHERE turn_id = ? AND seq > ? ORDER BY seq LIMIT ?",
                (turn_id, since_seq, limit),
            )
            .fetchall()
        )
        return [self._event_dict(r) for r in rows]

    def get_max_seq(self) -> int:
        row = self.db.connect().execute("SELECT MAX(seq) AS m FROM events").fetchone()
        return int(row["m"]) if row and row["m"] is not None else 0

    # ======================================================================
    # Triggers
    # ======================================================================

    def add_trigger(
        self,
        target_actor_id: str,
        kind: str,
        spec: dict[str, Any],
        message_type: str,
        payload: dict[str, Any],
        next_fire_at: str | None = None,
    ) -> dict[str, Any]:
        trigger_id = gen_id("trig")
        now = utc_now()
        self.db.connect().execute(
            """INSERT INTO triggers(
                trigger_id, target_actor_id, kind, spec,
                message_type, payload, next_fire_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                trigger_id,
                target_actor_id,
                kind,
                json.dumps(spec),
                message_type,
                json.dumps(payload),
                next_fire_at,
                now,
            ),
        )
        return {
            "trigger_id": trigger_id,
            "target_actor_id": target_actor_id,
            "kind": kind,
            "spec": spec,
            "message_type": message_type,
            "payload": payload,
            "next_fire_at": next_fire_at,
            "created_at": now,
        }

    def get_trigger(self, trigger_id: str) -> dict[str, Any] | None:
        row = (
            self.db.connect()
            .execute("SELECT * FROM triggers WHERE trigger_id = ?", (trigger_id,))
            .fetchone()
        )
        return self._trigger_dict(row)

    def list_triggers(self, actor_id: str | None = None) -> list[dict[str, Any]]:
        if actor_id:
            rows = (
                self.db.connect()
                .execute("SELECT * FROM triggers WHERE target_actor_id = ?", (actor_id,))
                .fetchall()
            )
        else:
            rows = self.db.connect().execute("SELECT * FROM triggers").fetchall()
        return [self._trigger_dict(r) for r in rows]

    def delete_trigger(self, trigger_id: str) -> bool:
        cur = self.db.connect().execute("DELETE FROM triggers WHERE trigger_id = ?", (trigger_id,))
        return cur.rowcount > 0

    def delete_triggers_for_actor(
        self,
        actor_id: str,
        *,
        conn: sqlite3.Connection | None = None,
    ) -> int:
        c = conn or self.db.connect()
        cur = c.execute("DELETE FROM triggers WHERE target_actor_id = ?", (actor_id,))
        return cur.rowcount

    def list_due_triggers(self, now: str) -> list[dict[str, Any]]:
        rows = (
            self.db.connect()
            .execute(
                "SELECT * FROM triggers WHERE next_fire_at IS NOT NULL AND next_fire_at <= ?",
                (now,),
            )
            .fetchall()
        )
        return [self._trigger_dict(r) for r in rows]

    def update_trigger_next_fire(self, trigger_id: str, next_fire_at: str) -> None:
        self.db.connect().execute(
            "UPDATE triggers SET next_fire_at = ? WHERE trigger_id = ?",
            (next_fire_at, trigger_id),
        )

    # ======================================================================
    # Composite atomic operations (for scheduler)
    # ======================================================================

    def open_turn_atomic(
        self,
        actor_id: str,
    ) -> tuple[dict[str, Any], dict[str, Any]] | None:
        """Atomically: claim oldest queued message, create pending turn, actor → active.

        Returns (turn, message) or None if no queued message.
        Must be called within an outer transaction or auto-commits.
        """
        with self.db.transaction() as conn:
            msg = self.claim_oldest_message(actor_id, conn=conn)
            if msg is None:
                return None
            turn = self.create_turn(actor_id, conn=conn)
            self.transition_actor(actor_id, ActorState.ACTIVE, conn=conn)
            return turn, msg

    def end_turn_atomic(
        self,
        turn_id: str,
        actor_id: str,
        *,
        outcome: TurnOutcome,
        result: str | None = None,
        error: str | None = None,
        close_actor: bool = False,
    ) -> None:
        """Atomically: end turn, ack the claimed message, actor → idle (or closed).

        The message to ack is resolved inside the transaction: one active
        turn per actor implies at most one claimed message per actor.
        """
        with self.db.transaction() as conn:
            self.transition_turn(
                turn_id,
                TurnState.ENDED,
                outcome=outcome,
                result=result,
                error=error,
                conn=conn,
            )
            claimed = self.get_claimed_message(actor_id, conn=conn)
            if claimed:
                self.ack_message(claimed["message_id"], conn=conn)
            target = ActorState.CLOSED if close_actor else ActorState.IDLE
            self.transition_actor(actor_id, target, conn=conn)

    # ======================================================================
    # Reconciliation helpers
    # ======================================================================

    def list_running_turns(self) -> list[dict[str, Any]]:
        rows = self.db.connect().execute("SELECT * FROM turns WHERE state = 'running'").fetchall()
        return [self._turn_dict(r) for r in rows]

    def list_pending_turns(self) -> list[dict[str, Any]]:
        rows = self.db.connect().execute("SELECT * FROM turns WHERE state = 'pending'").fetchall()
        return [self._turn_dict(r) for r in rows]

    def list_active_actors(self) -> list[dict[str, Any]]:
        rows = self.db.connect().execute("SELECT * FROM actors WHERE state = 'active'").fetchall()
        return [self._actor_dict(r) for r in rows]

    def list_idle_actors_with_queued(self) -> list[dict[str, Any]]:
        """Find idle actors that have queued messages (need wakeup)."""
        rows = (
            self.db.connect()
            .execute(
                """SELECT a.* FROM actors a
               WHERE a.state = 'idle'
               AND EXISTS (
                   SELECT 1 FROM mailbox m
                   WHERE m.actor_id = a.actor_id AND m.state = 'queued'
               )"""
            )
            .fetchall()
        )
        return [self._actor_dict(r) for r in rows]
