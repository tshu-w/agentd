"""Low-level SQLite database wrapper."""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path

SCHEMA_VERSION = 2

# version -> statements upgrading from version-1
MIGRATIONS: dict[int, list[str]] = {
    2: [
        "ALTER TABLE actors ADD COLUMN env TEXT NOT NULL DEFAULT '{}'",
        "ALTER TABLE mailbox ADD COLUMN env TEXT",
        # v1 persisted env overlay values inside turn.opened input snapshots;
        # scrub them — secrets don't belong in the append-only event log.
        "UPDATE events SET payload = json_remove(payload, '$.input.env')"
        " WHERE event_type = 'turn.opened'"
        " AND json_extract(payload, '$.input.env') IS NOT NULL",
    ],
}


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class Database:
    def __init__(self, path: Path):
        self.path = path.expanduser()
        self._conn: sqlite3.Connection | None = None

    def connect(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            self.path,
            timeout=10,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        # Restrict before WAL mode: SQLite copies the main db file's
        # permissions when creating -wal/-shm.
        self._restrict_permissions()
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        self._conn = conn
        return conn

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        else:
            if conn.in_transaction:
                conn.execute("COMMIT")

    def close(self) -> None:
        if self._conn is None:
            return
        try:
            self._conn.close()
        finally:
            self._conn = None

    def initialize(self) -> None:
        conn = self.connect()
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        required = {"actors", "turns", "events", "mailbox", "triggers"}
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if required.issubset(tables):
            if version == SCHEMA_VERSION:
                return
            if version < SCHEMA_VERSION:
                self._migrate(conn, version)
                return
            raise RuntimeError(f"unsupported schema version: {version} (expected {SCHEMA_VERSION})")
        # Stale/incompatible DB: has some tables but not the right set
        if tables - {"sqlite_sequence"} and not required.issubset(tables):
            raise RuntimeError(
                f"incompatible database (found tables: {sorted(tables - {'sqlite_sequence'})}). "
                f"Back up and remove {self.path} to start fresh."
            )
        schema_sql = (Path(__file__).parent / "schema.sql").read_text(encoding="utf-8")
        conn.executescript(
            f"BEGIN IMMEDIATE;\n{schema_sql}\nPRAGMA user_version = {SCHEMA_VERSION};\nCOMMIT;\n"
        )

    def _migrate(self, conn: sqlite3.Connection, from_version: int) -> None:
        for version in range(from_version + 1, SCHEMA_VERSION + 1):
            statements = MIGRATIONS.get(version)
            if statements is None:
                raise RuntimeError(f"no migration path to schema version {version}")
            script = ";\n".join(statements)
            conn.executescript(
                f"BEGIN IMMEDIATE;\n{script};\nPRAGMA user_version = {version};\nCOMMIT;\n"
            )

    def _restrict_permissions(self) -> None:
        for suffix in ("", "-wal", "-shm"):
            p = Path(f"{self.path}{suffix}")
            if p.exists():
                with suppress(OSError):
                    os.chmod(p, 0o600)

    def __del__(self) -> None:
        with suppress(Exception):
            self.close()
