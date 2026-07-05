-- agentd schema (see SCHEMA_VERSION in db.py; changes need a migration there)

CREATE TABLE actors (
    actor_id        TEXT PRIMARY KEY,
    name            TEXT,
    scope_id        TEXT NOT NULL,
    parent_actor_id TEXT,
    backend         TEXT NOT NULL,
    backend_args    TEXT NOT NULL DEFAULT '[]',
    cwd             TEXT,
    state           TEXT NOT NULL DEFAULT 'idle',
    checkpoint      TEXT,
    env             TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    closed_at       TEXT
);

CREATE TABLE turns (
    turn_id     TEXT PRIMARY KEY,
    actor_id    TEXT NOT NULL REFERENCES actors(actor_id),
    state       TEXT NOT NULL DEFAULT 'pending',
    exec_pid    INTEGER,
    result      TEXT,
    outcome     TEXT,
    error       TEXT,
    created_at  TEXT NOT NULL,
    started_at  TEXT,
    ended_at    TEXT
);

CREATE TABLE mailbox (
    message_id   TEXT PRIMARY KEY,
    actor_id     TEXT NOT NULL,
    message_type TEXT NOT NULL,
    payload      TEXT NOT NULL DEFAULT '{}',
    env          TEXT,
    state        TEXT NOT NULL DEFAULT 'queued',
    created_at   TEXT NOT NULL,
    acked_at     TEXT
);

CREATE TABLE events (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    actor_id    TEXT NOT NULL,
    turn_id     TEXT,
    event_type  TEXT NOT NULL,
    payload     TEXT NOT NULL DEFAULT '{}',
    created_at  TEXT NOT NULL
);

CREATE TABLE triggers (
    trigger_id       TEXT PRIMARY KEY,
    target_actor_id  TEXT NOT NULL,
    kind             TEXT NOT NULL DEFAULT 'cron',
    spec             TEXT NOT NULL DEFAULT '{}',
    message_type     TEXT NOT NULL,
    payload          TEXT NOT NULL DEFAULT '{}',
    next_fire_at     TEXT,
    created_at       TEXT NOT NULL
);

-- Actors: active name uniqueness within scope (null names excluded)
CREATE UNIQUE INDEX idx_active_name_scope
    ON actors(scope_id, name) WHERE state != 'closed' AND name IS NOT NULL;
CREATE INDEX idx_actors_parent ON actors(parent_actor_id);
CREATE INDEX idx_actors_state ON actors(state);

-- Turns: at most one active turn per actor
CREATE UNIQUE INDEX idx_turns_active_per_actor
    ON turns(actor_id) WHERE state IN ('pending', 'running');
CREATE INDEX idx_turns_actor ON turns(actor_id, created_at);

-- Mailbox: queued messages per actor (FIFO order)
CREATE INDEX idx_mailbox_queued
    ON mailbox(actor_id, state, created_at, message_id);

-- Events: per-actor and per-turn ordering
CREATE INDEX idx_events_actor ON events(actor_id, seq);
CREATE INDEX idx_events_turn ON events(turn_id, seq);

-- Triggers
CREATE INDEX idx_triggers_actor ON triggers(target_actor_id);
CREATE INDEX idx_triggers_fire ON triggers(next_fire_at);
