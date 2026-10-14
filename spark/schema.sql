-- Idempotent schema. Run by spark/job.py on every start, so the same file
-- bootstraps local Postgres and a fresh RDS instance.
CREATE TABLE IF NOT EXISTS events (
    event_id    TEXT PRIMARY KEY,
    user_id     BIGINT,
    event_type  TEXT,
    amount      DOUBLE PRECISION,
    ts          TIMESTAMPTZ,
    source_file TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS events_rejects (
    reject_key  TEXT PRIMARY KEY,
    raw         TEXT,
    source_file TEXT,
    reason      TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Landing tables Spark overwrites each micro-batch, then merged with
-- INSERT .. ON CONFLICT DO NOTHING so replays never duplicate rows.
CREATE UNLOGGED TABLE IF NOT EXISTS events_stage (
    event_id TEXT, user_id BIGINT, event_type TEXT,
    amount DOUBLE PRECISION, ts TIMESTAMPTZ, source_file TEXT
);
CREATE UNLOGGED TABLE IF NOT EXISTS events_rejects_stage (
    reject_key TEXT, raw TEXT, source_file TEXT, reason TEXT
);
