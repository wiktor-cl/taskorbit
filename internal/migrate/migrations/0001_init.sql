CREATE TABLE jobs (
    id           BIGSERIAL PRIMARY KEY,
    name         TEXT        NOT NULL,
    job_type     TEXT        NOT NULL CHECK (job_type IN ('log', 'http')),
    payload      JSONB       NOT NULL DEFAULT '{}',
    schedule_type TEXT       NOT NULL CHECK (schedule_type IN ('once', 'cron')),
    cron_expr    TEXT,
    run_at       TIMESTAMPTZ,
    next_run_at  TIMESTAMPTZ,
    enabled      BOOLEAN     NOT NULL DEFAULT TRUE,
    max_retries  INT         NOT NULL DEFAULT 5,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_once_has_run_at CHECK (schedule_type <> 'once' OR run_at IS NOT NULL),
    CONSTRAINT chk_cron_has_expr CHECK (schedule_type <> 'cron' OR cron_expr IS NOT NULL)
);

-- One row per *firing* of a job — a cron job accumulates many rows over
-- time, a one-off job gets exactly one. This is the queue: workers claim
-- rows from here, never from `jobs` directly.
CREATE TABLE task_runs (
    id               BIGSERIAL PRIMARY KEY,
    job_id           BIGINT      NOT NULL REFERENCES jobs (id),
    scheduled_for    TIMESTAMPTZ NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'PENDING'
                         CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'DEAD_LETTER')),
    claimed_by       TEXT,
    lease_expires_at TIMESTAMPTZ,
    attempt          INT         NOT NULL DEFAULT 0,
    max_retries      INT         NOT NULL,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Guards against the same firing being enqueued twice — by one
    -- scheduler polling overlapping ticks, or by multiple scheduler
    -- replicas racing each other.
    CONSTRAINT uq_task_runs_job_schedule UNIQUE (job_id, scheduled_for)
);

-- The index the claim query leans on: it filters on status and orders by
-- scheduled_for, for both the "new work" and "expired lease" branches.
CREATE INDEX idx_task_runs_claimable ON task_runs (status, scheduled_for)
    WHERE status IN ('PENDING', 'RUNNING');

CREATE TABLE workers (
    id               TEXT        PRIMARY KEY,
    hostname         TEXT        NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status           TEXT        NOT NULL DEFAULT 'ALIVE' CHECK (status IN ('ALIVE', 'DEAD'))
);
