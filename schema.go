package pgqueue

// Schema creates the pgqueue_jobs table and its indexes. Hosts that manage
// their own migrations should copy this SQL into a migration; if a release
// ever changes Schema, add a follow-up migration in the host application.
const Schema = `
CREATE TABLE pgqueue_jobs
(
    id           BIGSERIAL PRIMARY KEY,
    queue        TEXT        NOT NULL DEFAULT 'default',
    job_type     TEXT        NOT NULL,
    payload      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status       TEXT        NOT NULL DEFAULT 'pending'
        CONSTRAINT chk_pgqueue_jobs_status CHECK (status IN ('pending', 'running', 'completed', 'dead')),
    attempts     INT         NOT NULL DEFAULT 0,
    max_attempts INT         NOT NULL DEFAULT 5,
    run_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error   TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX idx_pgqueue_jobs_claim ON pgqueue_jobs (queue, run_at, id) WHERE status = 'pending';
CREATE INDEX idx_pgqueue_jobs_rescue ON pgqueue_jobs (updated_at) WHERE status = 'running';
`

// SchemaDown drops everything Schema creates.
const SchemaDown = `DROP TABLE IF EXISTS pgqueue_jobs;`
