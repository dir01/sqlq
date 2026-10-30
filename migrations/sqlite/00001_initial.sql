-- +goose Up
-- Times are milliseconds since the Unix epoch.
CREATE TABLE sqlq_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT, -- Never reused: dead-lettered jobs keep their IDs.
    job_type TEXT NOT NULL,
    payload BLOB,
    created_at INTEGER NOT NULL,
    scheduled_at INTEGER NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    trace_context TEXT, -- Propagated trace context
    consumed_at INTEGER NULL, -- When a consumer claimed the job
    processed_at INTEGER NULL, -- When the job was processed successfully
    claim_token TEXT NULL,
    claim_expires_at INTEGER NULL
);
CREATE INDEX idx_sqlq_jobs_available_claims ON sqlq_jobs(job_type, scheduled_at, claim_expires_at) WHERE processed_at IS NULL;
CREATE INDEX idx_sqlq_jobs_job_type_processed_at ON sqlq_jobs(job_type, processed_at); -- Cleaning up processed jobs

-- Dead-lettered jobs keep their original IDs.
CREATE TABLE sqlq_dead_letter_queue (
    original_job_id INTEGER PRIMARY KEY,
    job_type TEXT NOT NULL,
    payload BLOB,
    created_at INTEGER NOT NULL,
    failed_at INTEGER NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    failure_reason TEXT NOT NULL DEFAULT ''
);
CREATE INDEX idx_sqlq_dlq_job_type ON sqlq_dead_letter_queue(job_type);

-- +goose Down
DROP TABLE sqlq_dead_letter_queue;
DROP TABLE sqlq_jobs;
