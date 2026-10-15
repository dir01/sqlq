-- +goose Up
CREATE TABLE jobs (
    id BIGSERIAL PRIMARY KEY,
    job_type TEXT NOT NULL,
    payload BYTEA,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    trace_context JSONB, -- Propagated trace context
    consumed_at TIMESTAMP WITH TIME ZONE NULL, -- When a consumer claimed the job
    processed_at TIMESTAMP WITH TIME ZONE NULL, -- When the job was processed successfully
    claim_token TEXT NULL,
    claim_expires_at TIMESTAMP WITH TIME ZONE NULL
);
CREATE INDEX idx_jobs_available_claims ON jobs(job_type, scheduled_at, claim_expires_at) WHERE processed_at IS NULL;
CREATE INDEX idx_jobs_job_type ON jobs(job_type);
CREATE INDEX idx_jobs_scheduled_at_consumed_at ON jobs(scheduled_at, consumed_at); -- Finding available jobs
CREATE INDEX idx_jobs_job_type_processed_at ON jobs(job_type, processed_at); -- Cleaning up processed jobs

-- Dead-lettered jobs keep their original IDs.
CREATE TABLE dead_letter_queue (
    original_job_id BIGINT PRIMARY KEY,
    job_type TEXT NOT NULL,
    payload BYTEA,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    failed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    retry_count INTEGER NOT NULL DEFAULT 0,
    failure_reason TEXT NOT NULL DEFAULT ''
);
CREATE INDEX idx_dlq_job_type ON dead_letter_queue(job_type);

-- +goose Down
DROP TABLE dead_letter_queue;
DROP TABLE jobs;
