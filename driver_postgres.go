package sqlq

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"encoding/json"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

const pgTracerName = "github.com/dir01/sqlq/driver/postgres"

// PostgresDriver implements the Driver interface for PostgreSQL
type PostgresDriver struct {
	db     *sql.DB
	tracer trace.Tracer
}

// NewPostgresDriver creates a new PostgreSQL driver with the given database connection
func NewPostgresDriver(db *sql.DB) *PostgresDriver {
	return &PostgresDriver{db: db, tracer: otel.Tracer(pgTracerName)}
}

func (d *PostgresDriver) InitSchema(ctx context.Context) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.init_schema", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
	))
	defer span.End()

	queries := []string{
		`CREATE TABLE IF NOT EXISTS jobs (
			id SERIAL PRIMARY KEY,
			job_type TEXT NOT NULL,
			payload BYTEA,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			scheduled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			retry_count INTEGER DEFAULT 0,
			last_error TEXT,
			trace_context JSONB -- Added for trace propagation (using JSONB for efficiency)
		)`,

		`CREATE TABLE IF NOT EXISTS job_consumers (
			job_id INTEGER,
			consumer_name TEXT,
			processed_at TIMESTAMP NULL, -- Can be NULL to indicate processing
			PRIMARY KEY (job_id, consumer_name),
			FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE -- Cascade delete if job is deleted
		)`,

		`CREATE INDEX IF NOT EXISTS idx_jobs_job_type ON jobs(job_type)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_scheduled_at ON jobs(scheduled_at)`,
		// Add index for faster lookup in job_consumers
		`CREATE INDEX IF NOT EXISTS idx_job_consumers_job_id_consumer ON job_consumers(job_id, consumer_name)`,

		`CREATE TABLE IF NOT EXISTS dead_letter_queue (
			id SERIAL PRIMARY KEY,
			original_job_id INTEGER,
			job_type TEXT NOT NULL,
			payload BYTEA,
			created_at TIMESTAMP,
			failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			retry_count INTEGER,
			failure_reason TEXT
		)`,

		`CREATE INDEX IF NOT EXISTS idx_dlq_job_type ON dead_letter_queue(job_type)`,
	}

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	for _, query := range queries {
		_, err := tx.ExecContext(ctx, query)
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("failed to execute query (%s): %w", query, err)
		}
	}

	err = tx.Commit()
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (d *PostgresDriver) InsertJob(
	ctx context.Context,
	jobType string,
	payload []byte,
	delay time.Duration,
	traceContext map[string]string,
) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.insert_job", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.String("sqlq.job_type", jobType),
		attribute.String("sqlq.trace_context", fmt.Sprintf("%v", traceContext)),
		attribute.Float64("sqlq.delay_seconds", delay.Seconds()),
	))
	defer span.End()

	traceContextJSON, err := json.Marshal(traceContext)
	if err != nil {
		span.RecordError(fmt.Errorf("failed to marshal trace context: %w", err))
		traceContextJSON = []byte("{}")
	}

	var query string
	var args []any

	if delay <= 0 {
		query = "INSERT INTO jobs (job_type, payload, trace_context) VALUES ($1, $2, $3)"
		args = []any{jobType, payload, string(traceContextJSON)}
	} else {
		query = "INSERT INTO jobs (job_type, payload, scheduled_at, trace_context) VALUES ($1, $2, NOW() + $3, $4)"
		args = []any{jobType, payload, delay.String(), string(traceContextJSON)}
	}

	_, err = d.db.ExecContext(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (d *PostgresDriver) GetJobsForConsumer(ctx context.Context, consumerName, jobType string, prefetchCount int) ([]job, error) {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.get_jobs_for_consumer", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.String("sqlq.consumer_name", consumerName),
		attribute.String("sqlq.job_type", jobType),
		attribute.Int("sqlq.prefetch_count", prefetchCount),
	))
	defer span.End()

	// 1. Select potential jobs without locking
	rows, err := d.db.QueryContext(ctx, `
		SELECT j.id, j.payload, j.retry_count, j.trace_context
		FROM jobs j
		LEFT JOIN job_consumers jc 
		ON j.id = jc.job_id 
			AND jc.consumer_name = $1
		WHERE j.job_type = $2
			AND j.scheduled_at <= NOW()
			AND jc.job_id IS NULL
		ORDER BY j.id
		LIMIT $3
	`, consumerName, jobType, prefetchCount)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to select potential jobs: %w", err)
	}
	defer rows.Close()

	var potentialJobs []job
	for rows.Next() {
		var j job
		var traceContextJSON sql.NullString
		j.JobType = jobType

		if err := rows.Scan(&j.ID, &j.Payload, &j.RetryCount, &traceContextJSON); err != nil {
			span.RecordError(fmt.Errorf("failed to scan potential job details: %w", err))
			// TODO: Consider returning partial results or skipping the job
			return nil, fmt.Errorf("failed to scan potential job details: %w", err)
		}

		j.TraceContext = make(map[string]string)
		if traceContextJSON.Valid && traceContextJSON.String != "" && traceContextJSON.String != "null" {
			if err := json.Unmarshal([]byte(traceContextJSON.String), &j.TraceContext); err != nil {
				span.RecordError(fmt.Errorf("failed to unmarshal trace context for job %d: %w", j.ID, err))
				// Continue without trace context if unmarshal fails
				j.TraceContext = make(map[string]string)
			}
		}
		potentialJobs = append(potentialJobs, j)
	}
	if err := rows.Err(); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("error iterating potential job rows: %w", err)
	}
	rows.Close() // Close rows explicitly before starting transaction

	// 2. Try to "lock" the potential jobs by inserting into job_consumers
	var jobsToReturn []job
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(fmt.Errorf("failed to begin transaction for locking jobs: %w", err))
		return nil, err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO job_consumers (job_id, consumer_name, processed_at) 
		VALUES ($1, $2, NULL) 
		ON CONFLICT (job_id, consumer_name) DO NOTHING`)
	if err != nil {
		span.RecordError(fmt.Errorf("failed to prepare insert statement for locking: %w", err))
		return nil, err
	}
	defer stmt.Close()

	for _, j := range potentialJobs {
		res, err := stmt.ExecContext(ctx, j.ID, consumerName)
		if err != nil {
			// This shouldn't happen with ON CONFLICT DO NOTHING unless there's another issue
			span.RecordError(fmt.Errorf("failed executing insert for job lock (job_id: %d): %w", j.ID, err))
			// Decide whether to continue or return error based on severity
			continue // Skip this job
		}

		rowsAffected, _ := res.RowsAffected()
		if rowsAffected > 0 {
			// Successfully inserted placeholder, job is now "locked" for us
			jobsToReturn = append(jobsToReturn, j)
			if len(jobsToReturn) >= prefetchCount {
				break // Reached prefetch limit
			}
		} else {
			// Insert did nothing, likely due to ON CONFLICT. Job was already locked/processed.
			span.AddEvent("Failed to acquire lock for job (likely conflict)", trace.WithAttributes(
				attribute.Int64("sqlq.job_id", j.ID),
			))
			// Job likely locked or processed by another consumer, skip it.
		}
	}

	if err := tx.Commit(); err != nil {
		span.RecordError(fmt.Errorf("failed to commit transaction for locking jobs: %w", err))
		return nil, err
	}

	span.SetAttributes(attribute.Int("sqlq.jobs_fetched", len(jobsToReturn)))
	return jobsToReturn, nil
}

func (d *PostgresDriver) MarkJobProcessed(ctx context.Context, jobID int64, consumerName string) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.mark_processed", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.Int64("sqlq.job_id", jobID),
		attribute.String("sqlq.consumer_name", consumerName),
	))
	defer span.End()

	res, err := d.db.ExecContext(ctx,
		`UPDATE job_consumers 
		 SET processed_at = NOW() 
		 WHERE job_id = $1 AND consumer_name = $2 AND processed_at IS NULL`, // Only update if processing
		jobID, consumerName,
	)
	if err != nil {
		span.RecordError(err)
		return err
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		// This could happen if:
		// 1. The job was already marked processed (processed_at IS NOT NULL).
		// 2. The job failed and was rescheduled (placeholder row deleted).
		// 3. The job ID or consumer name is incorrect, or the placeholder was never inserted.
		// Log this as it might indicate an unexpected state or double processing attempt.
		span.AddEvent("MarkJobProcessed found no matching 'processing' row (processed_at IS NULL) to update", trace.WithAttributes(
			attribute.Int64("sqlq.job_id", jobID),
			attribute.String("sqlq.consumer_name", consumerName),
		))
		// Depending on desired strictness, we might return an error here.
		// return fmt.Errorf("no processing job found for job_id %d and consumer %s to mark as processed", jobID, consumerName)
	}

	return nil // Return nil even if rowsAffected is 0, as the goal is idempotency
}

func (d *PostgresDriver) MarkJobFailedAndReschedule(ctx context.Context, jobID int64, errorMsg string, backoffDuration time.Duration) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.mark_failed_and_reschedule", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.Int64("sqlq.job_id", jobID),
		attribute.String("sqlq.error_message", errorMsg),
		attribute.Float64("sqlq.backoff_seconds", backoffDuration.Seconds()),
	))
	defer span.End()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(fmt.Errorf("failed to begin transaction for reschedule: %w", err))
		return err
	}
	defer tx.Rollback()

	// Update the job itself
	_, err = tx.ExecContext(ctx,
		"UPDATE jobs SET retry_count = retry_count + 1, last_error = $1, scheduled_at = NOW() + $2 WHERE id = $3",
		errorMsg, backoffDuration.String(), jobID,
	)
	if err != nil {
		span.RecordError(fmt.Errorf("failed to update jobs table on reschedule: %w", err))
		return err
	}

	// Remove the "processing" placeholder (where processed_at IS NULL) from job_consumers
	// It's okay if this affects 0 rows (e.g., if GetJobsForConsumer failed before inserting)
	_, err = tx.ExecContext(ctx,
		"DELETE FROM job_consumers WHERE job_id = $1 AND processed_at IS NULL",
		jobID,
	)
	if err != nil {
		// Log the error but don't necessarily fail the whole operation,
		// as the job update is the critical part.
		span.RecordError(fmt.Errorf("failed to delete placeholder from job_consumers on reschedule (job_id: %d): %w", jobID, err))
	}

	err = tx.Commit()
	if err != nil {
		span.RecordError(fmt.Errorf("failed to commit transaction for reschedule: %w", err))
	}
	return err
}

func (d *PostgresDriver) MoveToDeadLetterQueue(ctx context.Context, jobID int64, reason string) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.move_to_dlq", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.Int64("sqlq.job_id", jobID),
		attribute.String("sqlq.dlq_reason", reason),
	))
	defer span.End()

	tx, err := d.db.BeginTx(ctx, nil) // Use BeginTx with context
	if err != nil {
		span.RecordError(err)
		return err
	}

	defer tx.Rollback()

	// Get the job details
	var job job
	err = tx.QueryRowContext(ctx, `
		SELECT id, job_type, payload, created_at, retry_count
		FROM jobs WHERE id = $1
	`, jobID).Scan(&job.ID, &job.JobType, &job.Payload, &job.CreatedAt, &job.RetryCount) // Use QueryRowContext
	if err != nil {
		span.RecordError(err)
		if err == sql.ErrNoRows {
			return ErrJobNotFound
		}
		return err
	}

	// Insert into dead letter queue
	_, err = tx.Exec(`
		INSERT INTO dead_letter_queue 
		(original_job_id, job_type, payload, created_at, retry_count, failure_reason)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, job.ID, job.JobType, job.Payload, job.CreatedAt, job.RetryCount, reason) // Use ExecContext
	if err != nil {
		span.RecordError(err)
		return err
	}

	// Delete from jobs table
	_, err = tx.ExecContext(ctx, "DELETE FROM jobs WHERE id = $1", jobID) // Use ExecContext
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to delete job from main table: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (d *PostgresDriver) GetDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]DeadLetterJob, error) {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.get_dlq_jobs", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.String("sqlq.job_type", jobType), // jobType might be empty
		attribute.Int("sqlq.limit", limit),
	))
	defer span.End()

	query := `
		SELECT id, original_job_id, job_type, payload, created_at, failed_at, retry_count, failure_reason
		FROM dead_letter_queue
		WHERE job_type = $1
		ORDER BY failed_at DESC
		LIMIT $2
	`
	if jobType == "" {
		query = `
			SELECT id, original_job_id, job_type, payload, created_at, failed_at, retry_count, failure_reason
			FROM dead_letter_queue
			ORDER BY failed_at DESC
			LIMIT $1
		`
		return d.queryDeadLetterJobs(ctx, query, limit)
	}

	jobs, err := d.queryDeadLetterJobs(ctx, query, jobType, limit) // Pass context
	if err != nil {
		span.RecordError(err)
	} else {
		span.SetAttributes(attribute.Int("sqlq.dlq_jobs_fetched", len(jobs)))
	}
	return jobs, err
}

// queryDeadLetterJobs is a helper, context is passed from the caller
func (d *PostgresDriver) queryDeadLetterJobs(ctx context.Context, query string, args ...interface{}) ([]DeadLetterJob, error) {
	rows, err := d.db.QueryContext(ctx, query, args...) // Use QueryContext
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []DeadLetterJob
	for rows.Next() {
		var j DeadLetterJob
		if err := rows.Scan(
			&j.ID,
			&j.OriginalID,
			&j.JobType,
			&j.Payload,
			&j.CreatedAt,
			&j.FailedAt,
			&j.RetryCount,
			&j.FailureReason,
		); err != nil {
			// Don't record error in span here, let the caller handle it
			return nil, err
		}
		jobs = append(jobs, j)
	}

	if err := rows.Err(); err != nil {
		// Don't record error in span here, let the caller handle it
		return nil, err
	}

	return jobs, nil
}

func (d *PostgresDriver) RequeueDeadLetterJob(ctx context.Context, dlqID int64) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.requeue_dlq_job", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.Int64("sqlq.dlq_id", dlqID),
	))
	defer span.End()

	tx, err := d.db.BeginTx(ctx, nil) // Use BeginTx with context
	if err != nil {
		span.RecordError(err)
		return err
	}
	defer tx.Rollback()

	// Get the job from the dead letter queue
	var dlqJob DeadLetterJob
	err = tx.QueryRowContext(ctx, `
		SELECT id, job_type, payload
		FROM dead_letter_queue WHERE id = $1
	`, dlqID).Scan(&dlqJob.ID, &dlqJob.JobType, &dlqJob.Payload) // Use QueryRowContext
	if err != nil {
		span.RecordError(err)
		if err == sql.ErrNoRows {
			return ErrJobNotFound
		}
		return err
	}

	// Insert back into the main queue with reset retry count and scheduled_at = NOW()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO jobs (job_type, payload, retry_count, scheduled_at)
		VALUES ($1, $2, 0, NOW())
	`, dlqJob.JobType, dlqJob.Payload) // Use ExecContext
	if err != nil {
		span.RecordError(err)
		return err
	}

	// Delete from the dead letter queue
	_, err = tx.ExecContext(ctx, "DELETE FROM dead_letter_queue WHERE id = $1", dlqID) // Use ExecContext
	if err != nil {
		span.RecordError(err)
		return err
	}

	err = tx.Commit()
	if err != nil {
		span.RecordError(err)
	}
	return err
}
