package sqlq

import (
	"context"
	"database/sql"
	"errors"
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

// newPostgresDriver creates a new PostgreSQL driver with the given database connection
func newPostgresDriver(db *sql.DB) *PostgresDriver {
	return &PostgresDriver{db: db, tracer: otel.Tracer(pgTracerName)}
}

// initSchema creates the necessary tables and indexes for PostgreSQL if they don't exist.
// It's idempotent and safe to call multiple times.
func (d *PostgresDriver) initSchema(ctx context.Context) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.init_schema", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
	))
	defer span.End()

	queries := []string{
		`CREATE TABLE IF NOT EXISTS jobs (
			id SERIAL PRIMARY KEY,
			job_type TEXT NOT NULL,
			payload BYTEA,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			scheduled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			retry_count INTEGER DEFAULT 0,
			last_error TEXT,
			trace_context JSONB, -- Added for trace propagation (using JSONB for efficiency)
			consumed_at TIMESTAMP WITH TIME ZONE NULL, -- Indicates when the job was claimed by a consumer
			processed_at TIMESTAMP WITH TIME ZONE NULL, -- Indicates when the job was successfully processed
			consumer_name TEXT NULL -- Name of the consumer instance that claimed the job
		)`,
		// Removed job_consumers table definition

		`CREATE INDEX IF NOT EXISTS idx_jobs_job_type ON jobs(job_type)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_scheduled_at_consumed_at ON jobs(scheduled_at, consumed_at)`, // Index for finding available jobs
		`CREATE INDEX IF NOT EXISTS idx_jobs_job_type_processed_at ON jobs(job_type, processed_at)`,       // Index for cleaning up processed jobs

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
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			span.AddEvent("Failed to rollback transaction", trace.WithAttributes(attribute.String("error", rErr.Error())))
			// Log or handle the rollback error appropriately
			// If the main function error 'err' is nil, you might want to assign rErr to it here.
		}
	}()

	for _, query := range queries {
		_, err = tx.ExecContext(ctx, query)
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

// cleanupJobs deletes old processed jobs for a specific job type in PostgreSQL.
// It deletes jobs where processed_at is older than maxAge, in batches.
func (d *PostgresDriver) cleanupJobs(ctx context.Context, jobType string, maxAge time.Duration, batchSize uint16) (int64, error) {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.cleanup_jobs", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.String("sqlq.job_type", jobType),
		attribute.String("sqlq.cleanup.age", maxAge.String()),
		attribute.Int("sqlq.cleanup.batch_size", int(batchSize)),
	))
	defer span.End()

	var totalDeleted int64
	threshold := time.Now().Add(-maxAge)

	for {
		var batchDeleted int64
		err := d.runInTx(ctx, func(tx *sql.Tx) error {
			// Efficiently delete a batch using ctid in PostgreSQL
			res, err := tx.ExecContext(ctx, `
				DELETE FROM jobs
				WHERE ctid IN (
					SELECT ctid
					FROM jobs
					WHERE job_type = $1 AND processed_at < $2
					LIMIT $3
				)
			`, jobType, threshold, batchSize)
			if err != nil {
				return fmt.Errorf("failed to delete batch of old processed jobs for type %s: %w", jobType, err)
			}
			count, _ := res.RowsAffected()
			batchDeleted = count
			return nil
		})

		if err != nil {
			span.RecordError(fmt.Errorf("error during processed job cleanup batch: %w", err))
			return totalDeleted, err // Return partial counts and error
		}

		totalDeleted += batchDeleted
		if batchDeleted < int64(batchSize) {
			break // Last batch processed
		}
	}
	span.SetAttributes(attribute.Int64("sqlq.db.rows_deleted.processed", totalDeleted))
	return totalDeleted, nil
}

// cleanupDeadLetterQueueJobs deletes old jobs from the dead-letter queue for a specific job type.
// It deletes DLQ jobs older than maxAge, in batches.
func (d *PostgresDriver) cleanupDeadLetterQueueJobs(ctx context.Context, jobType string, maxAge time.Duration, batchSize uint16) (int64, error) {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.cleanup_jobs", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.String("sqlq.job_type", jobType),
		attribute.String("sqlq.cleanup.age.processed", maxAge.String()),
		attribute.Int("sqlq.cleanup.batch_size", int(batchSize)),
	))
	defer span.End()

	// --- Cleanup DLQ Jobs (Batched) ---
	dlqThreshold := time.Now().Add(-maxAge)
	var totalDlqDeleted int64
	for {
		var batchDeleted int64
		err := d.runInTx(ctx, func(tx *sql.Tx) error {
			// Efficiently delete a batch using ctid in PostgreSQL
			resDLQ, err := tx.ExecContext(ctx, `
				DELETE FROM dead_letter_queue
				WHERE ctid IN (
					SELECT ctid
					FROM dead_letter_queue
					WHERE job_type = $1 AND failed_at < $2
					LIMIT $3
				)
			`, jobType, dlqThreshold, batchSize)
			if err != nil {
				return fmt.Errorf("failed to delete batch of old dlq jobs for type %s: %w", jobType, err)
			}
			count, _ := resDLQ.RowsAffected()
			batchDeleted = count
			return nil
		})

		if err != nil {
			span.RecordError(fmt.Errorf("error during DLQ cleanup batch: %w", err))
			return totalDlqDeleted, err // Return partial counts and error
		}

		totalDlqDeleted += batchDeleted
		if batchDeleted < int64(batchSize) {
			break // Last batch processed
		}
		// Optional: Add a small delay between batches if needed
		// time.Sleep(10 * time.Millisecond)
	}

	span.SetAttributes(
		attribute.Int64("sqlq.db.rows_deleted.dlq", totalDlqDeleted),
	)

	return totalDlqDeleted, nil
}

// runInTx is a helper to execute a function within a transaction
func (d *PostgresDriver) runInTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	// Defer rollback, checking for errors unless it's sql.ErrTxDone (already committed/rolled back)
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			// Log or handle the rollback error appropriately.
			// Since this helper doesn't have access to a span, we can't easily add events.
			// We could potentially pass a logger or span down, but for now, just log simply.
			// This satisfies staticcheck. A more robust solution might involve refactoring.
			fmt.Printf("WARN: Failed to rollback transaction in runInTx helper: %v\n", rErr)
		}
	}()

	if err := fn(tx); err != nil {
		return err // Error occurred, rollback will happen
	}

	return tx.Commit() // Commit if fn succeeded
}

// InsertJob inserts a new job into the PostgreSQL jobs table.
func (d *PostgresDriver) insertJob(
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

// getJobsForConsumer selects and locks available jobs for a given consumer and job type from PostgreSQL.
// It uses an advisory lock-like mechanism by updating `consumed_at`.
func (d *PostgresDriver) getJobsForConsumer(ctx context.Context, consumerName, jobType string, prefetchCount uint16) ([]job, error) {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.get_jobs_for_consumer", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.String("sqlq.consumer_name", consumerName),
		attribute.String("sqlq.job_type", jobType),
		attribute.Int("sqlq.prefetch_count", int(prefetchCount)),
	))
	defer span.End()

	var jobsToReturn []job
	err := d.runInTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			UPDATE jobs
			SET consumed_at = NOW(), consumer_name = $1
			WHERE id IN (
				SELECT id
				FROM jobs
				WHERE job_type = $2
				  AND scheduled_at <= NOW()
				  AND consumed_at IS NULL
				ORDER BY scheduled_at, id
				FOR UPDATE SKIP LOCKED
				LIMIT $3
			)
			RETURNING id, payload, retry_count, trace_context
		`, consumerName, jobType, prefetchCount)

		if err != nil {
			return fmt.Errorf("failed to update and select jobs: %w", err)
		}
		defer func() {
			_ = rows.Close() // Ensure rows are closed within the transaction function
		}()

		for rows.Next() {
			var j job
			var traceContextJSON sql.NullString
			j.JobType = jobType // Set job type as it's not returned by RETURNING

			if err = rows.Scan(&j.ID, &j.Payload, &j.RetryCount, &traceContextJSON); err != nil {
				// Log or record error, but potentially continue scanning other rows
				span.RecordError(fmt.Errorf("failed to scan returned job details (job_id: %d): %w", j.ID, err))
				continue
			}

			j.TraceContext = make(map[string]string)
			if traceContextJSON.Valid && traceContextJSON.String != "" && traceContextJSON.String != "null" {
				if err = json.Unmarshal([]byte(traceContextJSON.String), &j.TraceContext); err != nil {
					span.RecordError(fmt.Errorf("failed to unmarshal trace context for job %d: %w", j.ID, err))
					j.TraceContext = make(map[string]string) // Reset on error
				}
			}
			jobsToReturn = append(jobsToReturn, j)
		}
		return rows.Err() // Return any error encountered during iteration
	})

	if err != nil {
		span.RecordError(fmt.Errorf("transaction failed during getJobsForConsumer: %w", err))
		return nil, err // Return nil slice and the error
	}

	span.SetAttributes(attribute.Int("sqlq.jobs_fetched", len(jobsToReturn)))
	return jobsToReturn, nil
}

// markJobProcessed updates the jobs table to mark a job as successfully processed in PostgreSQL.
func (d *PostgresDriver) markJobProcessed(ctx context.Context, jobID int64) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.mark_processed", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.Int64("sqlq.job_id", jobID),
		// Removed consumerName attribute
	))
	defer span.End()

	res, err := d.db.ExecContext(ctx,
		`UPDATE jobs SET processed_at = NOW() WHERE id = $1 AND consumed_at IS NOT NULL AND processed_at IS NULL`,
		jobID,
	)
	if err != nil {
		span.RecordError(err)
		return err
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		// This could happen if:
		// 1. The job was already marked processed (processed_at IS NOT NULL).
		// 2. The job was not consumed (consumed_at IS NULL).
		// 3. The job failed and was rescheduled (consumed_at became NULL).
		// 4. The job ID is incorrect.
		// Log this as it might indicate an unexpected state or double processing attempt.
		span.AddEvent("MarkJobProcessed found no matching 'consumed' row (consumed_at IS NOT NULL, processed_at IS NULL) to update", trace.WithAttributes(
			attribute.Int64("sqlq.job_id", jobID),
		))
	}

	return nil // Return nil even if rowsAffected is 0, goal is idempotency
}

// MarkJobFailedAndReschedule updates a job's state to failed, increments the retry count,
// and schedules it for a future retry attempt in PostgreSQL.
func (d *PostgresDriver) markJobFailedAndReschedule(ctx context.Context, jobID int64, errorMsg string, backoffDuration time.Duration) error {
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
		span.RecordError(fmt.Errorf("failed to begin transaction for reschedule: %w", err))
		return err
	}
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			span.AddEvent("Failed to rollback transaction during reschedule", trace.WithAttributes(attribute.String("error", rErr.Error())))
		}
	}()

	// Update the job: increment retry, set error, schedule, clear consumption/processing state
	_, err = tx.ExecContext(ctx, `
		UPDATE jobs SET 
			retry_count = retry_count + 1, 
			last_error = $1, 
			scheduled_at = NOW() + $2::interval, 
			consumed_at = NULL, 
			processed_at = NULL,
			consumer_name = NULL
		WHERE id = $3`,
		errorMsg, backoffDuration.String(), jobID,
	)
	if err != nil {
		span.RecordError(fmt.Errorf("failed to update jobs table on reschedule: %w", err))
		return err // Rollback will happen
	}

	// No need to delete from job_consumers anymore

	err = tx.Commit()
	if err != nil {
		span.RecordError(fmt.Errorf("failed to commit transaction for reschedule: %w", err))
	}
	return err
}

// moveToDeadLetterQueue moves a failed job from the main jobs table to the dead_letter_queue table in PostgreSQL.
func (d *PostgresDriver) moveToDeadLetterQueue(ctx context.Context, jobID int64, reason string) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.move_to_dlq", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.Int64("sqlq.job_id", jobID),
		attribute.String("sqlq.dlq_reason", reason),
	))
	defer span.End()

	tx, err := d.db.BeginTx(ctx, nil) // Use BeginTx with context
	if err != nil {
		span.RecordError(err)
		span.RecordError(err)
		return err
	}
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			span.AddEvent("Failed to rollback transaction during DLQ move", trace.WithAttributes(attribute.String("error", rErr.Error())))
		}
	}()

	// Get the job details
	var j job
	err = tx.QueryRowContext(ctx, `
		SELECT id, job_type, payload, created_at, retry_count
		FROM jobs WHERE id = $1
	`, jobID).Scan(&j.ID, &j.JobType, &j.Payload, &j.CreatedAt, &j.RetryCount) // Use QueryRowContext
	if err != nil {
		span.RecordError(err)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrJobNotFound
		}
		return err
	}

	// Insert into dead letter queue
	_, err = tx.Exec(`
		INSERT INTO dead_letter_queue 
		(original_job_id, job_type, payload, created_at, retry_count, failure_reason)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, j.ID, j.JobType, j.Payload, j.CreatedAt, j.RetryCount, reason) // Use ExecContext
	if err != nil {
		span.RecordError(err)
		return err
	}

	// Delete from jobs table (job_consumers is deleted via CASCADE)
	_, err = tx.ExecContext(ctx, "DELETE FROM jobs WHERE id = $1", jobID) // Use ExecContext
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to delete job from main table: %w", err)
	}

	// No need to delete from job_consumers explicitly due to CASCADE

	err = tx.Commit()
	if err != nil {
		span.RecordError(err)
	}
	return err
}

// getDeadLetterJobs retrieves jobs from the dead_letter_queue table in PostgreSQL, optionally filtered by job type.
func (d *PostgresDriver) getDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]DeadLetterJob, error) {
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
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			// Cannot add to span here as it's not available, log instead or ignore.
			fmt.Printf("WARN: Failed to close rows in queryDeadLetterJobs helper: %v\n", closeErr)
		}
	}()

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

// requeueDeadLetterJob moves a job from the dead_letter_queue back into the main jobs table for reprocessing in PostgreSQL.
func (d *PostgresDriver) requeueDeadLetterJob(ctx context.Context, dlqID int64) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.requeue_dlq_job", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.Int64("sqlq.dlq_id", dlqID),
	))
	defer span.End()

	tx, err := d.db.BeginTx(ctx, nil) // Use BeginTx with context
	if err != nil {
		span.RecordError(err)
		span.RecordError(err)
		return err
	}
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			span.AddEvent("Failed to rollback transaction during DLQ requeue", trace.WithAttributes(attribute.String("error", rErr.Error())))
		}
	}()

	// Get the job from the dead letter queue
	var dlqJob DeadLetterJob
	err = tx.QueryRowContext(ctx, `
		SELECT id, job_type, payload
		FROM dead_letter_queue WHERE id = $1
	`, dlqID).Scan(&dlqJob.ID, &dlqJob.JobType, &dlqJob.Payload) // Use QueryRowContext
	if err != nil {
		span.RecordError(err)
		if errors.Is(err, sql.ErrNoRows) {
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
