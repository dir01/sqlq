package sqlq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"encoding/json"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const sqliteTracerName = "github.com/dir01/sqlq/driver_sqlite"

// SQLiteDriver implements the Driver interface for SQLite
type SQLiteDriver struct {
	db     *sql.DB
	tracer trace.Tracer
	mutex  sync.Mutex
}

// newSQLiteDriver creates a new SQLite driver with the given database connection.
func newSQLiteDriver(db *sql.DB) *SQLiteDriver {
	return &SQLiteDriver{db: db, mutex: sync.Mutex{}, tracer: otel.Tracer(sqliteTracerName)}
}

// initSchema creates the necessary tables and indexes for SQLite if they don't exist.
// It's idempotent and safe to call multiple times.
func (d *SQLiteDriver) initSchema(ctx context.Context) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.init_schema", trace.WithAttributes(
		semconv.DBSystemSqlite,
	))
	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	queries := []string{
		`CREATE TABLE IF NOT EXISTS jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_type TEXT NOT NULL,
			payload BLOB,
			created_at INTEGER NOT NULL, -- Milliseconds since epoch
			scheduled_at INTEGER NOT NULL, -- Milliseconds since epoch
			retry_count INTEGER DEFAULT 0,
			last_error TEXT,
			trace_context TEXT, -- Added for trace propagation
			consumed_at INTEGER NULL, -- Milliseconds since epoch, indicates when the job was claimed
			processed_at INTEGER NULL, -- Milliseconds since epoch, indicates successful processing
			consumer_name TEXT NULL -- Name of the consumer instance that claimed the job
		)`,
		// Removed job_consumers table definition

		`CREATE INDEX IF NOT EXISTS idx_jobs_job_type ON jobs(job_type)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_scheduled_at_consumed_at ON jobs(scheduled_at, consumed_at)`, // Index for finding available jobs
		`CREATE INDEX IF NOT EXISTS idx_jobs_job_type_processed_at ON jobs(job_type, processed_at)`,       // Index for cleaning up processed jobs

		`CREATE TABLE IF NOT EXISTS dead_letter_queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			original_job_id INTEGER,
			job_type TEXT NOT NULL,
			payload BLOB,
			created_at INTEGER NOT NULL, -- Milliseconds since epoch
			failed_at INTEGER NOT NULL, -- Milliseconds since epoch
			retry_count INTEGER,
			failure_reason TEXT
		)`,

		`CREATE INDEX IF NOT EXISTS idx_dlq_job_type ON dead_letter_queue(job_type)`,
	}

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(err)
		span.RecordError(err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	// Defer rollback and check error
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			span.AddEvent("Failed to rollback transaction during InitSchema", trace.WithAttributes(attribute.String("error", rErr.Error())))
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

// cleanupJobs deletes old processed jobs for a specific job type in SQLite.
// It deletes jobs where processed_at is older than maxAge, in batches.
func (d *SQLiteDriver) cleanupJobs(ctx context.Context, jobType string, maxAge time.Duration, batchSize uint16) (int64, error) { //nolint
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.cleanup_jobs", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.String("sqlq.job_type", jobType),
		attribute.String("sqlq.cleanup.age", maxAge.String()),
		attribute.Int("sqlq.cleanup.batch_size", int(batchSize)),
	))
	defer span.End()

	var totalDeleted int64
	thresholdMs := time.Now().Add(-maxAge).UnixMilli()

	for {
		var batchDeleted int64
		err := d.runInTx(ctx, func(tx *sql.Tx) error {
			// Efficiently delete a batch using rowid in SQLite
			res, err := tx.ExecContext(ctx, `
				DELETE FROM jobs
				WHERE rowid IN (
					SELECT rowid
					FROM jobs
					WHERE job_type = ? AND processed_at < ?
					LIMIT ?
				)
			`, jobType, thresholdMs, batchSize)
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
func (d *SQLiteDriver) cleanupDeadLetterQueueJobs(ctx context.Context, jobType string, maxAge time.Duration, batchSize uint16) (int64, error) { //nolint
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.cleanup_jobs", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.String("sqlq.job_type", jobType),
		attribute.String("sqlq.cleanup.age", maxAge.String()),
		attribute.Int("sqlq.cleanup.batch_size", int(batchSize)),
	))
	defer span.End()

	var totalDlqDeleted int64
	dlqThresholdMs := time.Now().Add(-maxAge).UnixMilli()

	for {
		var batchDeleted int64
		err := d.runInTx(ctx, func(tx *sql.Tx) error {
			// Efficiently delete a batch using rowid in SQLite
			resDLQ, err := tx.ExecContext(ctx, `
				DELETE FROM dead_letter_queue
				WHERE rowid IN (
					SELECT rowid
					FROM dead_letter_queue
					WHERE job_type = ? AND failed_at < ?
					LIMIT ?
				)
			`, jobType, dlqThresholdMs, batchSize)
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
	}

	span.SetAttributes(
		attribute.Int64("sqlq.db.rows_deleted.dlq", totalDlqDeleted),
	)

	return totalDlqDeleted, nil
}

// runInTx is a helper to execute a function within a transaction, acquiring the driver mutex
func (d *SQLiteDriver) runInTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	// Defer rollback and check error
	defer func() {
		// Note: No span available here. Log simply or ignore.
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			fmt.Printf("WARN: Failed to rollback transaction in runInTx helper: %v\n", rErr)
		}
	}()

	if err := fn(tx); err != nil {
		return err // Error occurred, rollback will happen
	}

	return tx.Commit() // Commit if fn succeeded
}

// InsertJob inserts a new job into the SQLite jobs table.
func (d *SQLiteDriver) insertJob(
	ctx context.Context,
	jobType string,
	payload []byte,
	delay time.Duration,
	traceContext map[string]string,
) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.insert_job", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.String("sqlq.job_type", jobType),
		attribute.String("sqlq.trace_context", fmt.Sprintf("%v", traceContext)),
		attribute.Float64("sqlq.delay_seconds", delay.Seconds()),
	))
	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	traceContextJSON, err := json.Marshal(traceContext)
	if err != nil {
		span.RecordError(fmt.Errorf("failed to marshal trace context: %w", err))
		traceContextJSON = []byte("{}")
	}

	// Use SQLite's built-in functions to get current time in milliseconds
	// and calculate scheduled time based on delay
	var query string
	var args []any

	nowMs := time.Now().UnixMilli()

	if delay <= 0 {
		query = `
			INSERT INTO jobs (job_type, payload, created_at, scheduled_at, trace_context) 
			VALUES (?, ?, ?, ?, ?)
		`
		args = []any{jobType, payload, nowMs, nowMs, string(traceContextJSON)}
	} else {
		scheduledMs := nowMs + delay.Milliseconds()
		query = `
			INSERT INTO jobs (job_type, payload, created_at, scheduled_at, trace_context) 
			VALUES (?, ?, ?, ?, ?)
		`
		args = []any{jobType, payload, nowMs, scheduledMs, string(traceContextJSON)}
	}

	_, err = d.db.ExecContext(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
	}

	return err
}

// getJobsForConsumer selects and locks available jobs for a given consumer and job type from SQLite.
// It uses an advisory lock-like mechanism by updating `consumed_at`.
func (d *SQLiteDriver) getJobsForConsumer(ctx context.Context, consumerName, jobType string, prefetchCount uint16) ([]job, error) {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.get_jobs_for_consumer", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.String("sqlq.consumer_name", consumerName),
		attribute.String("sqlq.job_type", jobType),
		attribute.Int("sqlq.prefetch_count", int(prefetchCount)),
	))
	defer span.End()

	var jobsToReturn []job
	nowMs := time.Now().UnixMilli()

	// Since SQLite doesn't support SKIP LOCKED or UPDATE...RETURNING well,
	// we use the mutex and a two-step process: SELECT potential IDs, then UPDATE.
	err := d.runInTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			SELECT id, payload, retry_count, trace_context
			FROM jobs
			WHERE job_type = ?
			AND scheduled_at <= ?
			AND consumed_at IS NULL
			ORDER BY id
			LIMIT ?
		`, jobType, nowMs, prefetchCount)
		if err != nil {
			return fmt.Errorf("failed to select potential jobs: %w", err)
		}

		defer func() {
			_ = rows.Close()
		}()

		var potentialJobs []job
		for rows.Next() {
			var j job
			var traceContextJSON sql.NullString
			j.JobType = jobType
			if err = rows.Scan(&j.ID, &j.Payload, &j.RetryCount, &traceContextJSON); err != nil {
				return fmt.Errorf("failed to scan potential job: %w", err) // Return error to rollback
			}

			// Deserialize trace context
			j.TraceContext = make(map[string]string)
			if traceContextJSON.Valid && traceContextJSON.String != "" {
				if err = json.Unmarshal([]byte(traceContextJSON.String), &j.TraceContext); err != nil {
					span.RecordError(fmt.Errorf("failed to unmarshal trace context for job %d: %w", j.ID, err))
					j.TraceContext = make(map[string]string) // Reset on error
				}
			}
			potentialJobs = append(potentialJobs, j)
		}
		if err = rows.Err(); err != nil {
			return fmt.Errorf("error iterating potential jobs: %w", err)
		}

		_ = rows.Close() // Close explicitly before update

		if len(potentialJobs) == 0 {
			return nil // No jobs found
		}

		// Prepare update statement
		stmt, err := tx.PrepareContext(ctx, `
			UPDATE jobs SET consumed_at = ?, consumer_name = ?
			WHERE id = ? AND consumed_at IS NULL
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare update statement: %w", err)
		}
		defer func() {
			_ = stmt.Close()
		}()

		// Attempt to lock/claim each potential job
		for _, j := range potentialJobs {
			res, err := stmt.ExecContext(ctx, nowMs, consumerName, j.ID)
			if err != nil {
				// Log error but continue trying other jobs in the batch
				span.AddEvent("Error updating job to consumed state", trace.WithAttributes(
					attribute.Int64("sqlq.job_id", j.ID),
					attribute.String("error", err.Error()),
				))
				continue
			}
			affected, _ := res.RowsAffected()
			if affected > 0 {
				// Successfully claimed
				jobsToReturn = append(jobsToReturn, j)
			} else {
				// Job was likely claimed by another consumer between SELECT and UPDATE
				span.AddEvent("Job already consumed", trace.WithAttributes(attribute.Int64("sqlq.job_id", j.ID)))
			}
		}
		return nil // Commit the successful updates
	})

	if err != nil {
		span.RecordError(fmt.Errorf("transaction failed during getJobsForConsumer: %w", err))
		return nil, err // Return nil slice and the error
	}

	span.SetAttributes(attribute.Int("sqlq.jobs_fetched", len(jobsToReturn)))
	return jobsToReturn, nil
}

// markJobProcessed updates the jobs table to mark a job as successfully processed in SQLite.
func (d *SQLiteDriver) markJobProcessed(ctx context.Context, jobID int64) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.mark_job_processed", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.Int64("sqlq.job_id", jobID),
		// Removed consumerName attribute
	))

	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	nowMs := time.Now().UnixMilli()

	res, err := d.db.ExecContext(ctx,
		`UPDATE jobs SET processed_at = ? WHERE id = ? AND consumed_at IS NOT NULL AND processed_at IS NULL`,
		nowMs, jobID,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to mark job as processed")
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

// markJobFailedAndReschedule updates a job's state to failed, increments the retry count,
// and schedules it for a future retry attempt in SQLite.
func (d *SQLiteDriver) markJobFailedAndReschedule(
	ctx context.Context,
	jobID int64,
	errorMsg string,
	backoffDuration time.Duration,
) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.mark_job_failed_and_reschedule", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.Int64("sqlq.job_id", jobID),
		attribute.String("sqlq.error_message", errorMsg),
		attribute.Float64("sqlq.backoff_seconds", backoffDuration.Seconds()),
	))

	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(fmt.Errorf("failed to begin transaction for reschedule: %w", err))
		return err
	}
	// Defer rollback and check error
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			span.AddEvent("Failed to rollback transaction during reschedule", trace.WithAttributes(attribute.String("error", rErr.Error())))
		}
	}()

	// Calculate new scheduled time in milliseconds
	nowMs := time.Now().UnixMilli()
	scheduledMs := nowMs + backoffDuration.Milliseconds()

	// Update the job: increment retry, set error, schedule, clear consumption/processing state
	_, err = tx.ExecContext(ctx, `
		UPDATE jobs SET 
			retry_count = retry_count + 1, 
			last_error = ?, 
			scheduled_at = ?,
			consumed_at = NULL, 
			processed_at = NULL,
			consumer_name = NULL
		WHERE id = ?`,
		errorMsg, scheduledMs, jobID,
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

// moveToDeadLetterQueue moves a failed job from the main jobs table to the dead_letter_queue table in SQLite.
func (d *SQLiteDriver) moveToDeadLetterQueue(ctx context.Context, jobID int64, reason string) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.move_to_dlq", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.Int64("sqlq.job_id", jobID),
		attribute.String("sqlq.dlq_reason", reason),
	))
	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(err)
		return err
	}
	// Defer rollback and check error
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			span.AddEvent("Failed to rollback transaction during DLQ move", trace.WithAttributes(attribute.String("error", rErr.Error())))
		}
	}()

	var job struct {
		JobType     string
		Payload     []byte
		ID          int64
		CreatedAtMs int64
		RetryCount  int
	}

	err = tx.QueryRowContext(ctx, `
		SELECT id, job_type, payload, created_at, retry_count
		FROM jobs WHERE id = ?
	`, jobID).Scan(&job.ID, &job.JobType, &job.Payload, &job.CreatedAtMs, &job.RetryCount)
	if err != nil {
		span.RecordError(err)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrJobNotFound
		}
		return err
	}

	// Get current time in milliseconds with microsecond precision
	nowMs := time.Now().UnixMicro() / 1000

	_, err = tx.ExecContext(ctx, `
		INSERT INTO dead_letter_queue 
		(original_job_id, job_type, payload, created_at, failed_at, retry_count, failure_reason)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, job.ID, job.JobType, job.Payload, job.CreatedAtMs, nowMs, job.RetryCount, reason)
	if err != nil {
		span.RecordError(err)
		return err
	}

	// Delete from jobs table (no CASCADE in SQLite by default, but job_consumers is gone anyway)
	_, err = tx.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, job.ID)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to delete job from main table: %w", err)
	}

	// No need to delete from job_consumers

	err = tx.Commit()
	if err != nil {
		span.RecordError(err)
	}
	return err
}

// getDeadLetterJobs retrieves jobs from the dead_letter_queue table in SQLite, optionally filtered by job type.
func (d *SQLiteDriver) getDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]DeadLetterJob, error) {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.get_dlq_jobs", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.String("sqlq.job_type", jobType), // jobType might be empty
		attribute.Int("sqlq.limit", limit),
	))
	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	query := `
		SELECT id, original_job_id, job_type, payload, created_at, failed_at, retry_count, failure_reason
		FROM dead_letter_queue
		WHERE job_type = ?
		ORDER BY failed_at DESC
		LIMIT ?
	`
	if jobType == "" {
		query = `
			SELECT id, original_job_id, job_type, payload, created_at, failed_at, retry_count, failure_reason
			FROM dead_letter_queue
			ORDER BY failed_at DESC
			LIMIT ?
		`
		return d.queryDeadLetterJobs(ctx, query, limit)
	}

	jobs, err := d.queryDeadLetterJobs(ctx, query, jobType, limit)
	if err != nil {
		span.RecordError(err)
	} else {
		span.SetAttributes(attribute.Int("sqlq.dlq_jobs_fetched", len(jobs)))
	}
	return jobs, err
}

func (d *SQLiteDriver) queryDeadLetterJobs(ctx context.Context, query string, args ...any) ([]DeadLetterJob, error) {
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	// Defer close and check error
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			// Cannot add to span here as it's not available, log instead or ignore.
			fmt.Printf("WARN: Failed to close rows in queryDeadLetterJobs helper: %v\n", closeErr)
		}
	}()

	var jobs []DeadLetterJob
	for rows.Next() {
		var j DeadLetterJob
		var createdAtMs, failedAtMs int64

		if err := rows.Scan(
			&j.ID,
			&j.OriginalID,
			&j.JobType,
			&j.Payload,
			&createdAtMs,
			&failedAtMs,
			&j.RetryCount,
			&j.FailureReason,
		); err != nil {
			// Don't record error in span here, let the caller handle it
			return nil, err
		}

		// Convert milliseconds to time.Time
		j.CreatedAt = time.UnixMilli(createdAtMs)
		j.FailedAt = time.UnixMilli(failedAtMs)

		jobs = append(jobs, j)
	}

	if err := rows.Err(); err != nil {
		// Don't record error in span here, let the caller handle it
		return nil, err
	}

	return jobs, nil
}

// requeueDeadLetterJob moves a job from the dead_letter_queue back into the main jobs table for reprocessing in SQLite.
func (d *SQLiteDriver) requeueDeadLetterJob(ctx context.Context, dlqID int64) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.requeue_dead_letter_job", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.Int64("sqlq.dlq_id", dlqID),
	))
	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(err)
		return err
	}

	// Defer rollback and check error
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			span.AddEvent(
				"Failed to rollback transaction during DLQ requeue",
				trace.WithAttributes(attribute.String("error", rErr.Error())),
			)
		}
	}()

	var dlqJob DeadLetterJob
	err = tx.QueryRowContext(ctx, `
		SELECT id, job_type, payload
		FROM dead_letter_queue WHERE id = ?
	`, dlqID).Scan(&dlqJob.ID, &dlqJob.JobType, &dlqJob.Payload)
	if err != nil {
		span.RecordError(err)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrJobNotFound
		}
		return err
	}

	// Get current time in milliseconds with microsecond precision
	nowMs := time.Now().UnixMicro() / 1000

	// reset retry count and scheduled_at = now
	_, err = tx.ExecContext(ctx, `
		INSERT INTO jobs (job_type, payload, retry_count, created_at, scheduled_at)
		VALUES (?, ?, 0, ?, ?)
	`, dlqJob.JobType, dlqJob.Payload, nowMs, nowMs)
	if err != nil {
		span.RecordError(err)
		return err
	}

	_, err = tx.ExecContext(ctx, "DELETE FROM dead_letter_queue WHERE id = ?", dlqID)
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
