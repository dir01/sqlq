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
	mutex  sync.Mutex
	tracer trace.Tracer
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
			consumed_at INTEGER -- Milliseconds since epoch, indicates when the job was consumed
		)`,

		`CREATE INDEX IF NOT EXISTS idx_jobs_job_type ON jobs(job_type)`,

		`CREATE INDEX IF NOT EXISTS idx_jobs_scheduled_at ON jobs(scheduled_at)`,

		`CREATE TABLE IF NOT EXISTS job_consumers (
			job_id INTEGER,
			consumer_name TEXT,
			processed_at INTEGER NOT NULL, -- Milliseconds since epoch
			PRIMARY KEY (job_id, consumer_name),
			FOREIGN KEY (job_id) REFERENCES jobs(id)
		)`,

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

// cleanupJobs deletes old processed jobs and old dead-letter queue jobs for a specific job type in SQLite.
// It deletes processed jobs older than processedMaxAge and DLQ jobs older than dlqMaxAge, in batches.
func (d *SQLiteDriver) cleanupJobs(ctx context.Context, jobType string, processedMaxAge, dlqMaxAge time.Duration, batchSize uint16) (processedDeleted int64, dlqDeleted int64, err error) {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.cleanup_jobs", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.String("sqlq.job_type", jobType),
		attribute.String("sqlq.cleanup.age.processed", processedMaxAge.String()),
		attribute.String("sqlq.cleanup.age.dlq", dlqMaxAge.String()),
		attribute.Int("sqlq.cleanup.batch_size", int(batchSize)),
	))
	defer span.End()

	var totalProcessedDeleted int64
	var totalDlqDeleted int64

	// --- Cleanup Processed Jobs (Batched) ---
	processedThresholdMs := time.Now().Add(-processedMaxAge).UnixMilli()
	for {
		var batchDeleted int64
		err = d.runInTx(ctx, func(tx *sql.Tx) error {
			// 1. Find a batch of job IDs to delete
			rows, err := tx.QueryContext(ctx, `
				SELECT jc.job_id
				FROM job_consumers jc
				JOIN jobs j ON jc.job_id = j.id
				WHERE j.job_type = ? AND jc.processed_at < ?
				LIMIT ?
			`, jobType, processedThresholdMs, batchSize)
			if err != nil {
				return fmt.Errorf("failed to query batch of old job_consumers for type %s: %w", jobType, err)
			}

			var jobIDsToDelete []int64
			for rows.Next() {
				var jobID int64
				if err := rows.Scan(&jobID); err != nil {
					// Check close error here as well
					if closeErr := rows.Close(); closeErr != nil {
						span.AddEvent("Failed to close rows during job ID scan (on scan error)", trace.WithAttributes(attribute.String("error", closeErr.Error())))
					}
					return fmt.Errorf("failed to scan batch job_id for type %s: %w", jobType, err)
				}
				jobIDsToDelete = append(jobIDsToDelete, jobID)
			}
			rowsErr := rows.Err()
			if closeErr := rows.Close(); closeErr != nil { // Close should happen before checking rows.Err()
				span.AddEvent("Failed to close rows after job ID scan", trace.WithAttributes(attribute.String("error", closeErr.Error())))
			}
			if rowsErr != nil {
				return fmt.Errorf("error iterating batch job_consumers rows for type %s: %w", jobType, rowsErr)
			}

			if len(jobIDsToDelete) == 0 {
				return nil // No more jobs in this batch
			}

			// Prepare statements for batch deletion
			stmtConsumers, err := tx.PrepareContext(ctx, `DELETE FROM job_consumers WHERE job_id = ?`)
			if err != nil {
				return fmt.Errorf("failed to prepare delete statement for job_consumers: %w", err)
			}
			// Defer close and check error
			defer func() {
				if closeErr := stmtConsumers.Close(); closeErr != nil {
					span.AddEvent("Failed to close job_consumers delete statement", trace.WithAttributes(attribute.String("error", closeErr.Error())))
				}
			}()

			stmtJobs, err := tx.PrepareContext(ctx, `DELETE FROM jobs WHERE id = ?`)
			if err != nil {
				return fmt.Errorf("failed to prepare delete statement for jobs: %w", err)
			}
			// Defer close and check error
			defer func() {
				if closeErr := stmtJobs.Close(); closeErr != nil {
					span.AddEvent("Failed to close jobs delete statement", trace.WithAttributes(attribute.String("error", closeErr.Error())))
				}
			}()

			// 2 & 3. Delete from job_consumers and jobs one by one within the batch transaction
			for _, jobID := range jobIDsToDelete {
				_, err := stmtConsumers.ExecContext(ctx, jobID)
				if err != nil {
					// Rollback will happen due to error return
					return fmt.Errorf("failed to delete from job_consumers for job id %d: %w", jobID, err)
				}

				resJobs, err := stmtJobs.ExecContext(ctx, jobID)
				if err != nil {
					// Rollback will happen due to error return
					return fmt.Errorf("failed to delete from jobs for job id %d: %w", jobID, err)
				}
				affectedJobs, _ := resJobs.RowsAffected()
				batchDeleted += affectedJobs
			}
			return nil
		})

		if err != nil {
			span.RecordError(fmt.Errorf("error during processed job cleanup batch: %w", err))
			return totalProcessedDeleted, totalDlqDeleted, err // Return partial counts and error
		}

		totalProcessedDeleted += batchDeleted
		if batchDeleted < int64(batchSize) {
			break // Last batch processed
		}
	}

	// --- Cleanup DLQ Jobs (Batched) ---
	dlqThresholdMs := time.Now().Add(-dlqMaxAge).UnixMilli()
	for {
		var batchDeleted int64
		err = d.runInTx(ctx, func(tx *sql.Tx) error {
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
			return totalProcessedDeleted, totalDlqDeleted, err // Return partial counts and error
		}

		totalDlqDeleted += batchDeleted
		if batchDeleted < int64(batchSize) {
			break // Last batch processed
		}
	}

	span.SetAttributes(
		attribute.Int64("sqlq.db.rows_deleted.jobs", totalProcessedDeleted),
		attribute.Int64("sqlq.db.rows_deleted.dlq", totalDlqDeleted),
	)

	return totalProcessedDeleted, totalDlqDeleted, nil
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

	d.mutex.Lock()
	defer d.mutex.Unlock()

	nowMs := time.Now().UnixMilli()

	rows, err := d.db.QueryContext(ctx, `
		SELECT j.id, j.payload, j.retry_count, j.trace_context, j.scheduled_at
		FROM jobs j
		WHERE j.job_type = ? 
		AND j.scheduled_at <= ?
		AND j.consumed_at IS NULL
		ORDER BY j.id
		LIMIT ?
	`, jobType, nowMs, prefetchCount)
	if err != nil {
		span.RecordError(err)
		span.RecordError(err)
		return nil, err
	}
	// Defer close and check error
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			span.AddEvent("Failed to close rows after selecting potential jobs", trace.WithAttributes(attribute.String("error", closeErr.Error())))
		}
	}()

	var potentialJobs []job // Store potential jobs first
	for rows.Next() {
		var j job
		var traceContextJSON sql.NullString
		var scheduledAtMs int64
		j.JobType = jobType
		if err := rows.Scan(&j.ID, &j.Payload, &j.RetryCount, &traceContextJSON, &scheduledAtMs); err != nil {
			span.RecordError(err)
			return nil, err
		}

		// Deserialize trace context
		j.TraceContext = make(map[string]string)
		if traceContextJSON.Valid && traceContextJSON.String != "" {
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
		span.RecordError(err)
		return nil, err
	}
	// rows are closed by the defer func() added earlier

	// Now, try to "lock" the potential jobs by inserting into job_consumers
	var jobsToReturn []job
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(fmt.Errorf("failed to begin transaction for locking jobs: %w", err))
		span.RecordError(fmt.Errorf("failed to begin transaction for locking jobs: %w", err))
		return nil, err
	}
	// Defer rollback and check error
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			span.AddEvent("Failed to rollback transaction during job locking", trace.WithAttributes(attribute.String("error", rErr.Error())))
		}
	}()

	for _, j := range potentialJobs {
		// Attempt to update the job to lock it for this consumer
		_, err := tx.ExecContext(ctx,
			"UPDATE jobs SET consumed_at = ? WHERE id = ? AND consumed_at IS NULL",
			nowMs, j.ID,
		)
		if err == nil {
			// Successfully inserted placeholder, job is now "locked" for us
			jobsToReturn = append(jobsToReturn, j)
			if len(jobsToReturn) >= int(prefetchCount) {
				break // Reached prefetch limit
			}
		} else {
			// Check if the error is a UNIQUE constraint violation
			// We need to import "github.com/mattn/go-sqlite3" to check the error type robustly
			// For simplicity here, we assume any error means we couldn't lock it.
			// A production system might check sqlite3.ErrConstraintUnique specifically.
			span.AddEvent("Failed to acquire lock for job", trace.WithAttributes(
				attribute.Int64("sqlq.job_id", j.ID),
				attribute.String("error", err.Error()),
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

// markJobProcessed updates the job_consumers table to mark a job as successfully processed in SQLite.
func (d *SQLiteDriver) markJobProcessed(ctx context.Context, jobID int64, consumerName string) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.mark_job_processed", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.Int64("sqlq.job_id", jobID),
		attribute.String("sqlq.consumer_name", consumerName),
	))

	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Get current time in milliseconds
	nowMs := time.Now().UnixMilli()
	processedAtPlaceholder := 0 // Must match the placeholder used in GetJobsForConsumer

	res, err := d.db.ExecContext(ctx,
		`UPDATE job_consumers 
		 SET processed_at = ? 
		 WHERE job_id = ? AND consumer_name = ? AND processed_at = ?`,
		nowMs, jobID, consumerName, processedAtPlaceholder,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to update job_consumers")
		return err
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		// This could happen if:
		// 1. The job was already marked processed (processed_at != 0).
		// 2. The job failed and was rescheduled (placeholder row deleted).
		// 3. The job ID or consumer name is incorrect.
		// Log this as it might indicate an unexpected state or double processing attempt.
		span.AddEvent("MarkJobProcessed found no matching 'processing' row to update", trace.WithAttributes(
			attribute.Int64("sqlq.job_id", jobID),
			attribute.String("sqlq.consumer_name", consumerName),
		))
		// Depending on desired strictness, you might return an error here.
		// return fmt.Errorf("no processing job found for job_id %d and consumer %s to mark as processed", jobID, consumerName)
	} else {
		span.SetStatus(codes.Error, "failed to mark job as processed")
	}

	return err
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

	// Update the job itself
	_, err = tx.ExecContext(ctx,
		`UPDATE jobs SET 
			retry_count = retry_count + 1, 
			last_error = ?, 
			consumed_at = NULL,
			scheduled_at = ?
		WHERE id = ?`,
		errorMsg, scheduledMs, jobID,
	)
	if err != nil {
		span.RecordError(fmt.Errorf("failed to update jobs table on reschedule: %w", err))
		return err
	}

	// Remove the "processing" placeholder from job_consumers
	processedAtPlaceholder := 0 // Must match the placeholder used in GetJobsForConsumer
	_, err = tx.ExecContext(ctx,
		"DELETE FROM job_consumers WHERE job_id = ? AND processed_at = ?",
		jobID, processedAtPlaceholder,
	)
	if err != nil {
		// Log the error but don't necessarily fail the whole operation,
		// as the job update is the critical part. The placeholder might be missing
		// if something went wrong previously.
		span.RecordError(fmt.Errorf("failed to delete placeholder from job_consumers on reschedule (job_id: %d): %w", jobID, err))
	}

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
		ID          int64
		JobType     string
		Payload     []byte
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

	_, err = tx.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, job.ID)
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
			span.AddEvent("Failed to rollback transaction during DLQ requeue", trace.WithAttributes(attribute.String("error", rErr.Error())))
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
