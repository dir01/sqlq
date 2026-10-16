package sqlq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"encoding/json"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/pressly/goose/v3/lock"
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

// initSchema applies the queue's PostgreSQL migrations (see migrate). An advisory lock keeps
// processes starting together from applying them twice.
func (d *PostgresDriver) initSchema(ctx context.Context) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.init_schema", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
	))
	defer span.End()

	locker, err := lock.NewPostgresSessionLocker(lock.WithLockID(postgresMigrationLockID), lock.WithLockTimeout(1, 300))
	if err != nil {
		span.RecordError(err)
		return err
	}
	if err = migrate(ctx, d.db, database.DialectPostgres, "postgres", goose.WithSessionLocker(locker)); err != nil {
		span.RecordError(err)
	}
	return err
}

// cleanupJobs deletes old processed jobs for a specific job type in PostgreSQL.
// It deletes jobs where processed_at is older than maxAge, in batches.
// Retention uses PostgreSQL's clock, just like processed_at.
func (d *PostgresDriver) cleanupJobs(ctx context.Context, jobType string, maxAge time.Duration, batchSize uint16) (int64, error) {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.cleanup_jobs", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.String("sqlq.job_type", jobType),
		attribute.String("sqlq.cleanup.age", maxAge.String()),
		attribute.Int("sqlq.cleanup.batch_size", int(batchSize)),
	))
	defer span.End()

	totalDeleted, err := d.deleteOldInBatches(ctx, "jobs", "processed_at", jobType, maxAge, batchSize)
	if err != nil {
		err = fmt.Errorf("failed to delete batch of old processed jobs for type %s: %w", jobType, err)
		span.RecordError(fmt.Errorf("error during processed job cleanup batch: %w", err))
		return totalDeleted, err // Return partial counts and error
	}
	span.SetAttributes(attribute.Int64("sqlq.db.rows_deleted.processed", totalDeleted))
	return totalDeleted, nil
}

// cleanupDeadLetterQueueJobs deletes old jobs from the dead-letter queue for a specific job type.
// It deletes DLQ jobs older than maxAge, in batches.
// Retention uses PostgreSQL's clock, just like failed_at.
func (d *PostgresDriver) cleanupDeadLetterQueueJobs(ctx context.Context, jobType string, maxAge time.Duration, batchSize uint16) (int64, error) {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.cleanup_dlq_jobs", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.String("sqlq.job_type", jobType),
		attribute.String("sqlq.cleanup.age", maxAge.String()),
		attribute.Int("sqlq.cleanup.batch_size", int(batchSize)),
	))
	defer span.End()

	totalDlqDeleted, err := d.deleteOldInBatches(ctx, "dead_letter_queue", "failed_at", jobType, maxAge, batchSize)
	if err != nil {
		err = fmt.Errorf("failed to delete batch of old dlq jobs for type %s: %w", jobType, err)
		span.RecordError(fmt.Errorf("error during DLQ cleanup batch: %w", err))
		return totalDlqDeleted, err // Return partial counts and error
	}
	span.SetAttributes(attribute.Int64("sqlq.db.rows_deleted.dlq", totalDlqDeleted))
	return totalDlqDeleted, nil
}

// deleteOldInBatches deletes rows of jobType from table whose timestampColumn is older than maxAge
// by PostgreSQL's clock, one transaction per batch. It returns how many rows it deleted, even on error.
// table and timestampColumn are trusted identifiers, never user input.
func (d *PostgresDriver) deleteOldInBatches(
	ctx context.Context, table, timestampColumn, jobType string, maxAge time.Duration, batchSize uint16,
) (int64, error) {
	// Efficiently delete a batch using ctid in PostgreSQL
	query := fmt.Sprintf(`
		DELETE FROM %[1]s
		WHERE ctid IN (
			SELECT ctid
			FROM %[1]s
			WHERE job_type = $1 AND %[2]s < NOW() - $2 * interval '1 microsecond'
			LIMIT $3
		)
	`, table, timestampColumn)

	var totalDeleted int64
	for {
		var batchDeleted int64
		err := runInTx(ctx, d.db, func(tx *sql.Tx) error {
			res, err := tx.ExecContext(ctx, query, jobType, maxAge.Microseconds(), batchSize)
			if err != nil {
				return err
			}
			batchDeleted, _ = res.RowsAffected()
			return nil
		})
		if err != nil {
			return totalDeleted, err
		}

		totalDeleted += batchDeleted
		if batchDeleted < int64(batchSize) {
			return totalDeleted, nil // Last batch processed
		}
	}
}

// InsertJob inserts a new job into the PostgreSQL jobs table.
func (d *PostgresDriver) insertJob(
	ctx context.Context,
	tx *sql.Tx,
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
		// clock_timestamp, not NOW: in a caller's transaction, NOW is when that transaction began,
		// which would shorten the delay by the transaction's age.
		query = "INSERT INTO jobs (job_type, payload, scheduled_at, trace_context) VALUES ($1, $2, clock_timestamp() + $3 * interval '1 microsecond', $4)"
		args = []any{jobType, payload, delay.Microseconds(), string(traceContextJSON)}
	}

	var executor sqlExecutor = d.db
	if tx != nil {
		executor = tx
	}
	_, err = executor.ExecContext(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

// getJobsForConsumer selects and locks available jobs for a given consumer and job type from PostgreSQL.
// It also takes jobs whose claim expired. Only one consumer gets each job,
// and jobs that are done are never returned.
func (d *PostgresDriver) getJobsForConsumer(ctx context.Context, jobType string, prefetchCount uint16, claimTimeout time.Duration) ([]job, error) {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.get_jobs_for_consumer", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.String("sqlq.job_type", jobType),
		attribute.Int("sqlq.prefetch_count", int(prefetchCount)),
	))
	defer span.End()

	var jobsToReturn []job
	deadline := localClaimDeadline(claimTimeout)
	claimToken := newClaimToken()
	err := runInTx(ctx, d.db, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			UPDATE jobs
			SET consumed_at = clock_timestamp(), claim_token = $3 || ':' || id::text,
				claim_expires_at = clock_timestamp() + $4 * interval '1 millisecond'
			WHERE id IN (
				SELECT id
				FROM jobs
				WHERE job_type = $1
				  AND scheduled_at <= NOW()
				  AND processed_at IS NULL
				  AND (claim_expires_at IS NULL OR claim_expires_at <= clock_timestamp())
				ORDER BY scheduled_at, id
				FOR UPDATE SKIP LOCKED
				LIMIT $2
			)
			RETURNING id, payload, retry_count, trace_context, created_at, scheduled_at, claim_token
		`, jobType, prefetchCount, claimToken, claimTimeout.Milliseconds())

		if err != nil {
			return fmt.Errorf("failed to update and select jobs: %w", err)
		}
		defer func() {
			_ = rows.Close() // Ensure rows are closed within the transaction function
		}()

		for rows.Next() {
			var j job
			var traceContextJSON sql.NullString
			var createdAt sql.NullTime // NOT NULL now, but failing to scan would strand the claimed job
			j.JobType = jobType        // Set job type as it's not returned by RETURNING

			if err = rows.Scan(&j.ID, &j.Payload, &j.RetryCount, &traceContextJSON, &createdAt, &j.ScheduledAt, &j.ClaimToken); err != nil {
				// Undo the whole batch. Otherwise this job would stay claimed but never reach a worker.
				span.RecordError(fmt.Errorf("failed to scan returned job details (job_id: %d): %w", j.ID, err))
				return err
			}
			j.CreatedAt = createdAt.Time // Zero if created_at is NULL
			j.ClaimExpiresAt = deadline

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

func (d *PostgresDriver) extendClaim(ctx context.Context, j job, timeout time.Duration) (time.Time, error) {
	deadline := localClaimDeadline(timeout)
	result, err := d.db.ExecContext(ctx, `UPDATE jobs SET claim_expires_at = clock_timestamp() + $1 * interval '1 millisecond'
		WHERE id = $2 AND claim_token = $3 AND processed_at IS NULL AND claim_expires_at > clock_timestamp()`,
		timeout.Milliseconds(), j.ID, j.ClaimToken)
	return deadline, checkClaimResult(result, err)
}

func (d *PostgresDriver) releaseClaim(ctx context.Context, j job) error {
	result, err := d.db.ExecContext(ctx, `UPDATE jobs SET consumed_at = NULL, claim_token = NULL, claim_expires_at = NULL
		WHERE id = $1 AND claim_token = $2 AND processed_at IS NULL`, j.ID, j.ClaimToken)
	return checkClaimResult(result, err)
}

func (d *PostgresDriver) subscribeForConsumer(_ context.Context, _ string, _ *tokenBucket) (<-chan struct{}, error) {
	return nil, nil
}

// markJobProcessed updates the jobs table to mark a job as successfully processed in PostgreSQL.
// markProcessedLockAttempts bounds how often markJobProcessed waits out a lock timeout.
const markProcessedLockAttempts = 3

// isLockTimeout reports whether err is PostgreSQL's lock_not_available (55P03), the lock_timeout error.
// Matched by SQLSTATE so that any database/sql driver exposing it (pgx, lib/pq) works.
func isLockTimeout(err error) bool {
	var stateErr interface{ SQLState() string }
	return errors.As(err, &stateErr) && stateErr.SQLState() == "55P03"
}

func (d *PostgresDriver) markJobProcessed(ctx context.Context, tx *sql.Tx, jobID int64, claimToken string) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.mark_processed", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.Int64("sqlq.job_id", jobID),
	))
	defer span.End()

	// A competing claim query can briefly lock this row although it doesn't take it: its FOR UPDATE
	// rechecks the row's newer, claimed version and keeps the lock until that query's transaction
	// commits. With a short lock_timeout, the update below could then time out after the handler
	// succeeded. Retry it from a savepoint, which keeps the handler's writes.
	if _, err := tx.ExecContext(ctx, `SAVEPOINT sqlq_mark_processed`); err != nil {
		span.RecordError(err)
		return err
	}
	var res sql.Result
	var err error
	for attempt := 1; ; attempt++ {
		res, err = tx.ExecContext(ctx,
			`UPDATE jobs SET processed_at = clock_timestamp() WHERE id = $1 AND claim_token = $2 AND processed_at IS NULL`,
			jobID, claimToken,
		)
		if err == nil || attempt == markProcessedLockAttempts || !isLockTimeout(err) {
			break
		}
		span.AddEvent("lock timeout marking job processed, retrying", trace.WithAttributes(attribute.Int("sqlq.attempt", attempt)))
		if _, rbErr := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT sqlq_mark_processed`); rbErr != nil {
			span.RecordError(rbErr)
			return err
		}
	}
	if err != nil {
		span.RecordError(err)
		return err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		span.RecordError(err)
		return err
	}
	if rowsAffected != 1 {
		err := fmt.Errorf("job %d: %w", jobID, ErrClaimLost)
		span.RecordError(err)
		return err
	}

	return nil
}

// postgresText returns s as text PostgreSQL can store: its text type rejects NUL bytes and,
// in a UTF-8 database, invalid UTF-8, and either would keep a job's failure from being
// recorded. Each is replaced with U+FFFD.
func postgresText(s string) string {
	return strings.ToValidUTF8(strings.ReplaceAll(s, "\x00", "\uFFFD"), "\uFFFD")
}

// MarkJobFailedAndReschedule updates a job's state to failed, increments the retry count,
// and schedules it for a future retry attempt in PostgreSQL.
func (d *PostgresDriver) markJobFailedAndReschedule(ctx context.Context, jobID int64, claimToken string, errorMsg string, backoffDuration time.Duration) error {
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
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			span.AddEvent("Failed to rollback transaction during reschedule", trace.WithAttributes(attribute.String("error", rErr.Error())))
		}
	}()

	// Update the job: increment retry, set error, schedule, clear consumption/processing state
	res, err := tx.ExecContext(ctx, `
		UPDATE jobs SET
			retry_count = CASE WHEN retry_count < 2147483647 THEN retry_count + 1 ELSE retry_count END,
			last_error = $1,
			scheduled_at = clock_timestamp() + $2 * interval '1 microsecond',
			consumed_at = NULL,
			claim_token = NULL,
			claim_expires_at = NULL,
			processed_at = NULL
		WHERE id = $3 AND claim_token = $4 AND processed_at IS NULL`,
		postgresText(errorMsg), backoffDuration.Microseconds(), jobID, claimToken,
	)
	err = checkClaimResult(res, err)
	if err != nil {
		span.RecordError(fmt.Errorf("failed to update jobs table on reschedule: %w", err))
		return err // Rollback will happen
	}

	err = tx.Commit()
	if err != nil {
		span.RecordError(fmt.Errorf("failed to commit transaction for reschedule: %w", err))
	}
	return err
}

// moveToDeadLetterQueue moves a failed job from the main jobs table to the dead_letter_queue table in PostgreSQL, using the original job ID as the primary key.
func (d *PostgresDriver) moveToDeadLetterQueue(ctx context.Context, jobID int64, claimToken string, reason string, inTx func(tx *sql.Tx) error) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.move_to_dlq", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.Int64("sqlq.original_job_id", jobID), // Use original_job_id in attribute
		attribute.String("sqlq.dlq_reason", reason),
	))
	defer span.End()

	tx, err := d.db.BeginTx(ctx, nil) // Use BeginTx with context
	if err != nil {
		span.RecordError(err)
		return err
	}
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			span.AddEvent("Failed to rollback transaction during DLQ move", trace.WithAttributes(attribute.String("error", rErr.Error())))
		}
	}()

	// Get the job details (no need to select ID, we have jobID)
	var j struct { // Define local struct for scanning
		CreatedAt  time.Time
		JobType    string
		Payload    []byte
		RetryCount uint32
	}
	err = tx.QueryRowContext(ctx, `
		SELECT job_type, payload, created_at, retry_count
		FROM jobs WHERE id = $1 AND claim_token = $2 AND processed_at IS NULL FOR UPDATE
	`, jobID, claimToken).Scan(&j.JobType, &j.Payload, &j.CreatedAt, &j.RetryCount)
	if err != nil {
		span.RecordError(err)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrClaimLost
		}
		return err
	}

	// Insert into dead letter queue
	// Use ExecContext consistently
	_, err = tx.ExecContext(ctx, `
		INSERT INTO dead_letter_queue
		(original_job_id, job_type, payload, created_at, retry_count, failure_reason)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, jobID, j.JobType, j.Payload, j.CreatedAt, j.RetryCount, postgresText(reason)) // Use jobID directly for original_job_id
	if err != nil {
		span.RecordError(err)
		return err // Rollback will happen
	}

	// Delete from jobs table
	res, err := tx.ExecContext(ctx, "DELETE FROM jobs WHERE id = $1 AND claim_token = $2 AND processed_at IS NULL", jobID, claimToken)
	err = checkClaimResult(res, err)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to delete job from main table: %w", err)
	}

	if inTx != nil {
		if err = inTx(tx); err != nil {
			span.RecordError(err)
			return err // Rollback will happen
		}
	}

	err = tx.Commit()
	if err != nil {
		span.RecordError(err)
	}
	return err
}

// getReadyJobStats counts the jobs of jobType that a consumer can claim now, in PostgreSQL.
// now() is stable within the statement, so the planner can use it in index conditions.
func (d *PostgresDriver) getReadyJobStats(ctx context.Context, jobType string) (readyJobStats, error) {
	var stats readyJobStats
	var lagSeconds float64
	err := d.db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(EXTRACT(EPOCH FROM now() - MIN(scheduled_at)), 0)::float8
		FROM jobs
		WHERE job_type = $1 AND processed_at IS NULL AND scheduled_at <= now()
		  AND (claim_expires_at IS NULL OR claim_expires_at <= now())
	`, jobType).Scan(&stats.count, &lagSeconds)
	stats.lag = time.Duration(lagSeconds * float64(time.Second))
	return stats, err
}

// getDeadLetterJobs retrieves jobs from the dead_letter_queue table in PostgreSQL, optionally filtered by job type.
func (d *PostgresDriver) getDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]DeadLetterJob, error) { //nolint:revive // Limit is fine
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.get_dlq_jobs", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.String("sqlq.job_type", jobType), // jobType might be empty
		attribute.Int("sqlq.limit", limit),
	))
	defer span.End()

	query := `
		SELECT original_job_id, job_type, payload, created_at, failed_at, retry_count, failure_reason
		FROM dead_letter_queue
		WHERE job_type = $1
		ORDER BY failed_at DESC -- Still order by failure time
		LIMIT $2
	`
	if jobType == "" {
		query = `
			SELECT original_job_id, job_type, payload, created_at, failed_at, retry_count, failure_reason
			FROM dead_letter_queue
			ORDER BY failed_at DESC -- Still order by failure time
			LIMIT $1
		`
		// Pass only limit when jobType is empty
		jobs, err := d.queryDeadLetterJobs(ctx, query, limit)
		if err != nil {
			span.RecordError(err)
		} else {
			span.SetAttributes(attribute.Int("sqlq.dlq_jobs_fetched", len(jobs)))
		}
		return jobs, err
	}

	// Pass jobType and limit when jobType is specified
	jobs, err := d.queryDeadLetterJobs(ctx, query, jobType, limit)
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
		_ = rows.Close() // rows.Err reports read errors; a Close error after that changes nothing.
	}()

	var jobs []DeadLetterJob
	for rows.Next() {
		var j DeadLetterJob
		if err := rows.Scan(
			&j.OriginalID, // Scan original_job_id directly into OriginalID
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

// requeueDeadLetterJob moves a job from the dead_letter_queue back into the main jobs table for reprocessing in PostgreSQL, identifying the job by its original ID.
func (d *PostgresDriver) requeueDeadLetterJob(ctx context.Context, originalJobID int64) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.requeue_dlq_job", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.Int64("sqlq.original_job_id", originalJobID), // Use original_job_id in attribute
	))
	defer span.End()

	tx, err := d.db.BeginTx(ctx, nil) // Use BeginTx with context
	if err != nil {
		span.RecordError(err)
		return err
	}
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			span.AddEvent("Failed to rollback transaction during DLQ requeue", trace.WithAttributes(attribute.String("error", rErr.Error())))
		}
	}()

	// Atomically take the DLQ row. A concurrent requeue waits and then observes
	// no row, rather than committing a second replacement. An INSERT failure
	// rolls back this DELETE too, so the failed job remains recoverable.
	var dlqJob struct { // Select only needed fields
		JobType string
		Payload []byte
	}
	err = tx.QueryRowContext(ctx, `
		DELETE FROM dead_letter_queue WHERE original_job_id = $1
		RETURNING job_type, payload
	`, originalJobID).Scan(&dlqJob.JobType, &dlqJob.Payload) // Use originalJobID in WHERE
	if err != nil {
		span.RecordError(err)
		if errors.Is(err, sql.ErrNoRows) {
			// Use ErrJobNotFound consistently
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
		return err // Rollback will happen
	}

	err = tx.Commit()
	if err != nil {
		span.RecordError(err)
	}
	return err
}
