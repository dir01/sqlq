package sqlq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
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
			id BIGSERIAL PRIMARY KEY,
			job_type TEXT NOT NULL,
			payload BYTEA,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
			scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
			retry_count INTEGER NOT NULL DEFAULT 0,
			last_error TEXT,
			trace_context JSONB, -- Added for trace propagation (using JSONB for efficiency)
			consumed_at TIMESTAMP WITH TIME ZONE NULL, -- Indicates when the job was claimed by a consumer
			processed_at TIMESTAMP WITH TIME ZONE NULL -- Indicates when the job was successfully processed
		)`,
		// Removed job_consumers table definition
		`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS claim_token TEXT NULL`,
		`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMP WITH TIME ZONE NULL`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_available_claims ON jobs(job_type, scheduled_at, claim_expires_at) WHERE processed_at IS NULL`,

		`CREATE INDEX IF NOT EXISTS idx_jobs_job_type ON jobs(job_type)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_scheduled_at_consumed_at ON jobs(scheduled_at, consumed_at)`, // Index for finding available jobs
		`CREATE INDEX IF NOT EXISTS idx_jobs_job_type_processed_at ON jobs(job_type, processed_at)`,       // Index for cleaning up processed jobs

		`CREATE TABLE IF NOT EXISTS dead_letter_queue (
			original_job_id BIGINT PRIMARY KEY, -- Use original job ID as PK
			job_type TEXT NOT NULL,
			payload BYTEA,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL,
			failed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
			retry_count INTEGER NOT NULL DEFAULT 0,
			failure_reason TEXT NOT NULL DEFAULT ''
		)`,
		// No separate ID index needed as original_job_id is PK
		`CREATE INDEX IF NOT EXISTS idx_dlq_job_type ON dead_letter_queue(job_type)`, // Index for filtering by type
		// Before, the dead-letter times were TIMESTAMP WITHOUT TIME ZONE: a local time without a zone.
		// failed_at was in the session time zone, and created_at was in the application time zone.
		// Thus, comparisons with an instant had an error equal to the zone offset, and cleanup could
		// delete new jobs. The migration reads old values as local times in the session time zone.
		// This is correct if the time zones of the database and the application did not change.
		// ALTER waits until a concurrent ALTER commits. Then it converts each value to the same instant.
		postgresTimestampTZMigration("dead_letter_queue", "created_at"),
		postgresTimestampTZMigration("dead_letter_queue", "failed_at"),
		// Before, the columns that the queue reads could be NULL. Rows with NULL from other writers
		// get the nearest known value. A scan of NULL failed. A job without created_at did not go to
		// the DLQ. A job without retry_count made all fetches of its type fail, and blocked the other
		// jobs. A job without scheduled_at was never fetched. The times are first, so that each fill
		// can use the other time.
		postgresNotNullMigration("jobs", "created_at", "COALESCE(scheduled_at, CURRENT_TIMESTAMP)"),
		postgresNotNullMigration("jobs", "scheduled_at", "created_at"),
		postgresNotNullMigration("jobs", "retry_count", "0"),
		postgresNotNullMigration("dead_letter_queue", "failed_at", "COALESCE(created_at, CURRENT_TIMESTAMP)"),
		postgresNotNullMigration("dead_letter_queue", "created_at", "failed_at"),
		postgresNotNullMigration("dead_letter_queue", "retry_count", "0"),
		postgresNotNullMigration("dead_letter_queue", "failure_reason", "''"),
		// Before, job IDs were 32-bit. After 2^31 jobs, all publish operations failed, also after cleanup.
		postgresBigintMigration("jobs", "id"),
		postgresBigintMigration("dead_letter_queue", "original_job_id"),
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

// postgresTimestampTZMigration returns a statement that changes column of table, in the current
// schema, from TIMESTAMP WITHOUT TIME ZONE to TIMESTAMP WITH TIME ZONE. It reads the values in the
// session time zone. If the column has a time zone, the statement does nothing.
func postgresTimestampTZMigration(table, column string) string {
	return fmt.Sprintf(`DO $$
		BEGIN
			IF EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = current_schema() AND table_name = '%[1]s' AND column_name = '%[2]s'
					AND data_type = 'timestamp without time zone'
			) THEN
				ALTER TABLE %[1]s ALTER COLUMN %[2]s TYPE TIMESTAMP WITH TIME ZONE
					USING %[2]s AT TIME ZONE current_setting('TimeZone');
			END IF;
		END $$`, table, column)
}

// postgresNotNullMigration returns a statement that sets column of table, in the current schema,
// to fill where it is NULL. Then it makes the column NOT NULL. If the column is NOT NULL, the statement does nothing.
func postgresNotNullMigration(table, column, fill string) string {
	return fmt.Sprintf(`DO $$
		BEGIN
			IF EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = current_schema() AND table_name = '%[1]s' AND column_name = '%[2]s'
					AND is_nullable = 'YES'
			) THEN
				UPDATE %[1]s SET %[2]s = %[3]s WHERE %[2]s IS NULL;
				ALTER TABLE %[1]s ALTER COLUMN %[2]s SET NOT NULL;
			END IF;
		END $$`, table, column, fill)
}

// postgresBigintMigration returns a statement that changes column of table, in the current schema,
// from INTEGER to BIGINT. It also changes the sequence of the column, if there is one.
// If the column is not INTEGER, the statement does nothing.
func postgresBigintMigration(table, column string) string {
	return fmt.Sprintf(`DO $$
		DECLARE
			seq TEXT;
		BEGIN
			IF EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = current_schema() AND table_name = '%[1]s' AND column_name = '%[2]s'
					AND data_type = 'integer'
			) THEN
				ALTER TABLE %[1]s ALTER COLUMN %[2]s TYPE BIGINT;
				seq := pg_get_serial_sequence('%[1]s', '%[2]s');
				IF seq IS NOT NULL THEN
					-- A maximum that was integer's becomes bigint's.
					EXECUTE format('ALTER SEQUENCE %%s AS BIGINT', seq);
				END IF;
			END IF;
		END $$`, table, column)
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
		err := runInTx(ctx, d.db, func(tx *sql.Tx) error {
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
		err := runInTx(ctx, d.db, func(tx *sql.Tx) error {
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
		// Use clock_timestamp, not NOW. In a transaction of the caller, NOW is the start time of
		// that transaction. Then the delay is shorter by the age of the transaction.
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
				WHERE job_type = $1 -- Correct parameter index
				  AND scheduled_at <= NOW()
				  AND processed_at IS NULL
				  AND (claim_expires_at IS NULL OR claim_expires_at <= clock_timestamp())
				ORDER BY scheduled_at, id
				FOR UPDATE SKIP LOCKED
				LIMIT $2 -- Correct parameter index
			)
			RETURNING id, payload, retry_count, trace_context, created_at, claim_token, last_error
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
			var createdAt sql.NullTime // The column is NOT NULL, but a failed scan would block the claimed job
			var lastError sql.NullString
			j.JobType = jobType // Set job type as it's not returned by RETURNING

			if err = rows.Scan(&j.ID, &j.Payload, &j.RetryCount, &traceContextJSON, &createdAt, &j.ClaimToken, &lastError); err != nil {
				// Undo the whole batch. Otherwise this job would stay claimed but never reach a worker.
				span.RecordError(fmt.Errorf("failed to scan returned job details (job_id: %d): %w", j.ID, err))
				return err
			}
			j.CreatedAt = createdAt.Time // Zero if created_at is NULL
			j.LastError = lastError.String
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

func (d *PostgresDriver) extendClaim(ctx context.Context, j job, timeout time.Duration, startAttempt bool) (time.Time, error) {
	deadline := localClaimDeadline(timeout)
	result, err := d.db.ExecContext(ctx, `UPDATE jobs SET claim_expires_at = clock_timestamp() + $1 * interval '1 millisecond',
			retry_count = CASE WHEN $4 AND retry_count < 2147483647 THEN retry_count + 1 ELSE retry_count END
		WHERE id = $2 AND claim_token = $3 AND processed_at IS NULL AND claim_expires_at > clock_timestamp()`,
		timeout.Milliseconds(), j.ID, j.ClaimToken, startAttempt)
	return deadline, checkClaimResult(result, err)
}

func (d *PostgresDriver) releaseClaim(ctx context.Context, j job) error {
	result, err := d.db.ExecContext(ctx, `UPDATE jobs SET consumed_at = NULL, claim_token = NULL, claim_expires_at = NULL, retry_count = $3
		WHERE id = $1 AND claim_token = $2 AND processed_at IS NULL`, j.ID, j.ClaimToken, int64(j.RetryCount))
	return checkClaimResult(result, err)
}

func (d *PostgresDriver) subscribeForConsumer(_ context.Context, _ string, _ *tokenBucket) (<-chan struct{}, error) {
	return nil, nil
}

// markJobProcessed updates the jobs table to mark a job as successfully processed in PostgreSQL.
// markProcessedLockAttempts is the maximum number of lock timeouts in markJobProcessed.
const markProcessedLockAttempts = 3

// isLockTimeout reports whether err is lock_not_available (55P03), the lock_timeout error of PostgreSQL.
// It compares the SQLSTATE. Thus, it works with all database/sql drivers that show it (pgx, lib/pq).
func isLockTimeout(err error) bool {
	var stateErr interface{ SQLState() string }
	return errors.As(err, &stateErr) && stateErr.SQLState() == "55P03"
}

func (d *PostgresDriver) markJobProcessed(ctx context.Context, tx *sql.Tx, jobID int64, claimToken string) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.mark_processed", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.Int64("sqlq.job_id", jobID),
		// Removed consumerName attribute
	))
	defer span.End()

	// A different claim query can lock this row for a short time, but not claim it. Its FOR UPDATE
	// examines the new claimed version of the row again, and keeps the lock until its transaction
	// commits. With a short lock_timeout, the update below can then time out after the handler
	// is successful. Thus, retry the update from a savepoint. The savepoint keeps the handler writes.
	if _, err := tx.ExecContext(ctx, `SAVEPOINT sqlq_mark_processed`); err != nil {
		span.RecordError(err)
		return err
	}
	var res sql.Result
	var err error
	// The start of the attempt incremented retry_count (see extendClaim). A success is not a retry.
	for attempt := 1; ; attempt++ {
		res, err = tx.ExecContext(ctx,
			`UPDATE jobs SET processed_at = clock_timestamp(), retry_count = GREATEST(retry_count - 1, 0)
				WHERE id = $1 AND claim_token = $2 AND processed_at IS NULL`,
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

// postgresText returns s as text that PostgreSQL can store. The text type of PostgreSQL does
// not accept NUL bytes, or invalid UTF-8 in a UTF-8 database. Then the failure of the job is
// not recorded. The function replaces each of these with U+FFFD.
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
		span.RecordError(fmt.Errorf("failed to begin transaction for reschedule: %w", err))
		return err
	}
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			span.AddEvent("Failed to rollback transaction during reschedule", trace.WithAttributes(attribute.String("error", rErr.Error())))
		}
	}()

	// Set the error and the schedule, and clear the claim. The start of the attempt already
	// incremented retry_count (see extendClaim).
	res, err := tx.ExecContext(ctx, `
		UPDATE jobs SET
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

	// No need to delete from job_consumers anymore

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
		SELECT job_type, payload, created_at, GREATEST(retry_count - 1, 0)
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
	`, jobID, j.JobType, j.Payload, j.CreatedAt, j.RetryCount, postgresText(reason)) // original_job_id is jobID
	if err != nil {
		span.RecordError(err)
		return err // Rollback will happen
	}

	// Delete from jobs table (job_consumers is deleted via CASCADE)
	res, err := tx.ExecContext(ctx, "DELETE FROM jobs WHERE id = $1 AND claim_token = $2 AND processed_at IS NULL", jobID, claimToken)
	err = checkClaimResult(res, err)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to delete job from main table: %w", err)
	}

	// No need to delete from job_consumers explicitly due to CASCADE

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
		if closeErr := rows.Close(); closeErr != nil {
			// Cannot add to span here as it's not available, log instead or ignore.
			fmt.Printf("WARN: Failed to close rows in queryDeadLetterJobs helper: %v\n", closeErr)
		}
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
func (d *PostgresDriver) requeueDeadLetterJob(ctx context.Context, originalJobID int64) (string, error) {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.postgres.requeue_dlq_job", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.Int64("sqlq.original_job_id", originalJobID), // Use original_job_id in attribute
	))
	defer span.End()

	tx, err := d.db.BeginTx(ctx, nil) // Use BeginTx with context
	if err != nil {
		span.RecordError(err)
		span.RecordError(err)
		return "", err
	}
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			span.AddEvent("Failed to rollback transaction during DLQ requeue", trace.WithAttributes(attribute.String("error", rErr.Error())))
		}
	}()

	// Take the DLQ row in one atomic operation. A concurrent requeue waits, and then finds
	// no row. Thus, it does not commit a second copy. If the INSERT fails, the DELETE
	// also rolls back. Thus, the failed job stays in the DLQ.
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
			return "", ErrJobNotFound
		}
		return "", err
	}

	// Insert back into the main queue with reset retry count and scheduled_at = NOW()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO jobs (job_type, payload, retry_count, scheduled_at)
		VALUES ($1, $2, 0, NOW())
	`, dlqJob.JobType, dlqJob.Payload) // Use ExecContext
	if err != nil {
		span.RecordError(err)
		return "", err // Rollback will happen
	}

	err = tx.Commit()
	if err != nil {
		span.RecordError(err)
		return "", err
	}
	return dlqJob.JobType, nil
}
