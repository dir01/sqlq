package sqlq

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
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
	db         *sql.DB
	tracer     trace.Tracer
	notif      map[string]*sqliteNotificationSubscription
	dbMutex    sync.Mutex
	notifMutex sync.RWMutex
}

type sqliteNotificationSubscription struct {
	// notifChan is used just to let know that jobs may be queried
	notifChan chan struct{}
	// bucket is used to limit rate at which we are delivering notifications
	bucket *tokenBucket
}

// newSQLiteDriver creates a new SQLite driver with the given database connection.
func newSQLiteDriver(db *sql.DB) *SQLiteDriver {
	return &SQLiteDriver{
		db:         db,
		dbMutex:    sync.Mutex{},
		notif:      make(map[string]*sqliteNotificationSubscription),
		notifMutex: sync.RWMutex{},
		tracer:     otel.Tracer(sqliteTracerName),
	}
}

// initSchema creates the necessary tables and indexes for SQLite if they don't exist.
// It's idempotent and safe to call multiple times.
func (d *SQLiteDriver) initSchema(ctx context.Context) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.init_schema", trace.WithAttributes(
		semconv.DBSystemSqlite,
	))
	defer span.End()

	d.dbMutex.Lock()
	defer d.dbMutex.Unlock()

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
			processed_at INTEGER NULL -- Milliseconds since epoch, indicates successful processing
		)`,
		// Removed job_consumers table definition

		`CREATE INDEX IF NOT EXISTS idx_jobs_job_type ON jobs(job_type)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_scheduled_at_consumed_at ON jobs(scheduled_at, consumed_at)`, // Index for finding available jobs
		`CREATE INDEX IF NOT EXISTS idx_jobs_job_type_processed_at ON jobs(job_type, processed_at)`,       // Index for cleaning up processed jobs

		`CREATE TABLE IF NOT EXISTS dead_letter_queue (
			original_job_id INTEGER PRIMARY KEY, -- Use original job ID as PK
			job_type TEXT NOT NULL,
			payload BLOB,
			created_at INTEGER NOT NULL, -- Milliseconds since epoch
			failed_at INTEGER NOT NULL, -- Milliseconds since epoch
			retry_count INTEGER,
			failure_reason TEXT
		)`,
		// No separate ID index needed as original_job_id is PK
		`CREATE INDEX IF NOT EXISTS idx_dlq_job_type ON dead_letter_queue(job_type)`, // Index for filtering by type
	}

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
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
	// CREATE TABLE IF NOT EXISTS does not upgrade existing installations.
	rows, err := tx.QueryContext(ctx, `PRAGMA table_info(jobs)`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	columns := make(map[string]bool)
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue sql.NullString
		if scanErr := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); scanErr != nil {
			return scanErr
		}
		columns[name] = true
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return err
	}
	if !columns["claim_token"] {
		// Versions without claims never deleted dead-lettered jobs (they deleted id 0).
		// Finish those moves, or claim recovery would run the jobs again. IDs are
		// AUTOINCREMENT, so a jobs row with a DLQ'd ID can only be such a leftover.
		if _, deleteErr := tx.ExecContext(ctx, `DELETE FROM jobs WHERE id IN (SELECT original_job_id FROM dead_letter_queue)`); deleteErr != nil {
			return deleteErr
		}
	}
	for _, column := range []struct{ name, definition string }{
		{"claim_token", "TEXT NULL"}, {"claim_expires_at", "INTEGER NULL"},
	} {
		if !columns[column.name] {
			if _, alterErr := tx.ExecContext(ctx, `ALTER TABLE jobs ADD COLUMN `+column.name+` `+column.definition); alterErr != nil {
				return alterErr
			}
		}
	}
	if _, indexErr := tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_jobs_available_claims ON jobs(job_type, scheduled_at, claim_expires_at) WHERE processed_at IS NULL`); indexErr != nil {
		return indexErr
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
		d.dbMutex.Lock()
		err := runInTx(ctx, d.db, func(tx *sql.Tx) error {
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
		d.dbMutex.Unlock()
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
		d.dbMutex.Lock()
		err := runInTx(ctx, d.db, func(tx *sql.Tx) error {
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
		d.dbMutex.Unlock()
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

// InsertJob inserts a new job into the SQLite jobs table.
func (d *SQLiteDriver) insertJob(
	ctx context.Context,
	tx *sql.Tx,
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

	var executor sqlExecutor = d.db
	if tx != nil {
		// A caller-owned transaction may already hold SQLite's write lock.
		// Taking dbMutex here could deadlock with a queue operation waiting on it.
		executor = tx
	} else {
		d.dbMutex.Lock()
		defer d.dbMutex.Unlock()
	}

	traceContextJSON := []byte("")
	if len(traceContext) > 0 {
		if b, err := json.Marshal(traceContext); err == nil {
			traceContextJSON = b
		} else {
			span.RecordError(fmt.Errorf("failed to marshal trace context: %w", err))
		}
	}

	// Use SQLite's built-in functions to get current time in milliseconds
	// and calculate scheduled time based on delay
	var query string
	var args []any

	now := time.Now()
	nowMs := now.UnixMilli()

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

	if _, err := executor.ExecContext(ctx, query, args...); err != nil {
		span.RecordError(err)
		return err
	}

	if tx != nil {
		// The caller has not committed yet. Polling will discover the job after
		// commit; a push here could wake a consumer before the row is visible.
		return nil
	}

	var notif *sqliteNotificationSubscription
	d.notifMutex.RLock()
	notif = d.notif[jobType]
	d.notifMutex.RUnlock()

	if notif != nil {
		if notif.bucket == nil || notif.bucket.SpendToken(time.Now().UnixMilli()) {
			select {
			case notif.notifChan <- struct{}{}:
			default:
			}
		}
	}

	return nil
}

// getJobsForConsumer selects and locks available jobs for a given consumer and job type from SQLite.
// Expired claims are replaced atomically; processed jobs are never returned.
func (d *SQLiteDriver) getJobsForConsumer(ctx context.Context, jobType string, prefetchCount uint16, claimTimeout time.Duration) ([]job, error) {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.get_jobs_for_consumer", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.String("sqlq.job_type", jobType),
		attribute.Int("sqlq.prefetch_count", int(prefetchCount)),
	))
	defer span.End()

	jobsToReturn := make([]job, 0, prefetchCount)
	deadline := localClaimDeadline(claimTimeout)
	claimToken, tokenErr := newClaimToken()
	if tokenErr != nil {
		return nil, tokenErr
	}

	// Claim with a single statement so the transaction starts as a write. A SELECT
	// followed by UPDATE would have to upgrade a read snapshot, which SQLite rejects
	// with SQLITE_BUSY whenever another connection has written in between, without
	// waiting on the busy timeout.
	d.dbMutex.Lock()
	err := runInTx(ctx, d.db, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			UPDATE jobs
			SET consumed_at = `+sqliteNow+`, claim_token = ? || ':' || id,
				claim_expires_at = `+sqliteNow+` + ?
			WHERE id IN (
				SELECT id
				FROM jobs
				WHERE job_type = ?
				AND scheduled_at <= `+sqliteNow+`
				AND processed_at IS NULL
				AND (claim_expires_at IS NULL OR claim_expires_at <= `+sqliteNow+`)
				ORDER BY id
				LIMIT ?
			)
			RETURNING id, payload, retry_count, trace_context, created_at, claim_token
		`, claimToken, claimTimeout.Milliseconds(), jobType, prefetchCount)
		if err != nil {
			return fmt.Errorf("failed to claim jobs: %w", err)
		}

		defer func() {
			_ = rows.Close()
		}()

		for rows.Next() {
			var j job
			var traceContextJSON sql.NullString
			var createdAtMs int64
			j.JobType = jobType
			if err = rows.Scan(&j.ID, &j.Payload, &j.RetryCount, &traceContextJSON, &createdAtMs, &j.ClaimToken); err != nil {
				return fmt.Errorf("failed to scan claimed job: %w", err) // Return error to rollback
			}
			j.CreatedAt = time.UnixMilli(createdAtMs)
			j.ClaimExpiresAt = deadline

			// Deserialize trace context
			j.TraceContext = make(map[string]string)
			if traceContextJSON.Valid && traceContextJSON.String != "" {
				if err = json.Unmarshal([]byte(traceContextJSON.String), &j.TraceContext); err != nil {
					span.RecordError(fmt.Errorf("failed to unmarshal trace context for job %d: %w", j.ID, err))
					j.TraceContext = make(map[string]string) // Reset on error
				}
			}
			jobsToReturn = append(jobsToReturn, j)
		}
		if err = rows.Err(); err != nil {
			return fmt.Errorf("error iterating claimed jobs: %w", err)
		}
		// RETURNING order is unspecified.
		slices.SortFunc(jobsToReturn, func(a, b job) int { return cmp.Compare(a.ID, b.ID) })
		return nil
	})
	d.dbMutex.Unlock()
	if err != nil {
		span.RecordError(fmt.Errorf("transaction failed during getJobsForConsumer: %w", err))
		return nil, err // Return nil slice and the error
	}

	span.SetAttributes(attribute.Int("sqlq.jobs_fetched", len(jobsToReturn)))
	return jobsToReturn, nil
}

func (d *SQLiteDriver) subscribeForConsumer(_ context.Context, jobType string, tb *tokenBucket) (<-chan struct{}, error) {
	// With SQLite, we expect to be in the same process as the publisher.
	// Because of that, instead of getting a signal that something happened
	// and then re-fetching jobs from the database and publishing them to a jobs channel,
	// we will just post directly to a jobs channel from a publisher.
	d.notifMutex.Lock()
	defer d.notifMutex.Unlock()

	if _, exists := d.notif[jobType]; exists {
		return nil, ErrDuplicateConsumer
	}

	ch := make(chan struct{})
	d.notif[jobType] = &sqliteNotificationSubscription{notifChan: ch, bucket: tb}

	return ch, nil
}

// markJobProcessed updates the jobs table to mark a job as successfully processed in SQLite.
func (d *SQLiteDriver) markJobProcessed(ctx context.Context, tx *sql.Tx, jobID int64, claimToken string) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.mark_job_processed", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.Int64("sqlq.job_id", jobID),
	))

	defer span.End()

	nowMs := time.Now().UnixMilli()

	res, err := tx.ExecContext(ctx,
		`UPDATE jobs SET processed_at = ? WHERE id = ? AND claim_token = ? AND processed_at IS NULL`,
		nowMs, jobID, claimToken,
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to mark job as processed")
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

// markJobFailedAndReschedule updates a job's state to failed, increments the retry count,
// and schedules it for a future retry attempt in SQLite.
func (d *SQLiteDriver) markJobFailedAndReschedule(
	ctx context.Context,
	jobID int64,
	claimToken string,
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

	d.dbMutex.Lock()
	defer d.dbMutex.Unlock()

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
	res, err := tx.ExecContext(ctx, `
		UPDATE jobs SET 
			retry_count = retry_count + 1, 
			last_error = ?, 
			scheduled_at = ?,
			consumed_at = NULL, 
			claim_token = NULL,
			claim_expires_at = NULL,
			processed_at = NULL
		WHERE id = ? AND claim_token = ? AND processed_at IS NULL`,
		errorMsg, scheduledMs, jobID, claimToken,
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

// moveToDeadLetterQueue moves a failed job from the main jobs table to the dead_letter_queue table in SQLite, using the original job ID as the primary key.
// inTx runs while dbMutex is held, so it must not call driver methods that take it.
func (d *SQLiteDriver) moveToDeadLetterQueue(ctx context.Context, jobID int64, claimToken string, reason string, inTx func(tx *sql.Tx) error) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.move_to_dlq", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.Int64("sqlq.original_job_id", jobID), // Use original_job_id in attribute
		attribute.String("sqlq.dlq_reason", reason),
	))
	defer span.End()

	d.dbMutex.Lock()
	defer d.dbMutex.Unlock()

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
		CreatedAtMs int64
		RetryCount  int
	}

	err = tx.QueryRowContext(ctx, `
		SELECT job_type, payload, created_at, retry_count
		FROM jobs WHERE id = ? AND claim_token = ? AND processed_at IS NULL
	`, jobID, claimToken).Scan(&job.JobType, &job.Payload, &job.CreatedAtMs, &job.RetryCount)
	if err != nil {
		span.RecordError(err)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrClaimLost
		}
		return err
	}

	// Get current time in milliseconds with microsecond precision
	nowMs := time.Now().UnixMicro() / 1000

	_, err = tx.ExecContext(ctx, `
		INSERT INTO dead_letter_queue
		(original_job_id, job_type, payload, created_at, failed_at, retry_count, failure_reason)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, jobID, job.JobType, job.Payload, job.CreatedAtMs, nowMs, job.RetryCount, reason) // Use jobID directly for original_job_id
	if err != nil {
		span.RecordError(err)
		return err // Rollback will happen
	}

	// Delete from jobs table (no CASCADE in SQLite by default, but job_consumers is gone anyway)
	res, err := tx.ExecContext(ctx, `DELETE FROM jobs WHERE id = ? AND claim_token = ? AND processed_at IS NULL`, jobID, claimToken)
	err = checkClaimResult(res, err)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to delete job from main table: %w", err)
	}

	// No need to delete from job_consumers

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

// getDeadLetterJobs retrieves jobs from the dead_letter_queue table in SQLite, optionally filtered by job type.
func (d *SQLiteDriver) getDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]DeadLetterJob, error) { //nolint:revive // Limit is fine
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.get_dlq_jobs", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.String("sqlq.job_type", jobType), // jobType might be empty
		attribute.Int("sqlq.limit", limit),
	))
	defer span.End()

	d.dbMutex.Lock()
	defer d.dbMutex.Unlock()

	query := `
		SELECT original_job_id, job_type, payload, created_at, failed_at, retry_count, failure_reason
		FROM dead_letter_queue
		WHERE job_type = ?
		ORDER BY failed_at DESC -- Still order by failure time
		LIMIT ?
	`
	if jobType == "" {
		query = `
			SELECT original_job_id, job_type, payload, created_at, failed_at, retry_count, failure_reason
			FROM dead_letter_queue
			ORDER BY failed_at DESC -- Still order by failure time
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
			&j.OriginalID, // Scan original_job_id directly into OriginalID
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

// requeueDeadLetterJob moves a job from the dead_letter_queue back into the main jobs table for reprocessing in SQLite, identifying the job by its original ID.
func (d *SQLiteDriver) requeueDeadLetterJob(ctx context.Context, originalJobID int64) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.requeue_dead_letter_job", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.Int64("sqlq.original_job_id", originalJobID), // Use original_job_id in attribute
	))
	defer span.End()

	d.dbMutex.Lock()
	defer d.dbMutex.Unlock()

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

	var dlqJob struct { // Select only needed fields
		JobType string
		Payload []byte
	}
	err = tx.QueryRowContext(ctx, `
		SELECT job_type, payload
		FROM dead_letter_queue WHERE original_job_id = ?
	`, originalJobID).Scan(&dlqJob.JobType, &dlqJob.Payload) // Use originalJobID in WHERE
	if err != nil {
		span.RecordError(err)
		if errors.Is(err, sql.ErrNoRows) {
			// Use ErrJobNotFound consistently
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
		return err // Rollback will happen
	}

	_, err = tx.ExecContext(ctx, "DELETE FROM dead_letter_queue WHERE original_job_id = ?", originalJobID) // Use originalJobID in WHERE
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
