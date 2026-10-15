package sqlq

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"encoding/json"

	"github.com/pressly/goose/v3/database"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const sqliteTracerName = "github.com/dir01/sqlq/driver_sqlite"

// sqliteNow is SQL that gives the current time in Unix milliseconds.
// The formula is from the SQLite docs: https://www.sqlite.org/lang_datefunc.html#examples
// (unixepoch('now','subsec') is simpler, but needs SQLite 3.42). ROUND fixes float error
// that would otherwise make CAST cut the value down by one millisecond.
//
// We use it instead of passing time.Now(), because SQLite reads 'now' when the statement
// runs, after any wait for dbLock or the write lock. A time.Now() value would be old by then.
// 'now' also stays the same for the whole statement, so one UPDATE uses one time everywhere.
//
// localClaimDeadline uses the time before the request on purpose: waiting must not add
// to the worker's time.
const sqliteNow = "CAST(ROUND((julianday('now') - 2440587.5) * 86400000) AS INTEGER)"

// SQLiteDriver implements the Driver interface for SQLite
type SQLiteDriver struct {
	db         *sql.DB
	tracer     trace.Tracer
	notif      map[string]*sqliteNotificationSubscription
	dbLock     chan struct{} // One slot: serializes queue writes in this process; see lock
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
		dbLock:     make(chan struct{}, 1),
		notif:      make(map[string]*sqliteNotificationSubscription),
		notifMutex: sync.RWMutex{},
		tracer:     otel.Tracer(sqliteTracerName),
	}
}

// lock takes dbLock, or returns ctx's error if ctx ends first. A holder may be waiting
// for a pool connection on behalf of an unrelated caller, and a sync.Mutex wait would
// keep a cancelled consumer, and so Shutdown, waiting for that caller.
func (d *SQLiteDriver) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case d.dbLock <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *SQLiteDriver) unlock() { <-d.dbLock }

// initSchema applies the queue's SQLite migrations (see migrate). dbLock keeps this process's
// other database work out of the way.
func (d *SQLiteDriver) initSchema(ctx context.Context) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.init_schema", trace.WithAttributes(
		semconv.DBSystemSqlite,
	))
	defer span.End()

	if err := d.lock(ctx); err != nil {
		span.RecordError(err)
		return err
	}
	defer d.unlock()

	err := migrate(ctx, d.db, database.DialectSQLite3, "sqlite")
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
		// The database serializes writers. Avoid an uncancellable Go mutex
		// wait here: shutdown must be able to cancel cleanup's pool/SQL wait.
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
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.cleanup_dlq_jobs", trace.WithAttributes(
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
		// Taking dbLock here could deadlock with a queue operation waiting on it.
		executor = tx
	} else {
		if err := d.lock(ctx); err != nil {
			span.RecordError(err)
			return err
		}
		defer d.unlock()
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
// It also takes jobs whose claim expired. Only one consumer gets each job,
// and jobs that are done are never returned.
func (d *SQLiteDriver) getJobsForConsumer(ctx context.Context, jobType string, prefetchCount uint16, claimTimeout time.Duration) ([]job, error) {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.get_jobs_for_consumer", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.String("sqlq.job_type", jobType),
		attribute.Int("sqlq.prefetch_count", int(prefetchCount)),
	))
	defer span.End()

	jobsToReturn := make([]job, 0, prefetchCount)
	deadline := localClaimDeadline(claimTimeout)
	claimToken := newClaimToken()

	// Claim the jobs in one statement, so the transaction starts with a write. If it started
	// with a SELECT, the UPDATE after it could fail at once with SQLITE_BUSY when another
	// connection wrote in between. A first write waits for the lock instead.
	if err := d.lock(ctx); err != nil {
		span.RecordError(err)
		return nil, err
	}
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
			RETURNING id, payload, retry_count, trace_context, created_at, scheduled_at, claim_token
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
			var createdAtMs, scheduledAtMs int64
			j.JobType = jobType
			if err = rows.Scan(&j.ID, &j.Payload, &j.RetryCount, &traceContextJSON, &createdAtMs, &scheduledAtMs, &j.ClaimToken); err != nil {
				return fmt.Errorf("failed to scan claimed job: %w", err) // Return error to rollback
			}
			j.CreatedAt = time.UnixMilli(createdAtMs)
			j.ScheduledAt = time.UnixMilli(scheduledAtMs)
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
		// RETURNING gives rows in no fixed order.
		slices.SortFunc(jobsToReturn, func(a, b job) int { return cmp.Compare(a.ID, b.ID) })
		return nil
	})
	d.unlock()
	if err != nil {
		span.RecordError(fmt.Errorf("transaction failed during getJobsForConsumer: %w", err))
		return nil, err // Return nil slice and the error
	}

	span.SetAttributes(attribute.Int("sqlq.jobs_fetched", len(jobsToReturn)))
	return jobsToReturn, nil
}

func (d *SQLiteDriver) extendClaim(ctx context.Context, j job, timeout time.Duration) (time.Time, error) {
	deadline := localClaimDeadline(timeout)
	if err := d.lock(ctx); err != nil {
		return deadline, err
	}
	defer d.unlock()
	result, err := d.db.ExecContext(ctx, `UPDATE jobs SET claim_expires_at = `+sqliteNow+` + ?
		WHERE id = ? AND claim_token = ? AND processed_at IS NULL AND claim_expires_at > `+sqliteNow,
		timeout.Milliseconds(), j.ID, j.ClaimToken)
	return deadline, checkClaimResult(result, err)
}

func (d *SQLiteDriver) releaseClaim(ctx context.Context, j job) error {
	// Do not take dbLock here. A dead-letter hook can hold it for a long time, and
	// shutdown's short release budget would run out waiting. SQLite's own lock is enough.
	result, err := d.db.ExecContext(ctx, `UPDATE jobs SET consumed_at = NULL, claim_token = NULL, claim_expires_at = NULL
		WHERE id = ? AND claim_token = ? AND processed_at IS NULL`, j.ID, j.ClaimToken)
	return checkClaimResult(result, err)
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

// isSQLiteBusy reports whether err is SQLITE_BUSY, including SQLITE_BUSY_SNAPSHOT. It goes by the
// result code where the driver gives one (see sqliteResultCode): a message can say anything, such as
// a trigger's RAISE, and a busy error lets markJobProcessed drop the handler's transaction. For other
// drivers it falls back on SQLite's own message.
func isSQLiteBusy(err error) bool {
	if code, ok := sqliteResultCode(err); ok {
		return code&0xff == sqliteBusy // The primary code, of an extended one too.
	}
	return strings.Contains(err.Error(), "database is locked")
}

// sqliteBusy is SQLITE_BUSY, the primary result code.
const sqliteBusy = 5

// sqliteResultCode returns the SQLite result code in err's chain, without depending on a driver:
// modernc.org/sqlite's errors have a Code method, and mattn/go-sqlite3's a Code field, which is
// read only from types of a package whose path names sqlite. It reports false if neither is found.
func sqliteResultCode(err error) (int, bool) {
	var coder interface{ Code() int }
	if errors.As(err, &coder) {
		return coder.Code(), true
	}
	for ; err != nil; err = errors.Unwrap(err) {
		v := reflect.ValueOf(err)
		if v.Kind() == reflect.Pointer {
			v = v.Elem()
		}
		if v.Kind() != reflect.Struct || !strings.Contains(v.Type().PkgPath(), "sqlite") {
			continue
		}
		if field := v.FieldByName("Code"); field.IsValid() && field.CanInt() {
			return int(field.Int()), true
		}
	}
	return 0, false
}

// wroteNothing reports whether a busy error on writing the main database proves that tx wrote nothing.
// It holds when main uses write-ahead logging, where a transaction that wrote main holds its write lock,
// and when the connection uses no other database, attached or temp, whose writes main's lock says nothing
// about. It is queried only on the busy path, through tx: the pool may have no other connection while
// tx holds one.
func wroteNothing(ctx context.Context, tx *sql.Tx) bool {
	var mode string
	if err := tx.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&mode); err != nil || !strings.EqualFold(mode, "wal") {
		return false
	}
	// The temp database is listed once the connection has used it.
	var others int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_database_list WHERE name <> 'main'`).Scan(&others); err != nil {
		return false
	}
	return others == 0
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
		// A handler that only read leaves tx a deferred read transaction, pinned to the snapshot of its
		// first read. Once another connection commits, it can't become a writer: SQLite fails this
		// update at once with SQLITE_BUSY_SNAPSHOT, which busy_timeout doesn't wait out.
		// In WAL mode, a transaction that has written main holds its write lock, so a busy error
		// here means the handler wrote nothing, unless it could write another database (see wroteNothing).
		// Rollback-journal mode gives no such guarantee.
		if isSQLiteBusy(err) && wroteNothing(ctx, tx) {
			err = fmt.Errorf("%w: %w", errMarkInFreshTx, err)
		}
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

	if err := d.lock(ctx); err != nil {
		span.RecordError(err)
		return err
	}
	defer d.unlock()

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
			retry_count = CASE WHEN retry_count < 2147483647 THEN retry_count + 1 ELSE retry_count END,
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

	err = tx.Commit()
	if err != nil {
		span.RecordError(fmt.Errorf("failed to commit transaction for reschedule: %w", err))
	}
	return err
}

// moveToDeadLetterQueue moves a failed job from the main jobs table to the dead_letter_queue table in SQLite, using the original job ID as the primary key.
// inTx runs while dbLock is held, so it must not call driver methods that take it.
func (d *SQLiteDriver) moveToDeadLetterQueue(ctx context.Context, jobID int64, claimToken string, reason string, inTx func(tx *sql.Tx) error) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.move_to_dlq", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.Int64("sqlq.original_job_id", jobID), // Use original_job_id in attribute
		attribute.String("sqlq.dlq_reason", reason),
	))
	defer span.End()

	if err := d.lock(ctx); err != nil {
		span.RecordError(err)
		return err
	}
	defer d.unlock()

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

	// Get current time in milliseconds with microsecond precision
	nowMs := time.Now().UnixMicro() / 1000

	// Copy the job in one statement, so the transaction starts with a write. A SELECT first would
	// pin a read snapshot, and once another process committed, the INSERT after it would fail at
	// once with SQLITE_BUSY_SNAPSHOT. dbLock only serializes this process. A first write waits for
	// the lock instead.
	res, err := tx.ExecContext(ctx, `
		INSERT INTO dead_letter_queue
		(original_job_id, job_type, payload, created_at, failed_at, retry_count, failure_reason)
		SELECT id, job_type, payload, created_at, ?, retry_count, ?
		FROM jobs WHERE id = ? AND claim_token = ? AND processed_at IS NULL
	`, nowMs, reason, jobID, claimToken)
	if err = checkClaimResult(res, err); err != nil {
		span.RecordError(err)
		return err // Rollback will happen
	}

	// Delete from jobs table
	res, err = tx.ExecContext(ctx, `DELETE FROM jobs WHERE id = ? AND claim_token = ? AND processed_at IS NULL`, jobID, claimToken)
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

// getReadyJobStats counts the jobs of jobType that a consumer can claim now, in SQLite.
// It does not take dbLock: it only reads, and the metrics callback must not delay queue writes.
func (d *SQLiteDriver) getReadyJobStats(ctx context.Context, jobType string) (readyJobStats, error) {
	var stats readyJobStats
	var lagMs int64
	err := d.db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(`+sqliteNow+` - MIN(scheduled_at), 0)
		FROM jobs
		WHERE job_type = ? AND processed_at IS NULL AND scheduled_at <= `+sqliteNow+`
		AND (claim_expires_at IS NULL OR claim_expires_at <= `+sqliteNow+`)
	`, jobType).Scan(&stats.count, &lagMs)
	stats.lag = time.Duration(lagMs) * time.Millisecond
	return stats, err
}

// getDeadLetterJobs retrieves jobs from the dead_letter_queue table in SQLite, optionally filtered by job type.
func (d *SQLiteDriver) getDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]DeadLetterJob, error) { //nolint:revive // Limit is fine
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.get_dlq_jobs", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.String("sqlq.job_type", jobType), // jobType might be empty
		attribute.Int("sqlq.limit", limit),
	))
	defer span.End()

	if err := d.lock(ctx); err != nil {
		span.RecordError(err)
		return nil, err
	}
	defer d.unlock()

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
	defer func() {
		_ = rows.Close() // rows.Err reports read errors; a Close error after that changes nothing.
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

	if err := d.lock(ctx); err != nil {
		span.RecordError(err)
		return err
	}
	defer d.unlock()

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

	// Get current time in milliseconds with microsecond precision
	nowMs := time.Now().UnixMicro() / 1000

	// reset retry count and scheduled_at = now. One statement, so the transaction starts with a
	// write, for the reason given in moveToDeadLetterQueue.
	res, err := tx.ExecContext(ctx, `
		INSERT INTO jobs (job_type, payload, retry_count, created_at, scheduled_at)
		SELECT job_type, payload, 0, ?, ?
		FROM dead_letter_queue WHERE original_job_id = ?
	`, nowMs, nowMs, originalJobID)
	if err != nil {
		span.RecordError(err)
		return err // Rollback will happen
	}
	if n, rowsErr := res.RowsAffected(); rowsErr != nil || n != 1 {
		if rowsErr == nil {
			rowsErr = ErrJobNotFound // Use ErrJobNotFound consistently
		}
		span.RecordError(rowsErr)
		return rowsErr
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
