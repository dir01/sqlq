package sqlq_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLiteHandlerReadThenConcurrentWrite makes sure that the queue does not count a retry for a
// successful handler that only reads, if a different connection commits before the mark.
// The first read of a deferred transaction sets a WAL snapshot. After a different connection
// commits, the transaction cannot become a writer. It fails with SQLITE_BUSY_SNAPSHOT, and
// busy_timeout does not wait. The pool of the queue has one connection, and the handler
// transaction holds it. Thus, the recovery must not need a second connection.
func TestSQLiteHandlerReadThenConcurrentWrite(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	dsn := "file:" + filepath.Join(t.TempDir(), "snapshot.db") + "?_journal_mode=WAL&_busy_timeout=1000"
	db, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	// A different process that uses the same database file.
	other, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)

	q, err := sqlq.New(
		db, sqlq.DBTypeSQLite,
		sqlq.WithDefaultBackoffFunc(func(_ uint16) time.Duration { return 0 }),
		sqlq.WithDefaultPollFunc(pollEvery(25*time.Millisecond)),
	)
	require.NoError(t, err)
	require.NoError(t, q.Run())
	t.Cleanup(func() {
		q.Shutdown()
		assert.NoError(t, db.Close())
		assert.NoError(t, other.Close())
	})
	_, err = other.ExecContext(ctx, "CREATE TABLE other_writes (n INTEGER)")
	require.NoError(t, err)

	const jobType = "snapshot_read_then_write"
	var calls atomic.Int32
	var handlerErrs atomic.Int32

	err = q.Consume(ctx, jobType, func(ctx context.Context, tx *sql.Tx, _ []byte) error {
		calls.Add(1)

		// The handler reads. This sets the snapshot of the transaction.
		var n int
		if readErr := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs").Scan(&n); readErr != nil {
			handlerErrs.Add(1)
			return readErr
		}

		// A different connection commits a write, as a different consumer with a claim, or a
		// publisher with a new job, does.
		if _, writeErr := other.ExecContext(ctx, "INSERT INTO other_writes VALUES (1)"); writeErr != nil {
			handlerErrs.Add(1)
			return writeErr
		}

		return nil
	}, sqlq.WithConsumerMaxRetries(3))
	require.NoError(t, err)

	require.NoError(t, q.Publish(ctx, jobType, TestPayload{Message: "succeeds"}))

	// Wait until the job is processed, or until it goes to the DLQ after all retries.
	var processed bool
	require.Eventually(t, func() bool {
		var processedAt sql.NullInt64
		if scanErr := db.QueryRowContext(ctx, "SELECT processed_at FROM jobs WHERE job_type = ?", jobType).Scan(&processedAt); scanErr == nil {
			processed = processedAt.Valid
			return processed
		}
		dlq, dlqErr := q.GetDeadLetterJobs(ctx, jobType, 10)
		return dlqErr == nil && len(dlq) > 0
	}, 5*time.Second, 20*time.Millisecond)

	dlq, err := q.GetDeadLetterJobs(ctx, jobType, 10)
	require.NoError(t, err)

	require.Zero(t, handlerErrs.Load(), "handler itself never failed")
	for _, j := range dlq {
		t.Logf("dead-lettered after %d handler calls that all returned nil: %s", calls.Load(), j.FailureReason)
	}
	require.Empty(t, dlq, "a job whose handler succeeded was dead-lettered")
	require.True(t, processed, "job was not marked processed")
	require.Equal(t, int32(1), calls.Load(), "a successful handler was retried")
}

// TestSQLiteHandlerReadThenLongLock examines the same, with a claim that is shorter than the wait
// for the write lock. A different connection holds the lock after the claim expires. The mark tries
// again and again, because a retry needs the same lock. The claim token prevents a mark on a job
// that a different worker has now.
func TestSQLiteHandlerReadThenLongLock(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	const (
		jobTimeout = 50 * time.Millisecond // The claim is 100ms.
		lockHeld   = 300 * time.Millisecond
	)
	dsn := "file:" + filepath.Join(t.TempDir(), "lock.db") + "?_journal_mode=WAL&_busy_timeout=100"
	db, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	other, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)

	q, err := sqlq.New(
		db, sqlq.DBTypeSQLite,
		sqlq.WithDefaultBackoffFunc(func(_ uint16) time.Duration { return 0 }),
		// The first poll takes the job. The next poll can claim it again after the claim expires.
		// It occurs long after the release of the lock.
		sqlq.WithDefaultPollFunc(pollEvery(time.Second)),
		sqlq.WithDefaultJobTimeout(jobTimeout),
	)
	require.NoError(t, err)
	require.NoError(t, q.Run())
	t.Cleanup(func() {
		q.Shutdown()
		assert.NoError(t, db.Close())
		assert.NoError(t, other.Close())
	})
	_, err = other.ExecContext(ctx, "CREATE TABLE other_writes (n INTEGER)")
	require.NoError(t, err)

	const jobType = "snapshot_read_then_lock"
	require.NoError(t, q.Publish(ctx, jobType, TestPayload{Message: "succeeds despite the lock"}))
	var calls atomic.Int32
	released := make(chan struct{})
	err = q.Consume(ctx, jobType, func(ctx context.Context, tx *sql.Tx, _ []byte) error {
		if calls.Add(1) > 1 {
			return nil
		}
		var n int
		if readErr := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs").Scan(&n); readErr != nil {
			return readErr
		}
		// A different process takes the write lock, and keeps it until after the claim expires.
		conn, connErr := other.Conn(context.Background())
		if connErr != nil {
			return connErr
		}
		if _, lockErr := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); lockErr != nil {
			return lockErr
		}
		if _, writeErr := conn.ExecContext(ctx, "INSERT INTO other_writes VALUES (1)"); writeErr != nil {
			return writeErr
		}
		time.AfterFunc(lockHeld, func() {
			defer close(released)
			_, _ = conn.ExecContext(context.Background(), "COMMIT")
			_ = conn.Close()
		})
		return nil
	}, sqlq.WithConsumerMaxRetries(3))
	require.NoError(t, err)

	// Wait until the job is processed, or until the queue counts a retry.
	var processedAt sql.NullInt64
	var retryCount int
	var lastError sql.NullString
	require.Eventually(t, func() bool {
		scanErr := db.QueryRowContext(ctx, "SELECT processed_at, retry_count, last_error FROM jobs WHERE job_type = ?", jobType).
			Scan(&processedAt, &retryCount, &lastError)
		return scanErr == nil && (processedAt.Valid || retryCount > 0)
	}, 5*time.Second, 20*time.Millisecond)
	<-released
	assert.Zero(t, retryCount, "a successful handler was charged a retry: %s", lastError.String)
	assert.True(t, processedAt.Valid, "job was not marked processed")
	assert.Equal(t, int32(1), calls.Load(), "a successful handler was retried")
}
