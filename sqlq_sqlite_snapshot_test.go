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

// TestSQLiteHandlerReadThenConcurrentWrite checks that a handler which only reads in its transaction
// and succeeds is not charged a retry when another connection commits before the job is marked
// processed. A deferred transaction's first read pins a WAL snapshot; once another connection
// commits, upgrading that transaction to a writer fails with SQLITE_BUSY_SNAPSHOT, which
// busy_timeout does not wait out. The queue's pool has a single connection, which the handler's
// transaction holds, so recovering from the conflict must not need another one.
func TestSQLiteHandlerReadThenConcurrentWrite(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	dsn := "file:" + filepath.Join(t.TempDir(), "snapshot.db") + "?_journal_mode=WAL&_busy_timeout=1000"
	db, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	// Another process sharing the database file.
	other, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)

	q, err := sqlq.New(
		db, sqlq.DBTypeSQLite,
		sqlq.WithDefaultBackoffFunc(func(_ uint16) time.Duration { return 0 }),
		sqlq.WithDefaultPollInterval(25*time.Millisecond),
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

		// The handler reads, which starts the transaction's snapshot.
		var n int
		if readErr := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs").Scan(&n); readErr != nil {
			handlerErrs.Add(1)
			return readErr
		}

		// Another connection commits a write, as another consumer claiming or a publisher
		// inserting a job would.
		if _, writeErr := other.ExecContext(ctx, "INSERT INTO other_writes VALUES (1)"); writeErr != nil {
			handlerErrs.Add(1)
			return writeErr
		}

		return nil
	}, sqlq.WithConsumerMaxRetries(3))
	require.NoError(t, err)

	require.NoError(t, q.Publish(ctx, jobType, TestPayload{Message: "succeeds"}))

	// Wait until the job settles: processed, or dead-lettered after every retry.
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

// TestSQLiteHandlerReadThenLongLock checks the same with a claim shorter than the wait for the
// write lock: another connection holds it past the claim's expiry. The mark keeps trying, since
// recording the success needs the lock no more than charging a retry would, and its claim token
// keeps it from marking a job another worker has claimed since.
func TestSQLiteHandlerReadThenLongLock(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	const (
		jobTimeout = 50 * time.Millisecond // The claim lasts 100ms.
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
		// The first poll takes the job. The next, which could claim it again once its claim expired,
		// comes well after the lock is released.
		sqlq.WithDefaultPollInterval(time.Second),
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
		// Another process takes the write lock and keeps it past the claim's expiry.
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

	// Wait until the job settles: processed, or charged a retry.
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
