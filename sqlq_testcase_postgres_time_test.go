package sqlq_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/require"
)

// TestSubMillisecondBackoffRetryLimit makes sure that retries stay in maxRetries if the Go text of the
// backoff ("1µs") is not a valid PostgreSQL interval. If the reschedule fails, retry_count does not
// increase, and the job runs forever.
func (tc *TestCase) TestSubMillisecondBackoffRetryLimit(ctx context.Context, t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	const jobType = "sub_ms_backoff_retry_limit"
	var calls atomic.Int32

	err := tc.Q.Consume(ctx, jobType, func(_ context.Context, _ *sql.Tx, _ []byte) error {
		calls.Add(1)
		return errors.New("simulated failure")
	},
		sqlq.WithConsumerMaxRetries(1), // 2 attempts in total
		sqlq.WithConsumerBackoffFunc(func(_ uint16) time.Duration { return time.Microsecond }),
	)
	require.NoError(t, err)

	require.NoError(t, tc.Q.Publish(ctx, jobType, TestPayload{Message: "always fails"}))

	// Stop when the job goes to the DLQ, or when the handler has too many attempts.
	require.Eventually(t, func() bool {
		if calls.Load() >= 5 {
			return true
		}
		dlq, dlqErr := tc.Q.GetDeadLetterJobs(ctx, jobType, 10)
		return dlqErr == nil && len(dlq) > 0
	}, 5*time.Second, 20*time.Millisecond)

	var retryCount int
	if scanErr := tc.DB.QueryRowContext(ctx, "SELECT retry_count FROM jobs WHERE job_type = $1", jobType).Scan(&retryCount); scanErr == nil {
		t.Logf("job still in jobs table with retry_count=%d after %d handler calls", retryCount, calls.Load())
	}

	dlq, err := tc.Q.GetDeadLetterJobs(ctx, jobType, 10)
	require.NoError(t, err)
	require.Len(t, dlq, 1, "job was not dead-lettered after exhausting its retries")
	require.Equal(t, int32(2), calls.Load(), "handler ran more than maxRetries+1 times")
}

// TestPublishTxDelayFromPublishTime makes sure that the delay of a job starts when it is published,
// also in a long transaction. NOW() is the start time of the transaction. If scheduled_at
// comes from NOW(), the delay is shorter by the age of the transaction.
func (tc *TestCase) TestPublishTxDelayFromPublishTime(ctx context.Context, t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	const (
		jobType = "publish_tx_delay_from_publish_time"
		txAge   = 400 * time.Millisecond
		delay   = 200 * time.Millisecond
	)

	tx, err := tc.DB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	// Set NOW() of the transaction. Then wait, to make the transaction older.
	_, err = tx.ExecContext(ctx, "SELECT NOW()")
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "SELECT pg_sleep($1)", txAge.Seconds())
	require.NoError(t, err)

	require.NoError(t, tc.Q.PublishTx(ctx, tx, jobType, TestPayload{Message: "delayed"}, sqlq.WithDelay(delay)))

	// Use the clock of the database. Thus, a clock difference between host and container has no effect.
	var remainingMs float64
	err = tx.QueryRowContext(ctx,
		"SELECT EXTRACT(EPOCH FROM (scheduled_at - clock_timestamp())) * 1000 FROM jobs WHERE job_type = $1",
		jobType,
	).Scan(&remainingMs)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	t.Logf("job is due in %.0fms right after publishing with a %s delay in a %s-old transaction", remainingMs, delay, txAge)
	require.Greater(t, remainingMs, float64(delay.Milliseconds())/2,
		"delay was measured from transaction start instead of publish time")
}

// TestMarkProcessedOutlastsTransientRowLock makes sure that a short lock on the row of a job does not
// make a successful handler fail. A different claim query can lock a row, but not claim it. Its FOR UPDATE
// examines the new claimed version of the row again, and keeps the lock until it commits. With a
// short lock_timeout, the mark can time out, and the queue counts a retry for a successful handler.
func (tc *TestCase) TestMarkProcessedOutlastsTransientRowLock(ctx context.Context, t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	const (
		jobType     = "mark_processed_transient_row_lock"
		lockTimeout = 50 * time.Millisecond
		lockHeld    = 80 * time.Millisecond // Longer than one lock wait, and shorter than two.
	)
	var calls atomic.Int32
	blockerErrs := make(chan error, 1)

	err := tc.Q.Consume(ctx, jobType, func(ctx context.Context, tx *sql.Tx, _ []byte) error {
		if calls.Add(1) > 1 {
			return nil
		}
		info, _ := sqlq.JobInfoFromContext(ctx)
		// This applies to the remaining part of this transaction, also to the mark.
		if _, execErr := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", lockTimeout.Milliseconds())); execErr != nil {
			return execErr
		}
		locked := make(chan struct{})
		// This context continues after the handler. The handler context ends when the handler returns.
		blockerCtx := context.WithoutCancel(ctx)
		go func() {
			blockerErrs <- func() error {
				blocker, beginErr := tc.DB.BeginTx(blockerCtx, nil)
				if beginErr != nil {
					close(locked)
					return beginErr
				}
				defer func() { _ = blocker.Rollback() }()
				_, lockErr := blocker.ExecContext(blockerCtx, "SELECT id FROM jobs WHERE id = $1 FOR UPDATE", info.ID)
				close(locked)
				if lockErr != nil {
					return lockErr
				}
				time.Sleep(lockHeld)
				return blocker.Commit()
			}()
		}()
		<-locked
		return nil
	}, sqlq.WithConsumerMaxRetries(3))
	require.NoError(t, err)

	require.NoError(t, tc.Q.Publish(ctx, jobType, TestPayload{Message: "succeeds"}))

	var retryCount int
	var lastError sql.NullString
	require.Eventually(t, func() bool {
		var processed bool
		scanErr := tc.DB.QueryRowContext(ctx,
			"SELECT processed_at IS NOT NULL, retry_count, last_error FROM jobs WHERE job_type = $1", jobType,
		).Scan(&processed, &retryCount, &lastError)
		return scanErr == nil && processed
	}, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, <-blockerErrs)

	require.Equal(t, int32(1), calls.Load(), "a successful handler was retried")
	require.Zero(t, retryCount, "a successful handler was charged a retry: %s", lastError.String)
}
