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

// TestSubMillisecondBackoffRetryLimit checks that retries stay within maxRetries when the backoff
// is a duration whose Go string form ("1µs") PostgreSQL does not parse as an interval.
// If rescheduling fails, retry_count is never incremented and the job runs forever.
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

	// Settles once dead-lettered, or once the handler has clearly exceeded its attempt budget.
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

// TestPublishTxDelayFromPublishTime checks that a delayed job published inside a long-running
// transaction is delayed from the moment of publishing. NOW() is fixed at transaction start,
// so computing scheduled_at from it subtracts the transaction's age from the delay.
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

	// Fix the transaction's NOW(), then let it age.
	_, err = tx.ExecContext(ctx, "SELECT NOW()")
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, "SELECT pg_sleep($1)", txAge.Seconds())
	require.NoError(t, err)

	require.NoError(t, tc.Q.PublishTx(ctx, tx, jobType, TestPayload{Message: "delayed"}, sqlq.WithDelay(delay)))

	// Measured by the database's own clock, so host/container clock skew doesn't matter.
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

// TestMarkProcessedOutlastsTransientRowLock checks that a brief lock on a job's row doesn't fail a
// successful handler. A competing claim query can lock a row it then rejects: its FOR UPDATE rechecks
// the row's newer, claimed version and keeps the lock until it commits. With a short lock_timeout,
// marking the job processed could time out, charging a retry to a handler that succeeded.
func (tc *TestCase) TestMarkProcessedOutlastsTransientRowLock(ctx context.Context, t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	const (
		jobType     = "mark_processed_transient_row_lock"
		lockTimeout = 50 * time.Millisecond
		lockHeld    = 80 * time.Millisecond // Longer than one lock wait, shorter than two.
	)
	var calls atomic.Int32
	blockerErrs := make(chan error, 1)

	err := tc.Q.Consume(ctx, jobType, func(ctx context.Context, tx *sql.Tx, _ []byte) error {
		if calls.Add(1) > 1 {
			return nil
		}
		info, _ := sqlq.JobInfoFromContext(ctx)
		// Applies to the rest of this transaction, including marking the job processed.
		if _, execErr := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", lockTimeout.Milliseconds())); execErr != nil {
			return execErr
		}
		locked := make(chan struct{})
		// Outlives the handler, whose context ends when it returns.
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
