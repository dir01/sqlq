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

func TestConsumerTransactionSQLite(t *testing.T) {
	t.Parallel()

	newPublishingSQLiteTestCase(t).TestConsumerTransaction(t.Context(), t)
}

// TestConsumerTransaction checks rollback before retries after errors, panics and
// timeouts, and that the final successful write commits with job completion.
func (tc *TestCase) TestConsumerTransaction(ctx context.Context, t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	_, err := tc.DB.ExecContext(ctx, "CREATE TABLE consumer_tx_attempts (attempt INTEGER NOT NULL)")
	require.NoError(t, err)

	var calls atomic.Int32
	err = tc.Q.Consume(ctx, "consumer_tx", func(ctx context.Context, tx *sql.Tx, _ []byte) error {
		attempt := calls.Add(1)
		if _, execErr := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO consumer_tx_attempts VALUES (%d)", attempt)); execErr != nil {
			return execErr
		}
		switch attempt {
		case 1:
			return errors.New("handler failed")
		case 2:
			panic("handler panicked")
		case 3:
			<-ctx.Done()
		}
		return nil
	}, sqlq.WithConsumerMaxRetries(3),
		sqlq.WithConsumerConcurrency(1),
		sqlq.WithConsumerPrefetchCount(1),
		sqlq.WithConsumerJobTimeout(200*time.Millisecond),
		sqlq.WithConsumerBackoffFunc(func(_ uint16) time.Duration { return 0 }),
	)
	require.NoError(t, err)
	require.NoError(t, tc.Q.Publish(ctx, "consumer_tx", "payload"))

	require.Eventually(t, func() bool {
		var processed int
		err := tc.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs WHERE job_type = 'consumer_tx' AND processed_at IS NOT NULL").Scan(&processed)
		return err == nil && processed == 1
	}, 5*time.Second, 10*time.Millisecond, "the successful attempt must commit job completion")

	var count, attempt int
	require.NoError(t, tc.DB.QueryRowContext(ctx, "SELECT COUNT(*), MAX(attempt) FROM consumer_tx_attempts").Scan(&count, &attempt))
	require.Equal(t, 1, count, "failed handler writes must be rolled back before retrying")
	require.Equal(t, 4, attempt)
	require.Equal(t, int32(4), calls.Load())
}
