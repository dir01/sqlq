package sqlq_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/require"
)

// GracefulContext creates a context that cancels gracePeriod after cancellation of its parent
func GracefulContext(ctx context.Context, gracePeriod time.Duration) context.Context {
	newCtx, cancel := context.WithCancel(context.Background())

	go func() {
		select {
		case <-newCtx.Done():
			return
		case <-ctx.Done():
			<-time.After(gracePeriod)
			cancel()
		}

	}()

	return newCtx
}

func (tc *TestCase) DelayedJobExecution(t *testing.T) {
	t.Helper()
	ctx := GracefulContext(t.Context(), 10*time.Millisecond)

	delay := 500 * time.Millisecond
	jobProcessed := make(chan bool, 1)

	tc.Q.Consume(ctx, "delayed_job", "test_consumer", func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
		jobProcessed <- true
		return nil
	})

	// Publish a job with a delay
	err := tc.Q.Publish(
		ctx,
		"delayed_job",
		TestPayload{Message: "This is a delayed job", Count: 42},
		sqlq.WithDelay(delay),
	)
	require.NoError(t, err, "Failed to publish delayed job")

	// The job should not be processed immediately
	select {
	case <-jobProcessed:
		t.Fatal("Job was processed before delay elapsed")
	case <-time.After(delay / 2):
		// This is expected
	}

	// Wait for the job to be processed after the delay
	select {
	case <-jobProcessed:
		t.Logf("[%s] job processed", time.Now().String())
		// Job was processed as expected
	case <-time.After(delay * 4):
		t.Fatal("Job was not processed after delay elapsed")
	}
}
