package sqlq_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/require"
)

func (tc *TestCase) TestDelayedJobExecution(ctx context.Context, t *testing.T) {
	t.Helper()

	ctx = GracefulContext(ctx, 10*time.Millisecond)

	delay := 500 * time.Millisecond
	jobProcessed := make(chan bool, 1)

	err := tc.Q.Consume(ctx, "delayed_job", "test_consumer", func(_ context.Context, _ *sql.Tx, _ []byte) error {
		jobProcessed <- true
		return nil
	})
	require.NoError(t, err, "Failed to start consumer for delayed_job")

	// Publish a job with a delay
	err = tc.Q.Publish(
		ctx,
		"delayed_job",
		TestPayload{Message: "This is a delayed job"},
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
		// Job was processed as expected
	case <-time.After(delay * 4):
		t.Fatal("Job was not processed after delay elapsed")
	}
}
