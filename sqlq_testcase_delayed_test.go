package sqlq_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/require"
)

// gracefulContext wraps a cancellable context but delegates Value lookups to a parent.
type gracefulContext struct {
	context.Context                 // The cancellable context (derived from Background)
	parentCtx       context.Context // The original parent context for value lookups
}

// Value retrieves a value from the parent context.
func (gc *gracefulContext) Value(key any) any {
	return gc.parentCtx.Value(key)
}

// GracefulContext creates a context that inherits values from its parent
// and cancels gracePeriod after the parent context is cancelled.
func GracefulContext(parentCtx context.Context, gracePeriod time.Duration) context.Context {
	newCtx := context.WithoutCancel(parentCtx)
	newCtx, newCancel := context.WithCancel(newCtx)

	go func() {
		<-parentCtx.Done()
		t := time.NewTimer(gracePeriod)
		<-t.C
		newCancel()
	}()

	return newCtx
}

func (tc *TestCase) TestDelayedJobExecution(ctx context.Context, t *testing.T) {
	t.Helper()

	ctx = GracefulContext(ctx, 10*time.Millisecond)

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
