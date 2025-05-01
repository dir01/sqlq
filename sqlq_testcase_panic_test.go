package sqlq_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/require"
)

func (tc *TestCase) TestPanic(ctx context.Context, t *testing.T) {
	t.Helper()

	jobType := "panic_job"
	panicMessage := "handler deliberately panicked"

	// Consume the queue with a handler that panics
	err := tc.Q.Consume(ctx, jobType,
		func(_ context.Context, _ *sql.Tx, _ []byte) error {
			panic(panicMessage)
		},
		sqlq.WithConsumerMaxRetries(0), // Move to DLQ immediately
		sqlq.WithConsumerPollInteval(10*time.Millisecond),
		sqlq.WithConsumerJobTimeout(1*time.Second),
	)
	require.NoError(t, err, "Failed to register consumer")

	// Publish a job that will trigger the panic
	testPayload := TestPayload{Message: "This job will panic"}
	err = tc.Q.Publish(ctx, jobType, testPayload)
	require.NoError(t, err, "Failed to publish job")

	// Wait a short time for the job to be processed, panic, and moved to DLQ
	time.Sleep(500 * time.Millisecond) // Adjust timing if needed based on system speed

	// Verify the job is in the Dead Letter Queue
	dlqJobs, err := tc.Q.GetDeadLetterJobs(ctx, jobType, 10)
	require.NoError(t, err, "Failed to get DLQ jobs")
	require.Len(t, dlqJobs, 1, "Expected exactly one job in DLQ")

	// Verify the failure reason indicates a panic
	require.Contains(t, dlqJobs[0].FailureReason, "panic recovered in handler", "DLQ reason should indicate panic recovery")
	require.Contains(t, dlqJobs[0].FailureReason, panicMessage, "DLQ reason should contain the panic message")
	require.Contains(t, dlqJobs[0].FailureReason, "Stack trace:", "DLQ reason should contain stack trace")

	// Verify payload integrity
	var receivedPayload TestPayload
	err = json.Unmarshal(dlqJobs[0].Payload, &receivedPayload)
	require.NoError(t, err, "Failed to unmarshal DLQ job payload")
	require.Equal(t, testPayload.Message, receivedPayload.Message, "DLQ job payload mismatch")
}
