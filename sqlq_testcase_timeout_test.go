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

func (tc *TestCase) TestJobExecutionTimeout(ctx context.Context, t *testing.T) {
	t.Helper()

	// Use a shorter overall test timeout than the default
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	jobType := "timeout_job"
	consumerName := "timeout_consumer"
	jobTimeout := 100 * time.Millisecond     // Job should timeout after this duration
	processingTime := 300 * time.Millisecond // Handler takes longer than the timeout

	handlerStarted := make(chan bool, 1)
	handlerFinished := make(chan bool, 1) // Should not receive on this channel

	// Consume the queue with a job execution timeout
	err := tc.Q.Consume(ctx, jobType, consumerName,
		func(ctx context.Context, _ *sql.Tx, _ []byte) error { // Rename unused payloadBytes to _
			handlerStarted <- true
			select {
			case <-time.After(processingTime):
				// If we reach here, the timeout didn't work as expected
				handlerFinished <- true
				return nil
			case <-ctx.Done():
				// This is the expected path when the timeout is exceeded
				return ctx.Err() // Propagate the context cancellation error
			}
		},
		sqlq.WithConsumerMaxRetries(0),          // Ensure it goes to DLQ immediately on failure
		sqlq.WithConsumerJobTimeout(jobTimeout), // Enable the job timeout
	)
	require.NoError(t, err, "Failed to start consumer")

	// Publish a job
	testPayload := TestPayload{Message: "timeout test"}
	err = tc.Q.Publish(ctx, jobType, testPayload)
	require.NoError(t, err, "Failed to publish job")

	// Wait for the handler to start processing
	select {
	case <-handlerStarted:
		// Handler started, now wait to see if it finishes or times out
	case <-time.After(2 * time.Second): // Give it some time to start
		t.Fatal("Handler did not start processing the job")
	}

	// Assert that the handler does NOT finish successfully
	select {
	case <-handlerFinished:
		t.Fatal("Handler finished processing, but it should have timed out")
	case <-time.After(jobTimeout + processingTime): // Wait a bit longer than the expected processing time
		// This is the expected outcome - the handler didn't finish
	}

	// Check if the job ended up in the DLQ
	var dlqJobs []sqlq.DeadLetterJob
	require.Eventually(t, func() bool {
		var err error
		dlqJobs, err = tc.Q.GetDeadLetterJobs(ctx, jobType, 10)
		require.NoError(t, err, "Failed to get DLQ jobs")
		return len(dlqJobs) > 0
	}, 3*time.Second, 200*time.Millisecond, "Job did not appear in DLQ after timeout")

	// Verify the DLQ job details
	require.Len(t, dlqJobs, 1, "Expected exactly one job in DLQ")
	dlqJob := dlqJobs[0]
	require.Equal(t, jobType, dlqJob.JobType, "DLQ job type mismatch")
	require.Contains(t, dlqJob.FailureReason, context.DeadlineExceeded.Error(), "DLQ reason should indicate timeout")

	var receivedPayload TestPayload
	err = json.Unmarshal(dlqJob.Payload, &receivedPayload)
	require.NoError(t, err, "Failed to unmarshal DLQ job payload")
	require.Equal(t, testPayload, receivedPayload, "DLQ job payload mismatch")
}
