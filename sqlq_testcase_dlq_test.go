package sqlq_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/require"
)

func (tc *TestCase) DLQBasic(ctx context.Context, t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	jobType := "job_moves_to_dlq_after_max_retries"
	consumerName := "dlq_test_consumer"
	maxRetries := 2 // Allow 2 retries (3 total attempts)

	jobAttempted := make(chan struct{}, maxRetries+1)

	tc.Q.Consume(ctx, jobType, consumerName, func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
		select {
		case jobAttempted <- struct{}{}:
			// Successfully recorded attempt
		case <-ctx.Done():
			return ctx.Err()
		}

		// Always fail the job to force it to the DLQ
		return errors.New("simulated failure to move job to DLQ")
	}, sqlq.WithMaxRetries(maxRetries))

	testPayload := TestPayload{Message: "test_dlq_job_payload"}
	// Publish the job with max retries
	err := tc.Q.Publish(ctx, jobType, testPayload)
	require.NoError(t, err, "Failed to publish job")

	// Wait for all retry attempts
	for i := 0; i <= maxRetries; i++ {
		select {
		case <-jobAttempted:
		// all good, attempt happened
		case <-time.After(10 * time.Second):
			t.Fatalf("Timed out waiting for attempt %d", i+1)
		case <-ctx.Done():
			t.Fatal("Context timeout while waiting for job attempts")
		}
	}

	// Give some time for the job to be moved to DLQ
	time.Sleep(1 * time.Second)

	// Now check the DLQ
	dlqJobs, err := tc.Q.GetDeadLetterJobs(ctx, jobType, 10)
	require.NoError(t, err, "Failed to get DLQ jobs")
	require.NotEmpty(t, dlqJobs, "No jobs found in DLQ")

	// Find our job in the DLQ
	var found bool
	for _, dlqJob := range dlqJobs {
		var payload TestPayload
		err = json.Unmarshal(dlqJob.Payload, &payload)
		require.NoError(t, err, "Failed to unmarshal DLQ job payload")

		if payload.Count == testPayload.Count {
			found = true
			require.Equal(t, testPayload.Message, payload.Message, "Message mismatch in DLQ")
			require.Equal(t, maxRetries, dlqJob.RetryCount, "Retry count mismatch in DLQ")
			require.NotEmpty(t, dlqJob.FailureReason, "Failure reason should not be empty")
			break
		}
	}

	require.True(t, found, "Job not found in DLQ")
}

func (tc *TestCase) DLQReque(ctx context.Context, t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	jobType := "dlq_requeue_test"
	consumerName := "dlq_requeue_consumer"
	maxRetries := 1 // Allow 1 retry (2 total attempts)

	// Track job processing attempts and success
	jobAttempted := make(chan int, maxRetries+1)
	jobSucceeded := make(chan bool, 1)
	shouldSucceed := false

	// Consume job type
	tc.Q.Consume(ctx, jobType, consumerName, func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
		var payload TestPayload
		if err := json.Unmarshal(payloadBytes, &payload); err != nil {
			return err
		}

		// Record this attempt
		select {
		case jobAttempted <- payload.Count:
			// Successfully recorded attempt
		case <-ctx.Done():
			return ctx.Err()
		}

		if shouldSucceed {
			// Signal that the job succeeded after requeue
			select {
			case jobSucceeded <- true:
				// Successfully signaled
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}

		// Fail the job to force it to the DLQ
		return errors.New("simulated failure for requeue test")
	}, sqlq.WithMaxRetries(maxRetries))

	// Create a unique payload
	testPayload := TestPayload{
		Message: "This job will be requeued from DLQ",
		Count:   2000, // Unique identifier
	}

	// Publish the job with max retries
	err := tc.Q.Publish(ctx, jobType, testPayload)
	require.NoError(t, err, "Failed to publish job")

	// Wait for all retry attempts
	for i := 0; i <= maxRetries; i++ {
		select {
		case count := <-jobAttempted:
			require.Equal(t, testPayload.Count, count, "Payload mismatch in attempt")
		case <-time.After(10 * time.Second):
			t.Fatalf("Timed out waiting for attempt %d", i+1)
		case <-ctx.Done():
			t.Fatal("Context timeout while waiting for job attempts")
		}
	}

	// Give some time for the job to be moved to DLQ
	time.Sleep(2 * time.Second)

	// Get the job from DLQ
	dlqJobs, err := tc.Q.GetDeadLetterJobs(ctx, jobType, 10)
	require.NoError(t, err, "Failed to get DLQ jobs")
	require.NotEmpty(t, dlqJobs, "No jobs found in DLQ")

	// Find our job in the DLQ
	var dlqJobID int64
	for _, dlqJob := range dlqJobs {
		var payload TestPayload
		err = json.Unmarshal(dlqJob.Payload, &payload)
		require.NoError(t, err, "Failed to unmarshal DLQ job payload")

		if payload.Count == testPayload.Count {
			dlqJobID = dlqJob.ID
			break
		}
	}

	require.NotZero(t, dlqJobID, "Job not found in DLQ")

	// Now we'll allow the job to succeed when requeued
	shouldSucceed = true

	// Requeue the job from DLQ
	err = tc.Q.RequeueDeadLetterJob(ctx, dlqJobID)
	require.NoError(t, err, "Failed to requeue job from DLQ")

	// Wait for the requeued job to be processed successfully
	select {
	case <-jobSucceeded:
		// Job was successfully processed after requeue
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out waiting for requeued job to succeed")
	case <-ctx.Done():
		t.Fatal("Context timeout while waiting for requeued job")
	}

	// Verify the job is no longer in the DLQ
	dlqJobs, err = tc.Q.GetDeadLetterJobs(ctx, jobType, 10)
	require.NoError(t, err, "Failed to get DLQ jobs")

	// Check if our job is still in the DLQ
	for _, dlqJob := range dlqJobs {
		if dlqJob.ID == dlqJobID {
			t.Fatal("Job still exists in DLQ after requeue")
		}
	}
}

func (tc *TestCase) DLQGet(ctx context.Context, t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Create two different job types
	jobType1 := "dlq_filter_test_1"
	jobType2 := "dlq_filter_test_2"
	consumerName := "dlq_filter_consumer"
	maxRetries := 0 // No retries to speed up the test

	// Consume both job types with a handler that always fails
	for _, jobType := range []string{jobType1, jobType2} {
		tc.Q.Consume(ctx, jobType, consumerName, func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
			// Always fail to move to DLQ
			return errors.New("simulated failure for filter test")
		}, sqlq.WithMaxRetries(maxRetries))
	}

	// Publish jobs of both types
	for i := 0; i < 3; i++ {
		err := tc.Q.Publish(ctx, jobType1, TestPayload{
			Message: "Type 1 job",
			Count:   3000 + i,
		})
		require.NoError(t, err, "Failed to publish type 1 job")

		err = tc.Q.Publish(ctx, jobType2, TestPayload{
			Message: "Type 2 job",
			Count:   4000 + i,
		})
		require.NoError(t, err, "Failed to publish type 2 job")
	}

	var dlqJobs1 []sqlq.DeadLetterJob
	var dlqJobs2 []sqlq.DeadLetterJob

	require.Eventually(t, func() bool {
		var err error
		if dlqJobs1, err = tc.Q.GetDeadLetterJobs(ctx, jobType1, 10); err != nil || len(dlqJobs1) == 0 {
			return false
		}
		if dlqJobs2, err = tc.Q.GetDeadLetterJobs(ctx, jobType2, 10); err != nil || len(dlqJobs2) == 0 {
			return false
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)

	// Verify all jobs are of type 1
	for _, job := range dlqJobs1 {
		require.Equal(t, jobType1, job.JobType, "Job type mismatch in filtered results")
	}

	// Verify all jobs are of type 2
	for _, job := range dlqJobs2 {
		require.Equal(t, jobType2, job.JobType, "Job type mismatch in filtered results")
	}

	// Get all jobs (empty job type)
	allDlqJobs, err := tc.Q.GetDeadLetterJobs(ctx, "", 20)
	require.NoError(t, err, "Failed to get all DLQ jobs")
	require.NotEmpty(t, allDlqJobs, "No jobs found in DLQ")

	// Verify we have both types in the results
	foundType1 := false
	foundType2 := false
	for _, job := range allDlqJobs {
		if job.JobType == jobType1 {
			foundType1 = true
		}
		if job.JobType == jobType2 {
			foundType2 = true
		}
	}
	require.True(t, foundType1, "Type 1 jobs not found in unfiltered results")
	require.True(t, foundType2, "Type 2 jobs not found in unfiltered results")
}

func (tc *TestCase) DLQGetLimit(ctx context.Context, t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Get all DLQ jobs with different limits
	limit1 := 1
	dlqJobs1, err := tc.Q.GetDeadLetterJobs(ctx, "", limit1)
	require.NoError(t, err, "Failed to get DLQ jobs with limit 1")
	require.LessOrEqual(t, len(dlqJobs1), limit1, "DLQ returned more jobs than the limit")

	// Only run the larger limit test if we have enough jobs
	if len(dlqJobs1) == limit1 {
		limit2 := 5
		dlqJobs2, err := tc.Q.GetDeadLetterJobs(ctx, "", limit2)
		require.NoError(t, err, "Failed to get DLQ jobs with limit 5")
		require.LessOrEqual(t, len(dlqJobs2), limit2, "DLQ returned more jobs than the limit")
		require.GreaterOrEqual(t, len(dlqJobs2), len(dlqJobs1), "Second query with higher limit should return at least as many jobs")
	}
}
