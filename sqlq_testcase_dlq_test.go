package sqlq_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/require"
)

func (tc *TestCase) TestDLQBasic(ctx context.Context, t *testing.T) {
	t.Helper()

	// Increase overall test timeout
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	jobType := "job_moves_to_dlq_after_max_retries"
	var maxRetries int32
	var attempts atomic.Int32

	err := tc.Q.Consume(ctx, jobType, func(_ context.Context, _ *sql.Tx, _ []byte) error {
		attempts.Add(1)
		return errors.New("simulated failure to move job to DLQ")
	}, sqlq.WithConsumerMaxRetries(maxRetries))
	require.NoError(t, err, "Failed to start consumer for DLQ test")

	testPayload := TestPayload{Message: "test_dlq_job_payload"}
	// Publish the job with max retries
	err = tc.Q.Publish(ctx, jobType, testPayload)
	require.NoError(t, err, "Failed to publish job")

	// Increase timeout for checking attempts
	require.Eventually(t, func() bool {
		return attempts.Load() == maxRetries+1 // Remove unnecessary conversion
	}, 5*time.Second, 10*time.Millisecond)

	var dlqJobs []sqlq.DeadLetterJob

	require.Eventually(t, func() bool {
		var err error

		dlqJobs, err = tc.Q.GetDeadLetterJobs(ctx, jobType, 10)
		require.NoError(t, err, "Failed to get DLQ jobs")

		return len(dlqJobs) > 0
	// Increase timeout for checking DLQ
	}, 5*time.Second, 10*time.Millisecond)

	require.Equal(t, 1, len(dlqJobs))

	j := dlqJobs[0]
	require.Equal(t, uint16(maxRetries), j.RetryCount)
	require.NotZero(t, j.OriginalID, "OriginalID should not be zero in DLQ job")
}

func (tc *TestCase) TestDLQReque(ctx context.Context, t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	jobType := "dlq_requeue_test"
	var maxRetries int32

	jobFailed := make(chan struct{})
	jobSucceeded := make(chan struct{})
	shouldSucceed := false

	// Consume job type
	err := tc.Q.Consume(ctx, jobType, func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
		var payload TestPayload
		if err := json.Unmarshal(payloadBytes, &payload); err != nil {
			return err
		}

		if !shouldSucceed {
			select {
			case jobFailed <- struct{}{}:
			// Successfully recorded first attempt
			// after this, job should go to DLQ
			case <-ctx.Done():
				return ctx.Err()
			}

			return errors.New("simulated failure for requeue test")
		}

		select {
		case jobSucceeded <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}

		return nil
	}, sqlq.WithConsumerMaxRetries(maxRetries))
	require.NoError(t, err, "Failed to start consumer for DLQ requeue test")

	testPayload := TestPayload{Message: "This job will be requeued from DLQ"}

	err = tc.Q.Publish(ctx, jobType, testPayload)
	require.NoError(t, err, "Failed to publish job")

	select {
	case <-jobFailed:
	// consumer failed, after this job should go to DLQ
	case <-time.After(2 * time.Second):
		t.Fatalf("Timed out waiting for job failure")
	case <-ctx.Done():
		t.Fatal("Context timeout while waiting for job attempts")
	}

	var dlqJobs []sqlq.DeadLetterJob
	require.Eventually(t, func() bool {
		dlqJobs, err = tc.Q.GetDeadLetterJobs(ctx, jobType, 10)
		require.NoError(t, err)
		return len(dlqJobs) != 0
	}, 1*time.Second, 10*time.Millisecond)

	// Find our job in the DLQ
	var originalJobID int64 // We need the original job ID to requeue
	require.NotEmpty(t, dlqJobs, "DLQ should contain the failed job")
	for _, dlqJob := range dlqJobs {
		var payload TestPayload
		err = json.Unmarshal(dlqJob.Payload, &payload)
		require.NoError(t, err, "Failed to unmarshal DLQ job payload")

		// Assuming the payload message identifies our job for simplicity in this test
		if payload.Message == testPayload.Message {
			originalJobID = dlqJob.OriginalID // Get the OriginalID
			break
		}
	}

	require.NotZero(t, originalJobID, "Job not found in DLQ by payload message")

	// Now we'll allow the job to succeed when requeued
	shouldSucceed = true

	// Requeue the job from DLQ using its original ID
	err = tc.Q.RequeueDeadLetterJob(ctx, originalJobID)
	require.NoError(t, err, "Failed to requeue job from DLQ")

	// Wait for the requeued job to be processed successfully using Eventually
	require.Eventually(t, func() bool {
		select {
		case <-jobSucceeded:
			return true // Job was successfully processed
		default:
			return false // Not yet processed
		}
	}, 5*time.Second, 50*time.Millisecond, "Timed out waiting for requeued job to succeed") // Adjust timeout/tick if needed

	// Verify the job is no longer in the DLQ
	dlqJobs, err = tc.Q.GetDeadLetterJobs(ctx, jobType, 10)
	require.NoError(t, err, "Failed to get DLQ jobs")

	// Check if our job (identified by original ID) is still in the DLQ
	for _, dlqJob := range dlqJobs {
		if dlqJob.OriginalID == originalJobID {
			t.Fatalf("Job with original ID %d still exists in DLQ after requeue", originalJobID)
		}
	}
}

func (tc *TestCase) TestDLQGet(ctx context.Context, t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Create two different job types
	jobType1 := "dlq_filter_test_1"
	jobType2 := "dlq_filter_test_2"
	var maxRetries int32 // No retries to speed up the test

	// Consume both job types with a handler that always fails
	for _, jobType := range []string{jobType1, jobType2} {
		err := tc.Q.Consume(ctx, jobType, func(_ context.Context, _ *sql.Tx, _ []byte) error {
			// Always fail to move to DLQ
			return errors.New("simulated failure for filter test")
		}, sqlq.WithConsumerMaxRetries(maxRetries))
		require.NoError(t, err, "Failed to start consumer for DLQ filter test (job type: %s)", jobType)
	}

	// Publish jobs of both types
	for range 3 { // Use integer range loop
		err := tc.Q.Publish(ctx, jobType1, TestPayload{
			Message: "Type 1 job",
		})
		require.NoError(t, err, "Failed to publish type 1 job")

		err = tc.Q.Publish(ctx, jobType2, TestPayload{
			Message: "Type 2 job",
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

func (tc *TestCase) TestDLQGetLimit(ctx context.Context, t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Get all DLQ jobs with different limits
	limit1 := 1
	dlqJobs1, err := tc.Q.GetDeadLetterJobs(ctx, "", limit1)
	require.NoError(t, err, "Failed to get DLQ jobs with limit 1")
	require.LessOrEqual(t, len(dlqJobs1), limit1, "DLQ returned more jobs than the limit")

	// Only run the larger limit test if we have enough jobs and the first query returned exactly the limit
	if len(dlqJobs1) == limit1 {
		// Fetch all jobs to see if there are actually more than limit1
		allJobs, errAll := tc.Q.GetDeadLetterJobs(ctx, "", 100) // Fetch a large number
		require.NoError(t, errAll, "Failed to get all DLQ jobs for comparison")

		if len(allJobs) > limit1 { // Only proceed if there are more jobs to fetch
			limit2 := 5
			dlqJobs2, err := tc.Q.GetDeadLetterJobs(ctx, "", limit2)
			require.NoError(t, err, "Failed to get DLQ jobs with limit 5")
			require.LessOrEqual(t, len(dlqJobs2), limit2, "DLQ returned more jobs than the limit")
			// Ensure the second query returned more or equal jobs only if enough jobs exist overall
			if len(allJobs) >= limit2 {
				require.GreaterOrEqual(t, len(dlqJobs2), len(dlqJobs1), "Second query with higher limit should return at least as many jobs as the first")
				require.Greater(t, len(dlqJobs2), len(dlqJobs1), "Second query with higher limit should return more jobs than the first when available")
			} else {
				require.Equal(t, len(allJobs), len(dlqJobs2), "Second query should return all available jobs if limit is higher than total")
			}
		}
	}
}
