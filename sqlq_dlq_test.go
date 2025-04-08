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

func TestSQLiteDeadLetterQueue(t *testing.T) {
	dbConfig := setupSQLiteDB(t)
	defer dbConfig.Cleanup()

	runDeadLetterQueueTests(t, dbConfig)
}

func TestPostgresDeadLetterQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping PostgreSQL tests in short mode")
	}

	dbConfig := setupPostgresDB(t)
	defer dbConfig.Cleanup()

	runDeadLetterQueueTests(t, dbConfig)
}

func TestMySQLDeadLetterQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping MySQL tests in short mode")
	}

	dbConfig := setupMySQLDB(t)
	defer dbConfig.Cleanup()

	runDeadLetterQueueTests(t, dbConfig)
}

func runDeadLetterQueueTests(t *testing.T, dbConfig *TestDBConfig) {
	t.Helper()

	// Create a queue for testing
	queue, err := sqlq.New(dbConfig.DB, dbConfig.DBType)
	require.NoError(t, err, "Failed to create queue")

	// Start the queue
	queue.Run()
	defer queue.Shutdown()

	t.Run("Job moves to DLQ after max retries", func(t *testing.T) {
		// Create a context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		jobType := "dlq_test_job"
		consumerName := "dlq_test_consumer"
		maxRetries := 2 // Allow 2 retries (3 total attempts)

		// Create channels to track job processing
		jobAttempted := make(chan int, maxRetries+1)

		// Subscribe to the job type
		queue.Subscribe(ctx, jobType, consumerName, func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
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

			// Always fail the job to force it to the DLQ
			return errors.New("simulated failure to move job to DLQ")
		})

		// Create a unique payload
		testPayload := TestPayload{
			Message: "This job will go to DLQ",
			Count:   1000, // Unique identifier
		}

		// Publish the job with max retries
		err = queue.Publish(ctx, jobType, testPayload, sqlq.WithMaxRetries(maxRetries))
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

		// Now check the DLQ
		dlqJobs, err := queue.GetDeadLetterJobs(ctx, jobType, 10)
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
				require.Equal(t, maxRetries+1, dlqJob.RetryCount, "Retry count mismatch in DLQ")
				require.Equal(t, maxRetries, dlqJob.MaxRetries, "Max retries mismatch in DLQ")
				require.NotEmpty(t, dlqJob.FailureReason, "Failure reason should not be empty")
				break
			}
		}

		require.True(t, found, "Job not found in DLQ")
	})

	t.Run("Requeue job from DLQ", func(t *testing.T) {
		// Create a context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		jobType := "dlq_requeue_test"
		consumerName := "dlq_requeue_consumer"
		maxRetries := 1 // Allow 1 retry (2 total attempts)

		// Track job processing attempts and success
		jobAttempted := make(chan int, maxRetries+1)
		jobSucceeded := make(chan bool, 1)
		shouldSucceed := false

		// Subscribe to the job type
		queue.Subscribe(ctx, jobType, consumerName, func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
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
		})

		// Create a unique payload
		testPayload := TestPayload{
			Message: "This job will be requeued from DLQ",
			Count:   2000, // Unique identifier
		}

		// Publish the job with max retries
		err = queue.Publish(ctx, jobType, testPayload, sqlq.WithMaxRetries(maxRetries))
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
		dlqJobs, err := queue.GetDeadLetterJobs(ctx, jobType, 10)
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
		err = queue.RequeueDeadLetterJob(ctx, dlqJobID)
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
		dlqJobs, err = queue.GetDeadLetterJobs(ctx, jobType, 10)
		require.NoError(t, err, "Failed to get DLQ jobs")

		// Check if our job is still in the DLQ
		for _, dlqJob := range dlqJobs {
			if dlqJob.ID == dlqJobID {
				t.Fatal("Job still exists in DLQ after requeue")
			}
		}
	})

	t.Run("Get DLQ jobs by type", func(t *testing.T) {
		// Create a context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Create two different job types
		jobType1 := "dlq_filter_test_1"
		jobType2 := "dlq_filter_test_2"
		consumerName := "dlq_filter_consumer"
		maxRetries := 0 // No retries to speed up the test

		// Subscribe to both job types with a handler that always fails
		for _, jobType := range []string{jobType1, jobType2} {
			queue.Subscribe(ctx, jobType, consumerName, func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
				// Always fail to move to DLQ
				return errors.New("simulated failure for filter test")
			})
		}

		// Publish jobs of both types
		for i := 0; i < 3; i++ {
			// Type 1 jobs
			err = queue.Publish(ctx, jobType1, TestPayload{
				Message: "Type 1 job",
				Count:   3000 + i,
			}, sqlq.WithMaxRetries(maxRetries))
			require.NoError(t, err, "Failed to publish type 1 job")

			// Type 2 jobs
			err = queue.Publish(ctx, jobType2, TestPayload{
				Message: "Type 2 job",
				Count:   4000 + i,
			}, sqlq.WithMaxRetries(maxRetries))
			require.NoError(t, err, "Failed to publish type 2 job")
		}

		// Give time for jobs to be processed and moved to DLQ
		time.Sleep(5 * time.Second)

		// Get jobs of type 1
		dlqJobs1, err := queue.GetDeadLetterJobs(ctx, jobType1, 10)
		require.NoError(t, err, "Failed to get type 1 DLQ jobs")
		require.NotEmpty(t, dlqJobs1, "No type 1 jobs found in DLQ")

		// Verify all jobs are of type 1
		for _, job := range dlqJobs1 {
			require.Equal(t, jobType1, job.JobType, "Job type mismatch in filtered results")
		}

		// Get jobs of type 2
		dlqJobs2, err := queue.GetDeadLetterJobs(ctx, jobType2, 10)
		require.NoError(t, err, "Failed to get type 2 DLQ jobs")
		require.NotEmpty(t, dlqJobs2, "No type 2 jobs found in DLQ")

		// Verify all jobs are of type 2
		for _, job := range dlqJobs2 {
			require.Equal(t, jobType2, job.JobType, "Job type mismatch in filtered results")
		}

		// Get all jobs (empty job type)
		allDlqJobs, err := queue.GetDeadLetterJobs(ctx, "", 20)
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
	})

	t.Run("DLQ respects limit parameter", func(t *testing.T) {
		// Create a context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Get all DLQ jobs with different limits
		limit1 := 1
		dlqJobs1, err := queue.GetDeadLetterJobs(ctx, "", limit1)
		require.NoError(t, err, "Failed to get DLQ jobs with limit 1")
		require.LessOrEqual(t, len(dlqJobs1), limit1, "DLQ returned more jobs than the limit")

		// Only run the larger limit test if we have enough jobs
		if len(dlqJobs1) == limit1 {
			limit2 := 5
			dlqJobs2, err := queue.GetDeadLetterJobs(ctx, "", limit2)
			require.NoError(t, err, "Failed to get DLQ jobs with limit 5")
			require.LessOrEqual(t, len(dlqJobs2), limit2, "DLQ returned more jobs than the limit")
			require.GreaterOrEqual(t, len(dlqJobs2), len(dlqJobs1), "Second query with higher limit should return at least as many jobs")
		}
	})
}
