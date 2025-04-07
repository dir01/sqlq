package sqlqueue_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dir01/sqlqueue"
	"github.com/stretchr/testify/require"
)

func TestDelayedJobs(t *testing.T) {
	dbConfig := setupSQLiteDB(t)
	defer dbConfig.Cleanup()

	// Create a queue with a short poll interval for testing
	queue, err := sqlqueue.NewSQLQueue(dbConfig.DB, dbConfig.DBType, 100*time.Millisecond)
	require.NoError(t, err, "Failed to create queue")

	// Start the queue
	queue.Run()
	defer queue.Shutdown()

	t.Run("Delayed job execution", func(t *testing.T) {
		// Create channels to track job processing
		jobProcessed := make(chan bool, 1)
		var receivedPayload TestPayload

		// Subscribe to jobs
		queue.Subscribe(t.Context(), "delayed_job", "test_consumer", func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
			var payload TestPayload
			if err := json.Unmarshal(payloadBytes, &payload); err != nil {
				t.Errorf("Failed to unmarshal payload: %v", err)
				return err
			}

			receivedPayload = payload
			jobProcessed <- true
			return nil
		})

		// Create a payload
		payload := TestPayload{
			Message: "This is a delayed job",
			Count:   42,
		}

		// Publish a job with a delay
		delay := 500 * time.Millisecond
		err := queue.Publish(t.Context(), "delayed_job", payload, sqlqueue.WithDelay(delay))
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
			require.Equal(t, payload.Message, receivedPayload.Message, "Payload message mismatch")
			require.Equal(t, payload.Count, receivedPayload.Count, "Payload count mismatch")
		case <-time.After(delay * 2):
			t.Fatal("Job was not processed after delay elapsed")
		}
	})
}

func TestJobRetries(t *testing.T) {
	dbConfig := setupSQLiteDB(t)
	defer dbConfig.Cleanup()

	// Create a queue with a short poll interval for testing
	queue, err := sqlqueue.NewSQLQueue(dbConfig.DB, dbConfig.DBType, 100*time.Millisecond)
	require.NoError(t, err, "Failed to create queue")

	// Start the queue
	queue.Run()
	defer queue.Shutdown()

	t.Run("Job retry mechanism", func(t *testing.T) {
		// Create channels to track job processing attempts
		jobAttempts := make(chan int, 5) // Buffer for multiple attempts

		// Use atomic counter to avoid race conditions
		var attemptCount int32 = 0
		maxRetries := 2 // We'll allow 2 retries (3 total attempts)

		// Subscribe to jobs
		queue.Subscribe(t.Context(), "retry_job", "retry_consumer", func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
			var payload TestPayload
			if err := json.Unmarshal(payloadBytes, &payload); err != nil {
				t.Errorf("Failed to unmarshal payload: %v", err)
				return err
			}

			count := atomic.AddInt32(&attemptCount, 1)
			jobAttempts <- int(count)

			// Fail the job for the first two attempts
			if count <= int32(maxRetries) {
				return errors.New("simulated failure")
			}

			// Succeed on the third attempt
			return nil
		})

		// Create a payload
		payload := TestPayload{
			Message: "This job will be retried",
			Count:   100,
		}

		// Publish a job with retry configuration
		err := queue.Publish(t.Context(), "retry_job", payload, sqlqueue.WithMaxRetries(maxRetries))
		require.NoError(t, err, "Failed to publish job with retries")

		// Wait for first attempt
		select {
		case attempt := <-jobAttempts:
			require.Equal(t, 1, attempt, "Expected first attempt")
		case <-time.After(2 * time.Second):
			t.Fatal("Job was not processed initially")
		}

		// Wait for second attempt (first retry)
		select {
		case attempt := <-jobAttempts:
			require.Equal(t, 2, attempt, "Expected second attempt")
		case <-time.After(4 * time.Second):
			t.Fatal("Job was not retried after first failure")
		}

		// Wait for third attempt (second retry)
		select {
		case attempt := <-jobAttempts:
			require.Equal(t, 3, attempt, "Expected third attempt")
		case <-time.After(8 * time.Second):
			t.Fatal("Job was not retried after second failure")
		}

		// Make sure we don't get a fourth attempt
		select {
		case attempt := <-jobAttempts:
			t.Fatalf("Unexpected fourth attempt: %d", attempt)
		case <-time.After(16 * time.Second):
			// This is expected - no more retries
		}
	})

	t.Run("Max retries exceeded", func(t *testing.T) {
		// Create channels to track job processing attempts
		jobAttempts := make(chan int, 3) // Buffer for multiple attempts

		// Use atomic counter to avoid race conditions
		var attemptCount int32 = 0
		maxRetries := 1 // We'll allow 1 retry (2 total attempts)

		// Subscribe to jobs
		queue.Subscribe(t.Context(), "max_retry_job", "max_retry_consumer", func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
			var payload TestPayload
			if err := json.Unmarshal(payloadBytes, &payload); err != nil {
				t.Errorf("Failed to unmarshal payload: %v", err)
				return err
			}

			count := atomic.AddInt32(&attemptCount, 1)
			jobAttempts <- int(count)

			// Always fail the job
			return errors.New("simulated failure")
		})

		// Create a payload
		payload := TestPayload{
			Message: "This job will exceed max retries",
			Count:   200,
		}

		// Publish a job with retry configuration
		err := queue.Publish(t.Context(), "max_retry_job", payload, sqlqueue.WithMaxRetries(maxRetries))
		require.NoError(t, err, "Failed to publish job with retries")

		// Wait for first attempt
		select {
		case attempt := <-jobAttempts:
			require.Equal(t, 1, attempt, "Expected first attempt")
		case <-time.After(2 * time.Second):
			t.Fatal("Job was not processed initially")
		}

		// Wait for second attempt (first retry)
		select {
		case attempt := <-jobAttempts:
			require.Equal(t, 2, attempt, "Expected second attempt")
		case <-time.After(4 * time.Second):
			t.Fatal("Job was not retried after first failure")
		}

		// Make sure we don't get a third attempt (max retries exceeded)
		select {
		case attempt := <-jobAttempts:
			t.Fatalf("Unexpected third attempt: %d", attempt)
		case <-time.After(8 * time.Second):
			// This is expected - no more retries
		}
	})
}
