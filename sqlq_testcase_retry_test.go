package sqlq_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/require"
)

func (tc *TestCase) TestRetry(ctx context.Context, t *testing.T) {
	var attemptCount atomic.Int32
	jobAttempts := make(chan int, 5)
	maxRetries := 2 // We'll allow 2 retries (3 total attempts)

	// Consume the queue
	tc.Q.Consume(ctx, "retry_job", "retry_consumer", func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
		count := attemptCount.Add(1)
		jobAttempts <- int(count)

		// For first attempt, count is 1 (since .Add has already happened)
		// For first retry, count is 2
		if count <= int32(maxRetries) {
			// failed on first attempt and first retry
			return errors.New("simulated failure")
		}

		return nil
	}, sqlq.WithMaxRetries(maxRetries))

	// Create a payload
	payload := TestPayload{Message: "TestRetry"}

	// Publish a job with retry configuration
	err := tc.Q.Publish(ctx, "retry_job", payload)
	require.NoError(t, err)

	timeout := time.NewTimer(1 * time.Second)
	start := time.Now()

	// Wait for first attempt
	select {
	case attempt := <-jobAttempts:
		require.Equal(t, 1, attempt, "Expected first attempt")
	case <-timeout.C:
		t.Fatal("Job was not processed initially")
	}

	// Wait for second attempt (first retry)
	timeout.Reset(1 * time.Second)
	select {
	case attempt := <-jobAttempts:
		require.Equal(t, 2, attempt, "Expected second attempt")
	case <-timeout.C:
		t.Fatal("Job was not retried after first failure")
	}

	// Wait for third attempt (second retry)
	timeout.Reset(1 * time.Second)
	select {
	case attempt := <-jobAttempts:
		require.Equal(t, 3, attempt, "Expected third attempt")
	case <-timeout.C:
		t.Fatal("Job was not retried after second failure")
	}

	// Make sure we don't get a fourth attempt
	enoughTime := time.Since(start) * 4
	// The idea is that if we take all the time that it took to complete previous 3 attempts
	// Then 4 times as long should be more than enough to make sure that 4th attempt does not happen
	timeout.Reset(enoughTime)
	select {
	case attempt := <-jobAttempts:
		t.Fatalf("Unexpected fourth attempt: %d", attempt)
	case <-timeout.C:
		// This is expected - no more retries
	}
}

func (tc *TestCase) TestMultiConsumerRetry(ctx context.Context, t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	jobType := "multi_consumer_retry_job"
	consumerA := "consumer_A"
	consumerB := "consumer_B"
	maxRetriesA := 3 // 2 total attempts allowed for the job
	maxRetriesB := 1 // 4 total attempts allowed for the job

	consumerAttempts := make(map[string]int)
	mu := sync.Mutex{} // Mutex to protect the map

	handler := func(consumerName string, maxRetries int) func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
		return func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
			mu.Lock()
			attemptNum := consumerAttempts[consumerName]
			consumerAttempts[consumerName]++
			mu.Unlock()

			// Fail on all but last attmpts
			if attemptNum <= maxRetries {
				t.Logf("Consumer %s failing attempt %d", consumerName, attemptNum)
				return errors.New("simulated failure")
			}

			// Succeed on the third attempt
			t.Logf("Consumer %s succeeding on attempt %d", consumerName, attemptNum)
			return nil
		}
	}

	// Register consumer A
	tc.Q.Consume(ctx, jobType, consumerA, handler(consumerA, maxRetriesA), sqlq.WithMaxRetries(maxRetriesA))
	// Register consumer B
	tc.Q.Consume(ctx, jobType, consumerB, handler(consumerB, maxRetriesB), sqlq.WithMaxRetries(maxRetriesB))

	// Publish the job
	payload := TestPayload{Message: "MultiConsumerRetry"}
	err := tc.Q.Publish(ctx, jobType, payload)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		switch {
		case len(consumerAttempts) != 2:
			return false
		case consumerAttempts[consumerA] != maxRetriesA+1:
			return false
		case consumerAttempts[consumerB] != maxRetriesB+1:
			return false
		default:
			return true
		}
	}, 2*time.Second, 10*time.Millisecond)

	// Verify job is not in DLQ (since it should have succeeded)
	dlqJobs, err := tc.Q.GetDeadLetterJobs(ctx, jobType, 1)
	require.NoError(t, err, "Failed to check DLQ")
	require.Empty(t, dlqJobs, "Job should not have ended up in the DLQ")
}

func (tc *TestCase) TestRetryMaxExceeded(ctx context.Context, t *testing.T) {
	// Create a new context with timeout to avoid test hanging
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Create channels to track job processing attempts
	jobAttempts := make(chan int, 3) // Buffer for multiple attempts

	// Use atomic counter to avoid race conditions
	var attemptCount int32 = 0
	maxRetries := 1 // We'll allow 1 retry (2 total attempts)

	// Consume the queue
	tc.Q.Consume(ctx, "max_retry_job", "max_retry_consumer", func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
		var payload TestPayload
		if err := json.Unmarshal(payloadBytes, &payload); err != nil {
			t.Errorf("Failed to unmarshal payload: %v", err)
			return err
		}

		count := atomic.AddInt32(&attemptCount, 1)
		select {
		case jobAttempts <- int(count):
			// Successfully sent the attempt count
		case <-ctx.Done():
			// Context was canceled, don't block
			return ctx.Err()
		}

		// Always fail the job
		return errors.New("simulated failure")
	}, sqlq.WithMaxRetries(maxRetries))

	// Create a payload
	payload := TestPayload{
		Message: "This job will exceed max retries",
		Count:   200,
	}

	// Publish a job with retry configuration
	err := tc.Q.Publish(ctx, "max_retry_job", payload)
	require.NoError(t, err, "Failed to publish job with retries")

	timeout := time.NewTimer(1 * time.Second)
	start := time.Now()

	// Wait for first attempt
	select {
	case attempt := <-jobAttempts:
		require.Equal(t, 1, attempt, "Expected first attempt")
	case <-timeout.C:
		t.Fatal("Job was not processed initially")
	case <-ctx.Done():
		t.Fatal("Context timeout while waiting for first attempt")
	}

	// Wait for second attempt (first retry)
	timeout.Reset(1 * time.Second)
	select {
	case attempt := <-jobAttempts:
		require.Equal(t, 2, attempt, "Expected second attempt")
	case <-timeout.C:
		t.Fatal("Job was not retried after first failure")
	case <-ctx.Done():
		t.Fatal("Context timeout while waiting for second attempt")
	}

	// We should not get a third attempt (max retries exceeded)
	// But we'll wait a reasonable time to be sure
	enoughTime := time.Since(start) * 4
	timeout.Reset(enoughTime)
	select {
	case <-jobAttempts:
		t.Fatal("Got unexpected third attempt")
	case <-timeout.C:
		// This is the expected path - no more retries
	case <-ctx.Done():
		// Context timeout is also acceptable
	}
}
