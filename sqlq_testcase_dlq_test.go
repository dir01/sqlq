package sqlq_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
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

	// Other tests add and remove dead letter jobs concurrently, so count only jobs of a type of its own
	jobType := "dlq_limit_test"
	jobsCount := 3

	err := tc.Q.Consume(ctx, jobType, func(_ context.Context, _ *sql.Tx, _ []byte) error {
		return errors.New("simulated failure for limit test")
	}, sqlq.WithConsumerMaxRetries(0))
	require.NoError(t, err, "Failed to start consumer for DLQ limit test")

	for range jobsCount {
		require.NoError(t, tc.Q.Publish(ctx, jobType, TestPayload{Message: "limit test job"}))
	}

	require.Eventually(t, func() bool {
		dlqJobs, getErr := tc.Q.GetDeadLetterJobs(ctx, jobType, 100)
		return getErr == nil && len(dlqJobs) == jobsCount
	}, 5*time.Second, 10*time.Millisecond)

	for _, limit := range []int{1, jobsCount - 1, jobsCount, jobsCount + 1} {
		limitedJobs, getErr := tc.Q.GetDeadLetterJobs(ctx, jobType, limit)
		require.NoError(t, getErr, "Failed to get DLQ jobs with limit %d", limit)
		require.Len(t, limitedJobs, min(limit, jobsCount), "DLQ returned wrong number of jobs for limit %d", limit)
	}

	// Without a job type, the limit applies across all types
	dlqJobs, err := tc.Q.GetDeadLetterJobs(ctx, "", 1)
	require.NoError(t, err, "Failed to get DLQ jobs of all types with limit 1")
	require.Len(t, dlqJobs, 1, "DLQ returned more jobs than the limit")
}

func (tc *TestCase) TestDLQHook(ctx context.Context, t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	jobType := "dlq_hook_test"
	maxRetries := int32(2)

	// The handler locks the order row, and the hook marks the order failed. The hook can only do so
	// if the handler's transaction has finished by the time the hook runs.
	for _, query := range []string{
		"DROP TABLE IF EXISTS dlq_hook_orders",
		"CREATE TABLE dlq_hook_orders (id INTEGER PRIMARY KEY, status TEXT NOT NULL)",
		"INSERT INTO dlq_hook_orders (id, status) VALUES (1, 'new')",
	} {
		_, err := tc.DB.ExecContext(ctx, query)
		require.NoError(t, err)
	}

	type handlerCall struct {
		info sqlq.JobInfo
		ok   bool
	}
	var handlerCalls []handlerCall
	var handlerCallsMu sync.Mutex

	type hookCall struct {
		info    sqlq.JobInfo
		payload []byte
		err     error
		ctxInfo sqlq.JobInfo
		ctxOK   bool
	}
	hookCalls := make(chan hookCall, 2)

	err := tc.Q.Consume(ctx, jobType, func(ctx context.Context, tx *sql.Tx, _ []byte) error {
		info, ok := sqlq.JobInfoFromContext(ctx)
		handlerCallsMu.Lock()
		handlerCalls = append(handlerCalls, handlerCall{info: info, ok: ok})
		handlerCallsMu.Unlock()

		if _, err := tx.ExecContext(ctx, "UPDATE dlq_hook_orders SET status = 'processing' WHERE id = 1"); err != nil {
			return err
		}

		return errors.New("always fails")
	},
		sqlq.WithConsumerMaxRetries(maxRetries),
		sqlq.WithConsumerOnDeadLetter(func(ctx context.Context, tx *sql.Tx, info sqlq.JobInfo, payload []byte, handlerErr error) error {
			ctxInfo, ctxOK := sqlq.JobInfoFromContext(ctx)
			hookCalls <- hookCall{info: info, payload: payload, err: handlerErr, ctxInfo: ctxInfo, ctxOK: ctxOK}

			_, err := tx.ExecContext(ctx, "UPDATE dlq_hook_orders SET status = 'failed' WHERE id = 1")
			return err
		}),
	)
	require.NoError(t, err)

	publishedAt := time.Now()
	require.NoError(t, tc.Q.Publish(ctx, jobType, TestPayload{Message: "dlq_hook"}))

	var call hookCall
	select {
	case call = <-hookCalls:
	case <-ctx.Done():
		t.Fatal("dead letter hook was not called")
	}

	require.Equal(t, jobType, call.info.JobType)
	require.NotZero(t, call.info.ID)
	require.Equal(t, maxRetries, call.info.MaxRetries)
	require.Equal(t, uint16(maxRetries), call.info.RetryCount)
	// Generous, since the database server's clock sets created_at on some drivers
	require.WithinDuration(t, publishedAt, call.info.CreatedAt, time.Minute)
	require.True(t, call.info.IsFinalAttempt())
	require.True(t, call.ctxOK, "job info should be in dead letter hook context")
	require.Equal(t, call.info, call.ctxInfo)
	require.EqualError(t, call.err, "always fails")

	var p TestPayload
	require.NoError(t, json.Unmarshal(call.payload, &p))
	require.Equal(t, "dlq_hook", p.Message)

	require.Eventually(t, func() bool {
		var status string
		err := tc.DB.QueryRowContext(ctx, "SELECT status FROM dlq_hook_orders WHERE id = 1").Scan(&status)
		return err == nil && status == "failed"
	}, 5*time.Second, 10*time.Millisecond, "dead letter hook should have marked the order failed")

	require.Eventually(t, func() bool {
		dlqJobs, err := tc.Q.GetDeadLetterJobs(ctx, jobType, 10)
		return err == nil && slices.ContainsFunc(dlqJobs, func(j sqlq.DeadLetterJob) bool { return j.OriginalID == call.info.ID })
	}, 5*time.Second, 10*time.Millisecond, "job should be in the dead letter queue")

	handlerCallsMu.Lock()
	defer handlerCallsMu.Unlock()
	require.Len(t, handlerCalls, int(maxRetries)+1)
	for i, handlerCall := range handlerCalls {
		require.True(t, handlerCall.ok, "job info should be in handler context")
		require.Equal(t, call.info.ID, handlerCall.info.ID)
		require.Equal(t, call.info.CreatedAt, handlerCall.info.CreatedAt)
		require.Equal(t, uint16(i), handlerCall.info.RetryCount)
		require.Equal(t, i == int(maxRetries), handlerCall.info.IsFinalAttempt())
	}

	select {
	case <-hookCalls:
		t.Fatal("dead letter hook called more than once")
	case <-time.After(200 * time.Millisecond):
	}
}

// TestDLQHookFailure checks that when the dead letter hook fails, times out or panics,
// both its writes and the move to the dead letter queue are rolled back, and the job is retried.
func (tc *TestCase) TestDLQHookFailure(ctx context.Context, t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	jobType := "dlq_hook_failure_test"

	for _, query := range []string{
		"DROP TABLE IF EXISTS dlq_hook_failure_attempts",
		"CREATE TABLE dlq_hook_failure_attempts (attempt INTEGER NOT NULL)",
	} {
		_, err := tc.DB.ExecContext(ctx, query)
		require.NoError(t, err)
	}

	var handlerCalls atomic.Int32
	var hookCalls atomic.Int32
	var jobID atomic.Int64

	err := tc.Q.Consume(ctx, jobType, func(_ context.Context, _ *sql.Tx, _ []byte) error {
		handlerCalls.Add(1)
		return errors.New("always fails")
	},
		sqlq.WithConsumerMaxRetries(0),
		sqlq.WithConsumerJobTimeout(200*time.Millisecond),
		sqlq.WithConsumerOnDeadLetter(func(ctx context.Context, tx *sql.Tx, info sqlq.JobInfo, _ []byte, _ error) error {
			attempt := hookCalls.Add(1)
			jobID.Store(info.ID)

			// Every call writes, but only the write of the call that succeeds should be committed.
			insert := fmt.Sprintf("INSERT INTO dlq_hook_failure_attempts (attempt) VALUES (%d)", attempt)
			if _, err := tx.ExecContext(ctx, insert); err != nil {
				return err
			}

			switch attempt {
			case 1:
				return errors.New("hook failed")
			case 2:
				panic("hook panicked")
			case 3:
				<-ctx.Done() // Canceled after the job timeout
				return ctx.Err()
			default:
				return nil
			}
		}),
	)
	require.NoError(t, err)

	require.NoError(t, tc.Q.Publish(ctx, jobType, TestPayload{Message: "dlq_hook_failure"}))

	var dlqJob sqlq.DeadLetterJob
	require.Eventually(t, func() bool {
		dlqJobs, getErr := tc.Q.GetDeadLetterJobs(ctx, jobType, 10)
		if getErr != nil {
			return false
		}
		i := slices.IndexFunc(dlqJobs, func(j sqlq.DeadLetterJob) bool { return j.OriginalID == jobID.Load() })
		if i < 0 {
			return false
		}
		dlqJob = dlqJobs[i]
		return true
	}, 5*time.Second, 10*time.Millisecond, "job should eventually be in the dead letter queue")

	require.Equal(t, int32(4), hookCalls.Load())
	require.Equal(t, int32(4), handlerCalls.Load(), "the handler should run again after each failed hook")
	require.Equal(t, uint16(3), dlqJob.RetryCount, "each failed hook should count as a failed attempt")

	rows, err := tc.DB.QueryContext(ctx, "SELECT attempt FROM dlq_hook_failure_attempts")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var attempts []int
	for rows.Next() {
		var attempt int
		require.NoError(t, rows.Scan(&attempt))
		attempts = append(attempts, attempt)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []int{4}, attempts, "writes of failed hook calls should have been rolled back")

	time.Sleep(200 * time.Millisecond)
	require.Equal(t, int32(4), hookCalls.Load(), "dead letter hook should not be called after it succeeded")
}
