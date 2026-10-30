package sqlq_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/require"
)

const (
	crashLoopDSNEnv     = "SQLQ_CRASH_LOOP_DSN"
	crashLoopJobType    = "crash_loop"
	crashLoopExitCode   = 3
	crashLoopMaxRetries = 1
)

// TestCrashingJobGoesToDeadLetterQueue examines a job whose handler stops the worker process.
// The process cannot write a failure, so only the start of each attempt counts it.
// After maxRetries + 1 attempts, the next worker moves the job to the dead letter queue,
// and does not run the handler again.
func TestCrashingJobGoesToDeadLetterQueue(t *testing.T) {
	t.Parallel()
	dsn := "file:" + filepath.Join(t.TempDir(), "crash.db") + "?_busy_timeout=5000&_journal_mode=WAL"
	db, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	q, err := sqlq.New(db, sqlq.DBTypeSQLite)
	require.NoError(t, err)
	require.NoError(t, q.Run()) // Run makes the tables. The parent has no consumers.
	t.Cleanup(q.Shutdown)
	require.NoError(t, q.Publish(t.Context(), crashLoopJobType, TestPayload{Message: "stops the process"}))

	crashes := 0
	for range crashLoopMaxRetries + 3 {
		cmd := exec.Command(os.Args[0], "-test.run=^TestCrashLoopWorkerHelper$")
		cmd.Env = append(os.Environ(), crashLoopDSNEnv+"="+dsn)
		cmd.Stderr = os.Stderr
		err = cmd.Run()
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == crashLoopExitCode {
			crashes++
			continue
		}
		require.NoError(t, err, "the worker must stop only in the handler")
		break
	}
	require.Equal(t, crashLoopMaxRetries+1, crashes, "the handler must run maxRetries + 1 times")

	dlq, err := q.GetDeadLetterJobs(t.Context(), crashLoopJobType, 10)
	require.NoError(t, err)
	require.Len(t, dlq, 1)
	require.Contains(t, dlq[0].FailureReason, "no attempts left")
	// The worker that moved the job also started an attempt. The DLQ does not count that attempt.
	require.Equal(t, uint32(crashLoopMaxRetries+1), dlq[0].RetryCount)
}

// TestCrashLoopWorkerHelper runs in a child process of TestCrashingJobGoesToDeadLetterQueue.
// The handler stops the process. If the job goes to the dead letter queue, the helper returns.
func TestCrashLoopWorkerHelper(t *testing.T) {
	t.Parallel()
	dsn := os.Getenv(crashLoopDSNEnv)
	if dsn == "" {
		t.Skip("subprocess helper")
	}
	db, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	q, err := sqlq.New(db, sqlq.DBTypeSQLite, sqlq.WithDefaultPollDelayFunc(pollEvery(10*time.Millisecond)))
	require.NoError(t, err)
	require.NoError(t, q.Run())
	t.Cleanup(q.Shutdown)
	require.NoError(t, q.Consume(t.Context(), crashLoopJobType, func(context.Context, *sql.Tx, []byte) error {
		os.Exit(crashLoopExitCode)
		return nil
	},
		sqlq.WithConsumerMaxRetries(crashLoopMaxRetries),
		// The claim of a stopped worker expires after 75ms. Then the next worker can take the job.
		sqlq.WithConsumerJobTimeout(50*time.Millisecond),
	))
	require.Eventually(t, func() bool {
		dlq, dlqErr := q.GetDeadLetterJobs(t.Context(), crashLoopJobType, 1)
		return dlqErr == nil && len(dlq) == 1
	}, 10*time.Second, 10*time.Millisecond, "the job did not go to the dead letter queue")
}
