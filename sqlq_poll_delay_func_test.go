package sqlq_test

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/require"
)

// TestUnusualPollDelayFuncs makes sure that a consumer still polls when the poll delay function is nil
// (the option ignores it) or returns zero (the consumer polls again immediately).
// The consumer runs in a subprocess, because a panic in its goroutine stops the test binary.
func TestUnusualPollDelayFuncs(t *testing.T) {
	t.Parallel()
	for _, pollDelayFunc := range []string{"nil", "zero"} {
		t.Run(pollDelayFunc, func(t *testing.T) {
			t.Parallel()
			cmd := exec.Command(os.Args[0], "-test.run=^TestPollDelayFuncHelper$")
			cmd.Env = append(os.Environ(), "SQLQ_POLL_FUNC="+pollDelayFunc, "SQLQ_POLL_DB="+filepath.Join(t.TempDir(), "poll.db"))
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "consumer process failed:\n%s", out)
		})
	}
}

// TestPollDelayFuncHelper is the subprocess of TestUnusualPollDelayFuncs.
func TestPollDelayFuncHelper(t *testing.T) { //nolint:paralleltest // A subprocess with one test.
	var pollDelayFunc func(uint16) time.Duration
	switch os.Getenv("SQLQ_POLL_FUNC") {
	case "":
		t.Skip("subprocess of TestUnusualPollDelayFuncs")
	case "zero":
		pollDelayFunc = pollEvery(0)
	}
	db, err := sql.Open("sqlite3", "file:"+os.Getenv("SQLQ_POLL_DB")+"?_journal_mode=WAL")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	q, err := sqlq.New(db, sqlq.DBTypeSQLite, sqlq.WithDefaultPollDelayFunc(pollDelayFunc))
	require.NoError(t, err)
	require.NoError(t, q.Run(t.Context()))
	defer func() { require.NoError(t, q.Shutdown(t.Context())) }()
	processed := make(chan struct{})
	require.NoError(t, q.Consume(t.Context(), "poll", func(context.Context, *sql.Tx, []byte) error {
		close(processed)
		return nil
	}))
	require.NoError(t, q.Publish(t.Context(), "poll", TestPayload{Message: "polled"}))
	select {
	case <-processed:
	case <-time.After(5 * time.Second):
		t.Fatal("job was never polled")
	}
}
