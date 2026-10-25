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

// TestUnusualPollFuncs makes sure that a consumer still polls when the poll function is nil
// (the option ignores it) or returns zero (the consumer polls again immediately).
// The consumer runs in a subprocess, because a panic in its goroutine stops the test binary.
func TestUnusualPollFuncs(t *testing.T) {
	t.Parallel()
	for _, pollFunc := range []string{"nil", "zero"} {
		t.Run(pollFunc, func(t *testing.T) {
			t.Parallel()
			cmd := exec.Command(os.Args[0], "-test.run=^TestPollFuncHelper$")
			cmd.Env = append(os.Environ(), "SQLQ_POLL_FUNC="+pollFunc, "SQLQ_POLL_DB="+filepath.Join(t.TempDir(), "poll.db"))
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "consumer process failed:\n%s", out)
		})
	}
}

// TestPollFuncHelper is the subprocess of TestUnusualPollFuncs.
func TestPollFuncHelper(t *testing.T) { //nolint:paralleltest // A subprocess with one test.
	var pollFunc func(uint16) time.Duration
	switch os.Getenv("SQLQ_POLL_FUNC") {
	case "":
		t.Skip("subprocess of TestUnusualPollFuncs")
	case "zero":
		pollFunc = pollEvery(0)
	}
	db, err := sql.Open("sqlite3", "file:"+os.Getenv("SQLQ_POLL_DB")+"?_journal_mode=WAL")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	q, err := sqlq.New(db, sqlq.DBTypeSQLite, sqlq.WithDefaultPollFunc(pollFunc))
	require.NoError(t, err)
	require.NoError(t, q.Run())
	defer q.Shutdown()
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
