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

// TestNonPositiveDefaultPollIntervalIgnored checks that a zero or negative default poll interval keeps
// the default, as the per-consumer option does, instead of crashing the process from the poller.
// The consumer runs in a subprocess, because a panic in its goroutine would end this test binary.
func TestNonPositiveDefaultPollIntervalIgnored(t *testing.T) {
	t.Parallel()
	for _, interval := range []string{"0s", "-1s"} {
		t.Run(interval, func(t *testing.T) {
			t.Parallel()
			cmd := exec.Command(os.Args[0], "-test.run=^TestPollIntervalHelper$")
			cmd.Env = append(os.Environ(), "SQLQ_POLL_INTERVAL="+interval, "SQLQ_POLL_DB="+filepath.Join(t.TempDir(), "poll.db"))
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "consumer process failed:\n%s", out)
		})
	}
}

// TestPollIntervalHelper is the subprocess of TestNonPositiveDefaultPollIntervalIgnored.
func TestPollIntervalHelper(t *testing.T) { //nolint:paralleltest // A subprocess with a single test.
	interval := os.Getenv("SQLQ_POLL_INTERVAL")
	if interval == "" {
		t.Skip("subprocess of TestNonPositiveDefaultPollIntervalIgnored")
	}
	tick, err := time.ParseDuration(interval)
	require.NoError(t, err)
	db, err := sql.Open("sqlite3", "file:"+os.Getenv("SQLQ_POLL_DB")+"?_journal_mode=WAL")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	q, err := sqlq.New(db, sqlq.DBTypeSQLite, sqlq.WithDefaultPollInterval(tick))
	require.NoError(t, err)
	q.Run()
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
