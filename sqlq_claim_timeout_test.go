package sqlq_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShortJobTimeoutRunsHandler examines very short job timeouts. The job timeout sets the claim.
// Before a worker starts a handler, it extends the claim if the remaining time is too short. If an
// extension also left too little time, the worker would extend the claim again and again, and the
// handler would never run.
func TestShortJobTimeoutRunsHandler(t *testing.T) {
	t.Parallel()
	for _, jobTimeout := range []time.Duration{time.Millisecond, 4 * time.Millisecond, 20 * time.Millisecond, 100 * time.Millisecond} {
		t.Run(jobTimeout.String(), func(t *testing.T) {
			t.Parallel()
			dsn := "file:" + filepath.Join(t.TempDir(), "claim.db") + "?_journal_mode=WAL&_busy_timeout=1000"
			db, err := sql.Open("sqlite3", dsn)
			require.NoError(t, err)
			q, err := sqlq.New(db, sqlq.DBTypeSQLite, sqlq.WithDefaultPollDelayFunc(pollEvery(10*time.Millisecond)))
			require.NoError(t, err)
			require.NoError(t, q.Run())
			t.Cleanup(func() {
				q.Shutdown()
				assert.NoError(t, db.Close())
			})
			ran := make(chan struct{}, 1)
			require.NoError(t, q.Consume(t.Context(), "claimed", func(context.Context, *sql.Tx, []byte) error {
				select {
				case ran <- struct{}{}:
				default:
				}
				return nil
			}, sqlq.WithConsumerJobTimeout(jobTimeout)))
			require.NoError(t, q.Publish(t.Context(), "claimed", TestPayload{Message: fmt.Sprint(jobTimeout)}))
			select {
			case <-ran:
			case <-time.After(3 * time.Second):
				t.Fatalf("the handler never ran with job timeout %s", jobTimeout)
			}
		})
	}
}
