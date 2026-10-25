package sqlq_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAcceptedClaimTimeoutRunsHandler examines claim timeouts a little above the job timeout.
// Consume does not accept the timeout, or the handler runs. Before a worker starts a handler, it
// extends the claim if the remaining time is less than the job timeout plus a margin. If the claim
// timeout is shorter than that, each extension is also too short, and the consumer never runs a job.
func TestAcceptedClaimTimeoutRunsHandler(t *testing.T) {
	t.Parallel()
	const jobTimeout = 100 * time.Millisecond
	for _, claimTimeout := range []time.Duration{
		jobTimeout + time.Millisecond, 110 * time.Millisecond, 150 * time.Millisecond, 152 * time.Millisecond,
		160 * time.Millisecond, 177 * time.Millisecond, 200 * time.Millisecond,
	} {
		t.Run(claimTimeout.String(), func(t *testing.T) {
			t.Parallel()
			dsn := "file:" + filepath.Join(t.TempDir(), "claim.db") + "?_journal_mode=WAL&_busy_timeout=1000"
			db, err := sql.Open("sqlite3", dsn)
			require.NoError(t, err)
			q, err := sqlq.New(db, sqlq.DBTypeSQLite, sqlq.WithDefaultPollInterval(10*time.Millisecond))
			require.NoError(t, err)
			require.NoError(t, q.Run())
			t.Cleanup(func() {
				q.Shutdown()
				assert.NoError(t, db.Close())
			})
			ran := make(chan struct{}, 1)
			err = q.Consume(t.Context(), "claimed", func(context.Context, *sql.Tx, []byte) error {
				ran <- struct{}{}
				return nil
			}, sqlq.WithConsumerJobTimeout(jobTimeout), sqlq.WithConsumerClaimTimeout(claimTimeout))
			if errors.Is(err, sqlq.ErrClaimTimeoutTooShort) {
				return
			}
			require.NoError(t, err)
			require.NoError(t, q.Publish(t.Context(), "claimed", TestPayload{Message: fmt.Sprint(claimTimeout)}))
			select {
			case <-ran:
			case <-time.After(3 * time.Second):
				t.Fatalf("claim timeout %s was accepted for job timeout %s, but the handler never ran", claimTimeout, jobTimeout)
			}
		})
	}
}
