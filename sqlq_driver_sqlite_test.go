package sqlq_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLite(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	db, err := sql.Open("sqlite3", "file:memdb1?mode=memory&cache=shared")
	require.NoError(err, "Failed to open SQLite database")

	q, err := sqlq.New(
		db, sqlq.DBTypeSQLite,
		sqlq.WithBackoffFunc(func(i int) time.Duration { return 0 }),
		sqlq.WithPollInterval(1*time.Millisecond),
	)
	require.NoError(err)

	q.Run()

	t.Cleanup(func() {
		t.Logf("[%s] Shutting down queue", time.Now().String())
		q.Shutdown()
		t.Logf("[%s] Closing database", time.Now().String())
		assert.NoError(db.Close())
	})

	tc := &TestCase{Q: q}

	t.Run("Basic pub/sub", func(t *testing.T) {
		t.Parallel()
		tc.BasicPubSub(t)
	})

	t.Run("Multiple subscribers for same job type", func(t *testing.T) {
		t.Parallel()
		tc.BasicPubMultiSub(t)
	})

	t.Run("Delayed job execution", func(t *testing.T) {
		t.Parallel()
		tc.DelayedJobExecution(t)
	})

	t.Run("Job retry", func(t *testing.T) {
		t.Parallel()
		tc.Retry(t)
	})

	t.Run("Max retries exeeded", func(t *testing.T) {
		t.Parallel()
		tc.RetryMaxExceeded(t)
	})

	t.Run("Failed jobs go to Dead Letter Queue", func(t *testing.T) {
		t.Parallel()
		tc.DLQBasic(t)
	})

	t.Run("Dead Letter Queue jobs may be requeued", func(t *testing.T) {
		t.Parallel()
		tc.DLQReque(t)
	})

	t.Run("Can get Dead Letter Queue jobs", func(t *testing.T) {
		t.Parallel()
		tc.DLQGet(t)
	})

	t.Run("Fetching Dead Letter Queue jobs respects limits", func(t *testing.T) {
		t.Parallel()
		tc.DLQGetLimit(t)
	})
}
