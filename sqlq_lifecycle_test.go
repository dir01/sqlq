package sqlq_test

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openLifecycleDB opens a SQLite database of the test's own.
func openLifecycleDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(t.TempDir(), "queue.db")+"?_journal_mode=WAL&_busy_timeout=1000")
	require.NoError(t, err)
	return db
}

func TestRunReturnsSchemaErrors(t *testing.T) {
	t.Parallel()
	db := openLifecycleDB(t)
	q, err := sqlq.New(db, sqlq.DBTypeSQLite)
	require.NoError(t, err)

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, q.Run(canceled), context.Canceled, "Run must stop when its context ends")

	require.NoError(t, db.Close())
	require.ErrorContains(t, q.Run(t.Context()), "database is closed")
}

// syncBuffer is a bytes.Buffer that goroutines can share.
type syncBuffer struct {
	buf bytes.Buffer
	mu  sync.Mutex
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestBackgroundErrorsReachTheLogger(t *testing.T) {
	t.Parallel()
	db := openLifecycleDB(t)
	logs := &syncBuffer{} //nolint:exhaustruct
	q, err := sqlq.New(db, sqlq.DBTypeSQLite,
		sqlq.WithLogger(slog.New(slog.NewTextHandler(logs, nil))),
		sqlq.WithDefaultPollInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, q.Shutdown(context.Background()))
		assert.NoError(t, db.Close())
	})

	// Run was not called, so the jobs table is missing and each poll fails.
	require.NoError(t, q.Consume(t.Context(), "unmigrated", func(context.Context, *sql.Tx, []byte) error { return nil }))
	require.Eventually(t, func() bool {
		out := logs.String()
		return strings.Contains(out, "sqlq: failed to fetch jobs") &&
			strings.Contains(out, "job_type=unmigrated") &&
			strings.Contains(out, "no such table: jobs")
	}, 5*time.Second, 10*time.Millisecond, "a failed poll must be logged; logs: %s", logs)
}

func TestShutdownStopsWaitingWhenItsContextEnds(t *testing.T) {
	t.Parallel()
	db := openLifecycleDB(t)
	q, err := sqlq.New(db, sqlq.DBTypeSQLite, sqlq.WithDefaultPollInterval(10*time.Millisecond))
	require.NoError(t, err)
	require.NoError(t, q.Run(t.Context()))
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	stuck := func(context.Context, *sql.Tx, []byte) error {
		startOnce.Do(func() { close(started) })
		<-release // Ignores its context, as a handler must not.
		return nil
	}
	// One worker, and a buffer for the two other jobs.
	require.NoError(t, q.Consume(t.Context(), "stuck", stuck,
		sqlq.WithConsumerConcurrency(1), sqlq.WithConsumerPrefetchCount(3)))
	for range 3 {
		require.NoError(t, q.Publish(t.Context(), "stuck", TestPayload{Message: "stuck"}))
	}
	count := func(query string) int {
		t.Helper()
		var n int
		require.NoError(t, db.QueryRowContext(t.Context(), query).Scan(&n))
		return n
	}
	const claimed = `SELECT COUNT(*) FROM jobs WHERE claim_token IS NOT NULL`
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}
	require.Eventually(t, func() bool {
		var n int
		return db.QueryRowContext(t.Context(), claimed).Scan(&n) == nil && n == 3
	}, 5*time.Second, 10*time.Millisecond, "the two other jobs must wait in the buffer")

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	begin := time.Now()
	require.ErrorIs(t, q.Shutdown(ctx), context.DeadlineExceeded)
	require.Less(t, time.Since(begin), 5*time.Second, "Shutdown must stop waiting when its context ends")
	require.Equal(t, 1, count(claimed), "Shutdown must release the claims of the jobs that did not start")

	// Once the handler returns, another call finishes the shutdown and releases the last claim.
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, q.Shutdown(t.Context()))
	require.Zero(t, count(claimed))
	require.Equal(t, 3, count(`SELECT COUNT(*) FROM jobs WHERE processed_at IS NULL`),
		"an attempt that outlived its context must not complete its job")
}

func TestConsumeRejectsNilFunctions(t *testing.T) {
	t.Parallel()
	db := openLifecycleDB(t)
	handler := func(context.Context, *sql.Tx, []byte) error { return nil }
	newQueue := func(opts ...sqlq.NewOption) sqlq.JobsQueue {
		q, err := sqlq.New(db, sqlq.DBTypeSQLite, opts...)
		require.NoError(t, err)
		require.NoError(t, q.Run(t.Context()))
		t.Cleanup(func() { assert.NoError(t, q.Shutdown(context.Background())) })
		return q
	}
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	q := newQueue()
	require.ErrorIs(t, q.Consume(t.Context(), "nil", nil), sqlq.ErrNilHandler)
	require.ErrorIs(t, q.Consume(t.Context(), "nil", handler, sqlq.WithConsumerBackoffFunc(nil)), sqlq.ErrNilBackoffFunc)
	require.NoError(t, q.Consume(t.Context(), "nil", handler), "a rejected registration must leave the job type free")

	q = newQueue(sqlq.WithDefaultBackoffFunc(nil))
	require.ErrorIs(t, q.Consume(t.Context(), "nil_default", handler), sqlq.ErrNilBackoffFunc)
	require.NoError(t, q.Consume(t.Context(), "nil_default", handler,
		sqlq.WithConsumerBackoffFunc(func(uint16) time.Duration { return 0 })))
}
