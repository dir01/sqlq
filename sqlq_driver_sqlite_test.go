package sqlq_test

import (
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/opentelemetry-go-extra/otelsql"
)

func TestSQLite(t *testing.T) {
	db, err := otelsql.Open("sqlite3", "file:memdb1?mode=memory&cache=shared")
	require.NoError(t, err, "Failed to open SQLite database")

	tracerCtx := GracefulContext(t.Context(), 10*time.Millisecond)
	tracer, stopTracer, err := newTracer(tracerCtx, "localhost:4318")
	require.NoError(t, err)

	q, err := sqlq.New(
		db, sqlq.DBTypeSQLite,
		sqlq.WithBackoffFunc(func(i int) time.Duration { return 0 }),
		sqlq.WithPollInterval(1*time.Millisecond),
		sqlq.WithTracer(tracer),
	)
	require.NoError(t, err)

	q.Run()

	t.Cleanup(func() {
		t.Logf("[%s] Stop tracer", time.Now().String())
		assert.NoError(t, stopTracer())

		t.Logf("[%s] Shutting down queue", time.Now().String())
		q.Shutdown()

		t.Logf("[%s] Closing database", time.Now().String())
		assert.NoError(t, db.Close())

	})

	tc := &TestCase{Q: q}

	t.Run("Basic pub/sub", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.BasicPubMultiSub")
		defer span.End()

		tc.BasicPubSub(ctx, t)
	})

	t.Run("Multiple subscribers for same job type", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.BasicPubMultiSub")
		defer span.End()

		tc.BasicPubMultiSub(ctx, t)
	})

	t.Run("Delayed job execution", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.DelayedJobExecution")
		defer span.End()

		tc.DelayedJobExecution(ctx, t)
	})

	t.Run("Job retry", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.Retry")
		defer span.End()

		tc.Retry(ctx, t)
	})

	t.Run("Max retries exeeded", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.RetryMaxExceeded")
		defer span.End()

		tc.RetryMaxExceeded(ctx, t)
	})

	t.Run("Failed jobs go to Dead Letter Queue", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.DLQBasic")
		defer span.End()

		tc.DLQBasic(ctx, t)
	})

	t.Run("Dead Letter Queue jobs may be requeued", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.DLQReque")
		defer span.End()

		tc.DLQReque(ctx, t)
	})

	t.Run("Can get Dead Letter Queue jobs", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.DLQGet")
		defer span.End()

		tc.DLQGet(ctx, t)
	})

	t.Run("Fetching Dead Letter Queue jobs respects limits", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.DLQGetLimit")
		defer span.End()

		tc.DLQGetLimit(ctx, t)
	})
}
