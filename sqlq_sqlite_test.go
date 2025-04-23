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
		sqlq.WithDefaultBackoffFunc(func(i uint16) time.Duration { return 0 }),
		sqlq.WithDefaultPollInterval(25*time.Millisecond),
		sqlq.WithTracer(tracer),
	)
	require.NoError(t, err)

	q.Run()

	t.Cleanup(func() {
		assert.NoError(t, stopTracer())
		q.Shutdown()
		assert.NoError(t, db.Close())
	})

	tc := &TestCase{Q: q}

	t.Run("Basic pub/sub", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.TestBasicPubSub")
		defer span.End()

		tc.TestBasicPubSub(ctx, t)
	})

	t.Run("Multiple subscribers for same job type", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.TestBasicPubMultiSub")
		defer span.End()

		tc.TestBasicPubMultiSub(ctx, t)
	})

	t.Run("Job execution timeout", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.TestJobExecutionTimeout")
		defer span.End()

		tc.TestJobExecutionTimeout(ctx, t)
	})

	t.Run("Delayed job execution", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.TestDelayedJobExecution")
		defer span.End()

		tc.TestDelayedJobExecution(ctx, t)
	})

	t.Run("Job retry", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.TestRetry")
		defer span.End()

		tc.TestRetry(ctx, t)
	})

	t.Run("Max retries exeeded", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.TestRetryMaxExceeded")
		defer span.End()

		tc.TestRetryMaxExceeded(ctx, t)
	})

	t.Run("Failed jobs go to Dead Letter Queue", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.TestDLQBasic")
		defer span.End()

		tc.TestDLQBasic(ctx, t)
	})

	t.Run("Dead Letter Queue jobs may be requeued", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.TestDLQReque")
		defer span.End()

		tc.TestDLQReque(ctx, t)
	})

	t.Run("Can get Dead Letter Queue jobs", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.TestDLQGet")
		defer span.End()

		tc.TestDLQGet(ctx, t)
	})

	t.Run("Fetching Dead Letter Queue jobs respects limits", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestSQLite.TestDLQGetLimit")
		defer span.End()

		tc.TestDLQGetLimit(ctx, t)
	})
}
