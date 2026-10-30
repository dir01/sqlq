package sqlq_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/trace"
)

func TestPostgreSQL(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping PostgreSQL tests in short mode")
	}

	tc, tracer, cleanup := setupPostgresTestCase(t)

	t.Cleanup(cleanup)

	t.Run("Handler writes and completion share a transaction", func(t *testing.T) {
		t.Parallel()

		tc.TestConsumerTransaction(t.Context(), t)
	})

	t.Run("Publishing uses the caller's transaction", func(t *testing.T) {
		t.Parallel()

		tc.TestPublishTx(t.Context(), t)
	})

	t.Run("Basic pub/sub", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestBasicPubSub")
		defer span.End()

		tc.TestBasicPubSub(ctx, t)
	})

	t.Run("Multiple subscribers for same job type", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestBasicPubMultiSub")
		defer span.End()

		tc.TestBasicPubMultiSub(ctx, t)
	})

	t.Run("Metrics are recorded", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestMetrics")
		defer span.End()

		tc.TestMetrics(ctx, t)
	})

	t.Run("Job execution timeout", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestJobExecutionTimeout")
		defer span.End()

		tc.TestJobExecutionTimeout(ctx, t)
	})

	t.Run("Delayed job execution", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestDelayedJobExecution")
		defer span.End()

		tc.TestDelayedJobExecution(ctx, t)
	})

	t.Run("Job retry", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestRetry")
		defer span.End()

		tc.TestRetry(ctx, t)
	})

	t.Run("Max retries exeeded", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestRetryMaxExceeded")
		defer span.End()

		tc.TestRetryMaxExceeded(ctx, t)
	})

	t.Run("Failed jobs go to Dead Letter Queue", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestDLQBasic")
		defer span.End()

		tc.TestDLQBasic(ctx, t)
	})

	t.Run("Dead Letter Queue jobs may be requeued", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestDLQReque")
		defer span.End()

		tc.TestDLQReque(ctx, t)
	})

	t.Run("Can get Dead Letter Queue jobs", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestDLQGet")
		defer span.End()

		tc.TestDLQGet(ctx, t)
	})

	t.Run("Dead letter hook and job info", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestDLQHook")
		defer span.End()

		tc.TestDLQHook(ctx, t)
	})

	t.Run("Failed dead letter hook rolls back and retries", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestDLQHookFailure")
		defer span.End()

		tc.TestDLQHookFailure(ctx, t)
	})

	t.Run("Handler panic is recovered", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestPanic")
		defer span.End()

		tc.TestPanic(ctx, t)
	})

	t.Run("Fetching Dead Letter Queue jobs respects limits", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestDLQGetLimit")
		defer span.End()

		tc.TestDLQGetLimit(ctx, t)
	})

	t.Run("Job without created_at is rejected", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestNullCreatedAt")
		defer span.End()

		tc.TestNullCreatedAt(ctx, t)
	})

	t.Run("Sub-millisecond backoff respects retry limit", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestSubMillisecondBackoffRetryLimit")
		defer span.End()

		tc.TestSubMillisecondBackoffRetryLimit(ctx, t)
	})

	t.Run("PublishTx delay is measured from publish time", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestPublishTxDelayFromPublishTime")
		defer span.End()

		tc.TestPublishTxDelayFromPublishTime(ctx, t)
	})

	t.Run("Marking processed outlasts a transient row lock", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestMarkProcessedOutlastsTransientRowLock")
		defer span.End()

		tc.TestMarkProcessedOutlastsTransientRowLock(ctx, t)
	})
}

// TestNullCreatedAt makes sure that the queue cannot store a job without created_at. Before, the
// column could be NULL, and a job without created_at did not go to the DLQ.
func (tc *TestCase) TestNullCreatedAt(ctx context.Context, t *testing.T) {
	t.Helper()

	_, err := tc.DB.ExecContext(ctx,
		"INSERT INTO sqlq_jobs (job_type, payload, created_at) VALUES ($1, $2, NULL)",
		"null_created_at_test", []byte(`{"message":"null created_at"}`),
	)
	require.ErrorContains(t, err, "violates not-null constraint")
}

func setupPostgresTestCase(t *testing.T) (*TestCase, trace.Tracer, func()) {
	t.Helper()
	ctx := t.Context()
	ctx = GracefulContext(ctx, 100*time.Millisecond) // for shutting down mainly

	req := testcontainers.ContainerRequest{ //nolint:exhaustruct
		Name:         "sqlq_postgres",
		Image:        "postgres:14",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "testuser",
			"POSTGRES_PASSWORD": "testpass",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            true,
		ProviderType:     0,   // Initialize ProviderType
		Logger:           nil, // Initialize Logger
	})
	require.NoError(t, err, "Failed to start Postgres container")

	mappedPort, err := container.MappedPort(ctx, "5432")
	require.NoError(t, err, "Failed to get mapped port")

	host, err := container.Host(ctx)
	require.NoError(t, err, "Failed to get host")

	dsn := fmt.Sprintf("postgres://testuser:testpass@%s:%s/testdb?sslmode=disable", host, mappedPort.Port())

	var db *sql.DB

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		db, err = otelsql.Open("pgx", dsn)
		assert.NoError(collect, err)

		err = db.Ping()
		assert.NoError(collect, err)
	}, 10*time.Second, 100*time.Millisecond)

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	tracerCtx := GracefulContext(ctx, 1*time.Second)
	tracer, stopTracer, err := newTracer(tracerCtx)
	require.NoError(t, err)

	metrics := sdkmetric.NewManualReader()
	q, err := sqlq.New(
		db,
		sqlq.DBTypePostgres,
		sqlq.WithDefaultPollDelayFunc(pollEvery(50*time.Millisecond)),
		sqlq.WithDefaultBackoffFunc(func(_ uint16) time.Duration { return 0 }), // Rename unused 'i' to '_'
		sqlq.WithTracer(tracer),
		sqlq.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(metrics))),
	)
	require.NoError(t, err)

	err = q.Run()
	require.NoError(t, err, "Failed to run queue (initialize schema)")

	return &TestCase{Q: q, DB: db, Metrics: metrics}, tracer, func() {
		q.Shutdown() // Ensure consumers are stopped before closing DB and terminating container
		require.NoError(t, db.Close())
		require.NoError(t, container.Terminate(tracerCtx))
		require.NoError(t, stopTracer())
	}
}
