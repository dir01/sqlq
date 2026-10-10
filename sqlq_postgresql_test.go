package sqlq_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	"go.opentelemetry.io/otel/trace"
)

func TestPostgreSQL(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping PostgreSQL tests in short mode")
	}

	tc, tracer, cleanup := setupPostgresTestCase(t)

	t.Cleanup(cleanup)

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

	t.Run("Job with NULL created_at is still delivered", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestNullCreatedAt")
		defer span.End()

		tc.TestNullCreatedAt(ctx, t)
	})
}

// TestNullCreatedAt checks that a job whose created_at is NULL, which the PostgreSQL schema allows
// (unlike SQLite's), is delivered with a zero CreatedAt instead of being claimed and dropped.
func (tc *TestCase) TestNullCreatedAt(ctx context.Context, t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	jobType := "null_created_at_test"
	infos := make(chan sqlq.JobInfo, 1)

	err := tc.Q.Consume(ctx, jobType, func(ctx context.Context, _ *sql.Tx, _ []byte) error {
		info, _ := sqlq.JobInfoFromContext(ctx)
		infos <- info
		return nil
	})
	require.NoError(t, err)

	_, err = tc.DB.ExecContext(ctx,
		"INSERT INTO jobs (job_type, payload, created_at) VALUES ($1, $2, NULL)",
		jobType, []byte(`{"message":"null created_at"}`),
	)
	require.NoError(t, err)

	select {
	case info := <-infos:
		require.True(t, info.CreatedAt.IsZero())
	case <-ctx.Done():
		t.Fatal("job with NULL created_at was not delivered")
	}
}

func setupPostgresTestCase(t *testing.T) (*TestCase, trace.Tracer, func()) {
	t.Helper()
	ctx := t.Context()
	ctx = GracefulContext(ctx, 100*time.Millisecond) // for shutting down mainly

	// Initialize only necessary fields for testcontainers request
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

	// Connect to the database with retry logic
	var db *sql.DB

	// Try to connect with retries
	var connectErr error
	for range 10 {
		db, connectErr = otelsql.Open("pgx", dsn)
		if connectErr != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		// Verify connection
		connectErr = db.Ping()
		if connectErr == nil {
			fmt.Println("Successfully connected to the database!")

			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	require.NoError(t, connectErr, "Failed to ping Postgres database after multiple attempts")

	// Set connection pool parameters
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	tracerCtx := GracefulContext(ctx, 1*time.Second)
	tracer, stopTracer, err := newTracer(tracerCtx, "localhost:4318")
	require.NoError(t, err)

	q, err := sqlq.New(
		db,
		sqlq.DBTypePostgres,
		sqlq.WithDefaultPollInterval(50*time.Millisecond),
		sqlq.WithDefaultBackoffFunc(func(_ uint16) time.Duration { return 0 }), // Rename unused 'i' to '_'
		sqlq.WithTracer(tracer),
	)
	require.NoError(t, err)

	q.Run()

	return &TestCase{Q: q, DB: db}, tracer, func() {
		require.NoError(t, db.Close())
		require.NoError(t, container.Terminate(tracerCtx))
		require.NoError(t, stopTracer())
	}
}
