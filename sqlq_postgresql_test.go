package sqlq_test

import (
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
	if testing.Short() {
		t.Skip("Skipping PostgreSQL tests in short mode")
	}

	tc, tracer, cleanup := setupPostgresTestCase(t)

	t.Cleanup(cleanup)

	t.Parallel()

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

	t.Run("Fetching Dead Letter Queue jobs respects limits", func(t *testing.T) {
		t.Parallel()

		ctx, span := tracer.Start(t.Context(), "TestPostgreSQL.TestDLQGetLimit")
		defer span.End()

		tc.TestDLQGetLimit(ctx, t)
	})
}

func setupPostgresTestCase(t *testing.T) (*TestCase, trace.Tracer, func()) {
	ctx := t.Context()
	ctx = GracefulContext(ctx, 100*time.Millisecond) // for shutting down mainly

	req := testcontainers.ContainerRequest{
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

		time.Sleep(1 * time.Second)
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
		sqlq.WithPollInterval(50*time.Millisecond),
		sqlq.WithBackoffFunc(func(i int) time.Duration { return 0 }),
		sqlq.WithTracer(tracer),
	)
	require.NoError(t, err)

	q.Run()

	return &TestCase{Q: q}, tracer, func() {
		require.NoError(t, db.Close())
		require.NoError(t, container.Terminate(tracerCtx))
		require.NoError(t, stopTracer())
	}
}
