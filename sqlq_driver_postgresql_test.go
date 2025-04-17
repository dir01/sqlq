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
)

func TestPostgreSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping PostgreSQL tests in short mode")
	}

	tc, cleanup := setupPostgresTestCase(t)
	defer cleanup()

	t.Run("Basic pub/sub", func(t *testing.T) {
		tc.TestBasicPubSub(t.Context(), t)
	})

	t.Run("Multiple subscribers for same job type", func(t *testing.T) {
		tc.TestBasicPubMultiSub(t.Context(), t)
	})
}

func setupPostgresTestCase(t *testing.T) (*TestCase, func()) {
	ctx := t.Context()

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
		db, connectErr = sql.Open("pgx", dsn)
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

	q, err := sqlq.New(db, sqlq.DBTypePostgres, sqlq.WithBackoffFunc(func(i int) time.Duration { return 0 }))
	require.NoError(t, err)

	q.Run()

	return &TestCase{Q: q}, func() {
		q.Shutdown()
		require.NoError(t, db.Close())
		require.NoError(t, container.Terminate(ctx))
	}
}
