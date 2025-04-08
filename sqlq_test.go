package sqlq_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/dir01/sqlq"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/mattn/go-sqlite3"
)

// TestPayload is the test data structure used across all tests
type TestPayload struct {
	Message string `json:"message"`
	Count   int    `json:"count"`
}

// TestDBConfig holds configuration for a test database
type TestDBConfig struct {
	DBType          sqlq.DBType
	DB              *sql.DB
	PollingInterval time.Duration
	Cleanup         func()
	Container       testcontainers.Container
}

func TestSQLiteQueue(t *testing.T) {
	dbConfig := setupSQLiteDB(t)
	defer dbConfig.Cleanup()

	runQueueTests(t, dbConfig)
}

func TestPostgresQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping PostgreSQL tests in short mode")
	}

	dbConfig := setupPostgresDB(t)
	defer dbConfig.Cleanup()

	runQueueTests(t, dbConfig)
}

func TestMysqlq(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping MySQL tests in short mode")
	}

	dbConfig := setupMySQLDB(t)
	defer dbConfig.Cleanup()

	runQueueTests(t, dbConfig)
}

// runQueueTests runs the standard test suite against a database
func runQueueTests(t *testing.T, dbConfig *TestDBConfig) {
	t.Helper()

	// Create a queue with a short poll interval for testing
	queue, err := sqlq.New(dbConfig.DB, dbConfig.DBType, 100*time.Millisecond)
	require.NoError(t, err, "Failed to create queue")

	// Start the queue
	queue.Run()
	defer queue.Shutdown()

	t.Run("Basic publish and subscribe", func(t *testing.T) {
		// Create channels to track job processing
		jobProcessed := make(chan bool, 1)
		var receivedPayload TestPayload

		// Subscribe to jobs
		queue.Subscribe(t.Context(), "test_job", "test_consumer", func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
			var payload TestPayload
			if err := json.Unmarshal(payloadBytes, &payload); err != nil {
				t.Errorf("Failed to unmarshal payload: %v", err)
				return err
			}

			receivedPayload = payload

			jobProcessed <- true
			return nil
		})

		// Publish a job
		testPayload := TestPayload{Message: "Hello, World!", Count: 42}
		err = queue.Publish(t.Context(), "test_job", testPayload)
		require.NoError(t, err, "Failed to publish job")

		// Wait for the job to be processed with a longer timeout
		select {
		case <-jobProcessed:
			// Job was processed
		case <-time.After(5 * time.Second):
			t.Fatal("Timed out waiting for job to be processed")
		}

		// Verify the payload was correctly received
		require.Equal(t, testPayload.Message, receivedPayload.Message, "Message mismatch")
		require.Equal(t, testPayload.Count, receivedPayload.Count, "Count mismatch")
	})

	t.Run("Multiple subscribers for same job type", func(t *testing.T) {
		// Create channels to track job processing
		consumer1Processed := make(chan bool, 1)
		consumer2Processed := make(chan bool, 1)

		// Subscribe first consumer
		queue.Subscribe(t.Context(), "shared_job", "consumer1", func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
			consumer1Processed <- true
			return nil
		})

		// Subscribe second consumer
		queue.Subscribe(t.Context(), "shared_job", "consumer2", func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
			consumer2Processed <- true
			return nil
		})

		// Publish a job
		err = queue.Publish(context.Background(), "shared_job", TestPayload{Message: "Shared job", Count: 100})
		require.NoError(t, err, "Failed to publish shared job")

		// Wait for both consumers to process the job
		for i, ch := range []chan bool{consumer1Processed, consumer2Processed} {
			consumerName := fmt.Sprintf("consumer%d", i+1)
			select {
			case <-ch:
				// Job was processed by this consumer
			case <-time.After(5 * time.Second):
				t.Fatalf("Timed out waiting for job to be processed by %s", consumerName)
			}
		}
	})

	t.Run("Transaction support", func(t *testing.T) {
		jobProcessed := make(chan bool, 1)

		// Subscribe to jobs
		queue.Subscribe(context.Background(), "tx_job", "tx_consumer", func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
			var payload TestPayload
			if err := json.Unmarshal(payloadBytes, &payload); err != nil {
				t.Errorf("Failed to unmarshal payload: %v", err)
				return err
			}
			jobProcessed <- true
			return nil
		})

		// Start a transaction
		tx, err := dbConfig.DB.BeginTx(context.Background(), nil)
		require.NoError(t, err, "Failed to begin transaction")

		// Publish a job within the transaction
		err = queue.PublishTx(context.Background(), tx, "tx_job", TestPayload{Message: "Transaction job", Count: 200})
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("Failed to publish job in transaction: %v", err)
		}
		// Commit the transaction
		err = tx.Commit()
		require.NoError(t, err, "Failed to commit transaction")

		// Wait for the job to be processed
		select {
		case <-jobProcessed:
			// Job was processed
		case <-time.After(5 * time.Second):
			t.Fatal("Timed out waiting for transaction job to be processed")
		}
	})
}

// setupSQLiteDB sets up an in-memory SQLite database for testing
func setupSQLiteDB(t *testing.T) *TestDBConfig {
	db, err := sql.Open("sqlite3", "file:memdb1?mode=memory&cache=shared")
	require.NoError(t, err, "Failed to open SQLite database")

	return &TestDBConfig{
		DBType:          sqlq.DBTypeSQLite,
		DB:              db,
		PollingInterval: 10 * time.Millisecond,
		Cleanup:         func() { _ = db.Close() },
	}
}

// setupPostgresDB sets up a PostgreSQL database using testcontainers
func setupPostgresDB(t *testing.T) *TestDBConfig {
	ctx := context.Background()

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

	return &TestDBConfig{
		DBType:          sqlq.DBTypePostgres,
		DB:              db,
		Container:       container,
		PollingInterval: 10 * time.Millisecond,
		Cleanup: func() {
			_ = db.Close()
			_ = container.Terminate(ctx)
		},
	}
}

// setupMySQLDB sets up a MySQL database using testcontainers
func setupMySQLDB(t *testing.T) *TestDBConfig {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "mysql:8",
		Name:         "sqlq_mysql",
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "rootpass",
			"MYSQL_USER":          "testuser",
			"MYSQL_PASSWORD":      "testpass",
			"MYSQL_DATABASE":      "testdb",
		},
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            true,
	})
	require.NoError(t, err, "Failed to start MySQL container")

	mappedPort, err := container.MappedPort(ctx, "3306")
	require.NoError(t, err, "Failed to get mapped port")

	host, err := container.Host(ctx)
	require.NoError(t, err, "Failed to get host")

	dsn := fmt.Sprintf("testuser:testpass@tcp(%s:%s)/testdb?parseTime=true", host, mappedPort.Port())

	// Connect to the database
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "Failed to open MySQL database")

	// MySQL needs some time to be fully ready
	for i := 0; i < 10; i++ {
		err = db.Ping()
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	require.NoError(t, err, "Failed to ping MySQL database")

	return &TestDBConfig{
		DBType:          sqlq.DBTypeMySQL,
		DB:              db,
		Container:       container,
		PollingInterval: 10 * time.Millisecond,
		Cleanup: func() {
			_ = db.Close()
			_ = container.Terminate(ctx)
		},
	}
}
