package sqlq_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/require"
)

func TestSQLiteDelayedJobs(t *testing.T) {
	dbConfig := setupSQLiteDB(t)
	defer dbConfig.Cleanup()

	runDelayedJobTests(t, dbConfig)
}

func TestPostgresDelayedJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping PostgreSQL tests in short mode")
	}

	dbConfig := setupPostgresDB(t)
	defer dbConfig.Cleanup()

	runDelayedJobTests(t, dbConfig)
}

func TestMySQLDelayedJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping MySQL tests in short mode")
	}

	dbConfig := setupMySQLDB(t)
	defer dbConfig.Cleanup()

	runDelayedJobTests(t, dbConfig)
}

func runDelayedJobTests(t *testing.T, dbConfig *TestDBConfig) {
	t.Helper()

	// Create a queue for testing
	queue, err := sqlq.New(dbConfig.DB, dbConfig.DBType)
	require.NoError(t, err, "Failed to create queue")

	// Start the queue
	queue.Run()
	defer queue.Shutdown()

	t.Run("Delayed job execution", func(t *testing.T) {
		jobProcessed := make(chan bool, 1)

		queue.Subscribe(t.Context(), "delayed_job", "test_consumer", func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
			jobProcessed <- true
			return nil
		})

		delay := 500 * time.Millisecond

		// Publish a job with a delay
		err := queue.Publish(
			t.Context(),
			"delayed_job",
			TestPayload{Message: "This is a delayed job", Count: 42},
			sqlq.WithDelay(delay),
		)
		require.NoError(t, err, "Failed to publish delayed job")

		// The job should not be processed immediately
		select {
		case <-jobProcessed:
			t.Fatal("Job was processed before delay elapsed")
		case <-time.After(delay / 2):
			// This is expected
		}

		// Wait for the job to be processed after the delay
		select {
		case <-jobProcessed:
			// Job was processed as expected
		case <-time.After(delay * 6): // Give a bit more time for slower DB containers
			t.Fatal("Job was not processed after delay elapsed")
		}
	})
}
