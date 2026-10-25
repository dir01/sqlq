package sqlq_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	_ "github.com/mattn/go-sqlite3" // Import SQLite driver
	"github.com/stretchr/testify/require"
)

// setupBenchmarkDB creates an in-memory SQLite database for benchmark tests.
func setupBenchmarkDB(b *testing.B) (*sql.DB, func()) {
	b.Helper()

	options := "?_journal_mode=wal"
	dbPath := fmt.Sprintf("file:%s/bench_%s_%d.db%s", b.TempDir(), b.Name(), time.Now().UnixNano(), options)

	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(b, err, "Failed to open SQLite database for benchmark")

	db.SetMaxOpenConns(500)
	db.SetMaxIdleConns(500)

	cleanup := func() {
		err := db.Close()
		if err != nil {
			b.Logf("Warning: Error closing benchmark database: %v", err)
		}
		// db file will destroy itself by virtue of using b.TempDir()
	}

	return db, cleanup
}

// BenchmarkHighConcurrency measures the throughput of publishing and consuming jobs
// with a high number of concurrent workers.
// $ go test -v -bench=. -benchmem -run BenchmarkHighConcurrency

// goos: darwin
// goarch: arm64
// pkg: github.com/dir01/sqlq
// cpu: Apple M1 Pro

// ### No options
// BenchmarkHighConcurrency-10         1204            960542 ns/op           19868 B/op        238 allocs/op
// ### ?_journal_mode=wal
// BenchmarkHighConcurrency-10         5648            215926 ns/op           12469 B/op        194 allocs/op

func BenchmarkHighConcurrency(b *testing.B) {
	b.ReportAllocs()

	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	// Use reasonable defaults, adjust concurrency as needed for benchmarking
	concurrency := uint16(50)   // Number of concurrent workers
	prefetchCount := uint16(20) // How many jobs each poll fetches
	jobType := "benchmark_job"

	q, err := sqlq.New(db, sqlq.DBTypeSQLite,
		sqlq.WithDefaultConcurrency(concurrency),
		sqlq.WithDefaultPrefetchCount(prefetchCount),
		// Disable cleanup during benchmark for less noise
		sqlq.WithDefaultCleanupProcessedInterval(0),
		sqlq.WithDefaultCleanupDLQInterval(0),
	)
	require.NoError(b, err)

	require.NoError(b, q.Run())

	var processedCount atomic.Int64
	totalJobs := int64(b.N) // Total jobs to process in this benchmark run

	// Simple handler that just increments a counter
	handler := func(_ context.Context, _ *sql.Tx, _ []byte) error {
		processedCount.Add(1)
		return nil
	}

	err = q.Consume(
		b.Context(), jobType, handler,
		sqlq.WithConsumerPollFunc(pollEvery(100*time.Millisecond)),
		sqlq.WithAsyncPush(),
	)

	require.NoError(b, err)
	require.NoError(b, q.Run())

	payload := []byte(`{"data":"benchmark"}`)

	b.ResetTimer()

	for i := range b.N {
		err = q.Publish(b.Context(), jobType, payload)
		if err != nil {
			b.Fatalf("Failed to publish job %d: %v", i, err)
		}
	}

	// Wait for all b.N jobs to be processed
	// Use a timeout to prevent benchmark hanging indefinitely
	waitTimeout := time.Duration(totalJobs/int64(concurrency)*50)*time.Millisecond + 5*time.Second // Estimate + buffer
	waitCtx, cancelWait := context.WithTimeout(context.Background(), waitTimeout)
	defer cancelWait()

	ticker := time.NewTicker(50 * time.Millisecond) // Check frequently but not too aggressively
	defer ticker.Stop()

	for processedCount.Load() < totalJobs {
		select {
		case <-ticker.C:
			// Continue loop
		case <-waitCtx.Done():
			b.Fatalf("Timeout waiting for jobs to be processed. Processed %d/%d", processedCount.Load(), totalJobs)
		}
	}

	b.StopTimer() // Stop timing before cleanup

	// Verification
	finalCount := processedCount.Load()
	if finalCount != totalJobs {
		b.Errorf("Benchmark finished but processed count mismatch: expected %d, got %d", totalJobs, finalCount)
	}

	// Check DLQ is empty
	dlqJobs, err := q.GetDeadLetterJobs(context.Background(), jobType, 1)
	require.NoError(b, err, "Failed to check DLQ")
	require.Empty(b, dlqJobs, "DLQ should be empty after successful benchmark run")

	// Cleanup
	q.Shutdown()
}
