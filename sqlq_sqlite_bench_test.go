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
	// Use a unique in-memory DB for each benchmark run to avoid interference
	// "file::memory:?cache=shared" allows multiple connections in the same process
	// We add a unique identifier based on benchmark name/time if needed, but for simple cases,
	// just recreating it might be sufficient if benchmarks run sequentially.
	// Using a file-based DB might be more realistic for some benchmarks, but slower.
	dbPath := fmt.Sprintf("file:bench_%s_%d.db?mode=memory&cache=shared", b.Name(), time.Now().UnixNano())
	// dbPath := fmt.Sprintf("./bench_%s.db", b.Name()) // Option for file-based DB
	// _ = os.Remove(dbPath) // Clean up previous file if exists

	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(b, err, "Failed to open SQLite database for benchmark")

	// Optional: Configure connection pool for potentially better performance
	db.SetMaxOpenConns(50) // Adjust based on expected load
	db.SetMaxIdleConns(10)

	cleanup := func() {
		err := db.Close()
		if err != nil {
			b.Logf("Warning: Error closing benchmark database: %v", err)
		}
		// If using a file-based DB:
		// err = os.Remove(dbPath)
		// if err != nil && !os.IsNotExist(err) {
		// 	b.Logf("Warning: Error removing benchmark database file %s: %v", dbPath, err)
		// }
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
// BenchmarkHighConcurrency
// With polling alone
// BenchmarkHighConcurrency-10          517           2184726 ns/op            7774 B/op        154 allocs/op
// With polling and push
// BenchmarkHighConcurrency-10         6561            210670 ns/op            9699 B/op        208 allocs/op
func BenchmarkHighConcurrency(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	// Use reasonable defaults, adjust concurrency as needed for benchmarking
	concurrency := uint16(50)   // Number of concurrent workers
	prefetchCount := uint16(20) // How many jobs each poll fetches
	jobType := "benchmark_job"

	q, err := sqlq.New(db, sqlq.DBTypeSQLite,
		sqlq.WithDefaultConcurrency(concurrency),
		sqlq.WithDefaultPrefetchCount(prefetchCount),
		// Disable cleanup during benchmark for less noise, unless cleanup is part of the test
		sqlq.WithDefaultCleanupProcessedInterval(0),
		sqlq.WithDefaultCleanupDLQInterval(0),
	)
	require.NoError(b, err)

	q.Run()

	var processedCount atomic.Int64
	totalJobs := int64(b.N) // Total jobs to process in this benchmark run

	// Simple handler that just increments a counter
	handler := func(_ context.Context, _ *sql.Tx, _ []byte) error {
		processedCount.Add(1)
		return nil
	}

	err = q.Consume(
		b.Context(), jobType, handler,
		sqlq.WithConsumerPollInteval(100*time.Millisecond),
		sqlq.WithConsumerPushSubscription(10000),
	)

	require.NoError(b, err)
	q.Run()

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
