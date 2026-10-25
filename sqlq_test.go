package sqlq_test

import (
	"context"
	"database/sql"
	"time"

	"github.com/dir01/sqlq"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
)

// TestPayload is the test data structure used across all tests
type TestPayload struct {
	Message string `json:"message"`
}

// pollEvery returns a poll function with a fixed delay.
func pollEvery(delay time.Duration) func(uint16) time.Duration {
	return func(uint16) time.Duration { return delay }
}

// TestCase contains actual logic of tests that will work across all the different drivers
// The idea is that you create per-driver test, and then call methods of test case
// This solution aims at providing an easy way to run and debug individual tests while sharing test logic
type TestCase struct {
	Q       sqlq.JobsQueue
	DB      *sql.DB                 // The database Q works on, for tests that need tables of their own
	Metrics *sdkmetric.ManualReader // Reads the metrics Q records; nil if the test case doesn't collect them
}

// GracefulContext creates a context that inherits values from its parent
// and cancels gracePeriod after the parent context is cancelled.
func GracefulContext(parentCtx context.Context, gracePeriod time.Duration) context.Context {
	newCtx := context.WithoutCancel(parentCtx)
	newCtx, newCancel := context.WithCancel(newCtx)

	go func() {
		<-parentCtx.Done()
		t := time.NewTimer(gracePeriod)
		<-t.C
		newCancel()
	}()

	return newCtx
}
