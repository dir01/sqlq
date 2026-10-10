// Demonstrates inspecting jobs in the dead-letter queue.
package main

import (
	"context"
	"database/sql"
	"log"

	"github.com/dir01/sqlq"
	"github.com/dir01/sqlq/demo/internal/demoapp"
)

func main() {
	demoapp.RunSQLite(runExample)
}

func runExample(ctx context.Context, _ *sql.DB, q sqlq.JobsQueue) error {
	jobs, err := q.GetDeadLetterJobs(ctx, "slow_job", 10)
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		log.Print("no slow_job failures found")
		return nil
	}

	for _, job := range jobs {
		log.Printf("id=%d retries=%d failed_at=%s reason=%s payload=%s",
			job.OriginalID, job.RetryCount, job.FailedAt,
			job.FailureReason, job.Payload)
	}
	return nil
}
