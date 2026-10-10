// Demonstrates requeueing a job from the dead-letter queue.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"

	"github.com/dir01/sqlq"
	"github.com/dir01/sqlq/demo/internal/demoapp"
)

func main() {
	demoapp.RunSQLite(runExample)
}

func runExample(ctx context.Context, _ *sql.DB, q sqlq.JobsQueue) error {
	handler := func(_ context.Context, _ *sql.Tx, payload []byte) error {
		var message string
		if err := json.Unmarshal(payload, &message); err != nil {
			return err
		}
		log.Printf("fixed handler processed: %s", message)
		return nil
	}
	if err := q.Consume(ctx, "slow_job", handler); err != nil {
		return err
	}

	jobs, err := q.GetDeadLetterJobs(ctx, "slow_job", 1)
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		log.Print("nothing to requeue")
		return nil
	}

	id := jobs[0].OriginalID
	if err := q.RequeueDeadLetterJob(ctx, id); err != nil {
		return err
	}
	log.Printf("requeued original job %d", id)
	return nil
}
