// Demonstrates configuring consumer concurrency and prefetching.
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
		var number int
		if err := json.Unmarshal(payload, &number); err != nil {
			return err
		}
		log.Printf("processing job %d", number)
		return nil
	}

	err := q.Consume(ctx, "numbered_job", handler,
		sqlq.WithConsumerConcurrency(3),
		sqlq.WithConsumerPrefetchCount(6),
	)
	if err != nil {
		return err
	}

	for number := 1; number <= 6; number++ {
		if err := q.Publish(ctx, "numbered_job", number); err != nil {
			return err
		}
	}
	return nil
}
