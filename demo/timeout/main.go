package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/dir01/sqlq"
	"github.com/dir01/sqlq/demo/internal/demoapp"
)

func main() {
	demoapp.RunSQLite(runExample)
}

func runExample(ctx context.Context, _ *sql.DB, q sqlq.JobsQueue) error {
	handler := func(ctx context.Context, _ *sql.Tx, _ []byte) error {
		work := time.NewTimer(5 * time.Second)
		defer work.Stop()

		select {
		case <-work.C:
			log.Print("work completed")
			return nil
		case <-ctx.Done():
			log.Printf("work stopped: %v", ctx.Err())
			return ctx.Err()
		}
	}

	err := q.Consume(ctx, "slow_job", handler,
		sqlq.WithConsumerJobTimeout(time.Second),
		sqlq.WithConsumerMaxRetries(0),
	)
	if err != nil {
		return err
	}
	return q.Publish(ctx, "slow_job", "takes too long")
}
