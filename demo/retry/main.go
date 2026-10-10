// Demonstrates retrying a job that succeeds on its third attempt.
package main

import (
	"context"
	"database/sql"
	"errors"
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
		info, ok := sqlq.JobInfoFromContext(ctx)
		if !ok {
			return errors.New("job metadata is missing")
		}

		log.Printf("job=%d attempt=%d final=%t",
			info.ID, int(info.RetryCount)+1, info.IsFinalAttempt())
		if info.RetryCount < 2 {
			return errors.New("temporary failure for this demo")
		}

		log.Print("third attempt succeeded")
		return nil
	}

	err := q.Consume(ctx, "retry_demo", handler,
		sqlq.WithConsumerMaxRetries(2),
		sqlq.WithConsumerBackoffFunc(func(_ uint16) time.Duration {
			return time.Second
		}),
	)
	if err != nil {
		return err
	}
	return q.Publish(ctx, "retry_demo", "try again")
}
