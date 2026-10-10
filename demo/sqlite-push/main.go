package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

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
		log.Printf("notification: %s", message)
		return nil
	}

	err := q.Consume(ctx, "notification", handler,
		sqlq.WithAsyncPush(),
		sqlq.WithAsyncPushRateLimit(60),
		sqlq.WithConsumerPollInteval(time.Second),
	)
	if err != nil {
		return err
	}
	return q.Publish(ctx, "notification", "new activity")
}
