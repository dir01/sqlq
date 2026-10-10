// Demonstrates publishing and consuming a job with SQLite.
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
		log.Printf("received: %s", message)
		return nil
	}

	if err := q.Consume(ctx, "greeting", handler); err != nil {
		return err
	}
	return q.Publish(ctx, "greeting", "hello")
}
