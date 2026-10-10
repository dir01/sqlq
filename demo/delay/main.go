// Demonstrates delaying a job until its scheduled time.
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
		var reminder string
		if err := json.Unmarshal(payload, &reminder); err != nil {
			return err
		}
		log.Printf("reminder: %s", reminder)
		return nil
	}

	if err := q.Consume(ctx, "reminder", handler); err != nil {
		return err
	}

	log.Print("scheduling a reminder for five seconds from now")
	return q.Publish(ctx, "reminder", "check the oven",
		sqlq.WithDelay(5*time.Second),
	)
}
