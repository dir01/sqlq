// Demonstrates recording terminal failures in the dead-letter transaction.
package main

import (
	"context"
	"database/sql"
	"errors"

	"github.com/dir01/sqlq"
	"github.com/dir01/sqlq/demo/internal/demoapp"
)

func main() {
	demoapp.RunSQLite(runExample)
}

func runExample(ctx context.Context, db *sql.DB, q sqlq.JobsQueue) error {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS failed_tasks (
			job_id INTEGER PRIMARY KEY,
			reason TEXT NOT NULL
		)
	`)
	if err != nil {
		return err
	}

	handler := func(_ context.Context, _ *sql.Tx, _ []byte) error {
		return errors.New("this task cannot be completed")
	}

	onDeadLetter := func(ctx context.Context, tx *sql.Tx, info sqlq.JobInfo,
		_ []byte, handlerErr error) error {
		_, insertErr := tx.ExecContext(ctx,
			"INSERT INTO failed_tasks (job_id, reason) VALUES (?, ?)",
			info.ID, handlerErr.Error(),
		)
		return insertErr
	}

	err = q.Consume(ctx, "terminal_task", handler,
		sqlq.WithConsumerMaxRetries(0),
		sqlq.WithConsumerOnDeadLetter(onDeadLetter),
	)
	if err != nil {
		return err
	}
	return q.Publish(ctx, "terminal_task", "will fail")
}
