// Demonstrates publishing a job in the same transaction as an order.
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

func runExample(ctx context.Context, db *sql.DB, q sqlq.JobsQueue) error {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS orders (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			description TEXT NOT NULL
		)
	`)
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }() // Harmless after a successful commit.

	result, err := tx.ExecContext(ctx,
		"INSERT INTO orders (description) VALUES (?)", "a new order")
	if err != nil {
		return err
	}
	orderID, err := result.LastInsertId()
	if err != nil {
		return err
	}

	if err := q.PublishTx(ctx, tx, "process_order", orderID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	handler := func(_ context.Context, _ *sql.Tx, payload []byte) error {
		var orderID int64
		if err := json.Unmarshal(payload, &orderID); err != nil {
			return err
		}
		log.Printf("processing committed order %d", orderID)
		return nil
	}
	return q.Consume(ctx, "process_order", handler)
}
