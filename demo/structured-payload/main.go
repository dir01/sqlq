// Demonstrates publishing and decoding a structured JSON payload.
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

type WelcomeEmail struct {
	Address string `json:"address"`
	Name    string `json:"name"`
}

func runExample(ctx context.Context, _ *sql.DB, q sqlq.JobsQueue) error {
	handler := func(_ context.Context, _ *sql.Tx, payload []byte) error {
		var email WelcomeEmail
		if err := json.Unmarshal(payload, &email); err != nil {
			return err
		}
		log.Printf("welcome %s; recipient=%s", email.Name, email.Address)
		return nil
	}

	if err := q.Consume(ctx, "welcome_email", handler); err != nil {
		return err
	}

	email := WelcomeEmail{Address: "alex@example.com", Name: "Alex"}
	return q.Publish(ctx, "welcome_email", email)
}
