// Package demoapp provides the common lifecycle for the runnable queue demos.
package demoapp

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dir01/sqlq"
	_ "github.com/jackc/pgx/v5/stdlib" // Register the PostgreSQL driver for the demos.
	_ "github.com/mattn/go-sqlite3"    // Register the SQLite driver for the demos.
)

// Example sets up a demo's jobs and consumers.
type Example func(context.Context, *sql.DB, sqlq.JobsQueue) error

// RunSQLite runs an example using a local SQLite database until interrupted.
func RunSQLite(example Example) {
	if err := run("sqlite3", "file:demo/queue.db?_busy_timeout=5000&_journal_mode=WAL", sqlq.DBTypeSQLite, example); err != nil {
		log.Fatal(err)
	}
}

// RunPostgres runs an example using DATABASE_URL until interrupted.
func RunPostgres(example Example) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}
	if err := run("pgx", dsn, sqlq.DBTypePostgres, example); err != nil {
		log.Fatal(err)
	}
}

func run(driver, dsn string, dbType sqlq.DBType, example Example) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("close database: %v", closeErr)
		}
	}()
	if err = db.PingContext(ctx); err != nil {
		return err
	}

	q, err := sqlq.New(db, dbType,
		sqlq.WithDefaultConcurrency(1),
		sqlq.WithDefaultPrefetchCount(1),
	)
	if err != nil {
		return err
	}
	q.Run()
	defer q.Shutdown()

	if err = example(ctx, db, q); err != nil {
		return err
	}
	log.Print("queue running; press Ctrl-C to stop")
	<-ctx.Done()
	return nil
}
