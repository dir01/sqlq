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
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
)

type Example func(context.Context, *sql.DB, sqlq.JobsQueue) error

func RunSQLite(example Example) {
	run("sqlite3", "file:demo/queue.db?_busy_timeout=5000&_journal_mode=WAL", sqlq.DBTypeSQLite, example)
}

func RunPostgres(example Example) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}
	run("pgx", dsn, sqlq.DBTypePostgres, example)
}

func run(driver, dsn string, dbType sqlq.DBType, example Example) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open(driver, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal(err)
	}

	q, err := sqlq.New(db, dbType,
		sqlq.WithDefaultConcurrency(1),
		sqlq.WithDefaultPrefetchCount(1),
	)
	if err != nil {
		log.Fatal(err)
	}
	q.Run()
	defer q.Shutdown()

	if err := example(ctx, db, q); err != nil {
		log.Fatal(err)
	}
	log.Print("queue running; press Ctrl-C to stop")
	<-ctx.Done()
}
