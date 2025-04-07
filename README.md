# SQL Queue

A robust, database-backed job queue system for Go applications that supports SQLite, PostgreSQL, and MySQL.

## Features

- Simple API for publishing and consuming jobs
- Support for multiple database backends (SQLite, PostgreSQL, MySQL)
- Concurrent job processing with configurable worker pools
- Transaction support for atomic job publishing
- Durable storage with at-least-once delivery guarantees
- Configurable polling intervals and prefetch counts

## Installation

```bash
go get github.com/dir01/sqlqueue
```

## Quick Start

```go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"github.com/dir01/sqlqueue"
	_ "github.com/mattn/go-sqlite3"
)

type MyPayload struct {
	Message string `json:"message"`
	Count   int    `json:"count"`
}

func main() {
	// Open a database connection
	db, err := sql.Open("sqlite3", "file:queue.db?cache=shared")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a queue with 500ms polling interval
	queue, err := sqlqueue.NewSQLQueue(db, sqlqueue.DBTypeSQLite, 500*time.Millisecond)
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}

	// Start the queue
	queue.Run()
	defer queue.Shutdown()

	// Subscribe to jobs
	queue.Subscribe(
		context.Background(),
		"example_job",
		"example_consumer",
		func(ctx context.Context, payloadBytes []byte) error {
			var payload MyPayload
			if err := json.Unmarshal(payloadBytes, &payload); err != nil {
				return err
			}
			
			log.Printf("Received job: %s (count: %d)", payload.Message, payload.Count)
			return nil
		},
		// Optional: Configure concurrency and prefetch
		sqlqueue.WithConcurrency(5),
		sqlqueue.WithPrefetchCount(10),
	)

	// Publish a job
	err = queue.Publish(
		context.Background(),
		"example_job",
		MyPayload{Message: "Hello, World!", Count: 42},
	)
	if err != nil {
		log.Fatalf("Failed to publish job: %v", err)
	}

	// Keep the program running
	select {}
}
```

## Usage

### Creating a Queue

```go
// SQLite
db, _ := sql.Open("sqlite3", "file:queue.db?cache=shared")
queue, _ := sqlqueue.NewSQLQueue(db, sqlqueue.DBTypeSQLite, 1*time.Second)

// PostgreSQL
db, _ := sql.Open("postgres", "postgres://user:password@localhost/dbname?sslmode=disable")
queue, _ := sqlqueue.NewSQLQueue(db, sqlqueue.DBTypePostgres, 1*time.Second)

// MySQL
db, _ := sql.Open("mysql", "user:password@tcp(localhost:3306)/dbname")
queue, _ := sqlqueue.NewSQLQueue(db, sqlqueue.DBTypeMySQL, 1*time.Second)
```

### Publishing Jobs

```go
// Simple publish
err := queue.Publish(ctx, "job_type", payload)

// Publish within a transaction
tx, _ := db.BeginTx(ctx, nil)
err := queue.PublishTx(ctx, tx, "job_type", payload)
// ... other operations in the same transaction
tx.Commit()
```

### Subscribing to Jobs

```go
queue.Subscribe(
    ctx,
    "job_type",
    "consumer_name",
    func(ctx context.Context, payloadBytes []byte) error {
        // Process the job
        return nil
    },
    // Optional configuration
    sqlqueue.WithConcurrency(10),
    sqlqueue.WithPrefetchCount(20),
)
```

## Configuration Options

### Subscription Options

- `WithConcurrency(n int)`: Sets the number of concurrent workers for a subscription (default: number of CPUs)
- `WithPrefetchCount(n int)`: Sets the number of jobs to prefetch in a single query (default: same as concurrency)

## Database Schema

The system uses two tables:

1. `jobs` - Stores job information:
   - `id` - Primary key
   - `job_type` - Type of job
   - `payload` - JSON-encoded job data
   - `created_at` - Timestamp when the job was created

2. `job_consumers` - Tracks which consumers have processed which jobs:
   - `job_id` - Foreign key to jobs table
   - `consumer_name` - Name of the consumer
   - `processed_at` - Timestamp when the job was processed

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
