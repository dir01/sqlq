# Inspect dead-letter jobs

Prints up to ten `slow_job` failures from the DLQ.

From the repository root, run:

```sh
go run ./demo/inspect-dlq
```

Run the timeout demo first and wait for its failure if you want a row to inspect. This demo is safe to run with an empty DLQ.

All SQLite demos share `demo/queue.db` when run from the repository root. The file is retained between runs. Press Ctrl-C to stop the queue and close the database.

See [main.go](main.go) for the complete program and [the shared lifecycle](../internal/demoapp/app.go) for connection and shutdown setup.
