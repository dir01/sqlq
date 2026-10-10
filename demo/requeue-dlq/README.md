# Requeue a failed job

Registers a fixed `slow_job` handler, then requeues the newest failed `slow_job`.

From the repository root, run:

```sh
go run ./demo/requeue-dlq
```

Run the timeout demo first and wait for its failure. This demo reports that there is nothing to requeue when the DLQ is empty.

All SQLite demos share `demo/queue.db` when run from the repository root. The file is retained between runs. Press Ctrl-C to stop the queue and close the database.

See [main.go](main.go) for the complete program and [the shared lifecycle](../internal/demoapp/app.go) for connection and shutdown setup.
