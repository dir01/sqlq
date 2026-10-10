# Retention and cleanup

Publishes a report and configures hourly deletion of completed jobs older than a day, in batches of 100. DLQ cleanup is disabled.

From the repository root, run:

```sh
go run ./demo/cleanup
```

Cleanup only runs while this consumer is active; it does not affect other job types.

All SQLite demos share `demo/queue.db` when run from the repository root. The file is retained between runs. Press Ctrl-C to stop the queue and close the database.

See [main.go](main.go) for the complete program and [the shared lifecycle](../internal/demoapp/app.go) for connection and shutdown setup.
