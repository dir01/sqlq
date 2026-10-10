# Handler timeout

Runs work that would take five seconds with a one-second handler deadline and no retries. The failed job goes to the DLQ.

From the repository root, run:

```sh
go run ./demo/timeout
```

This also seeds the shared SQLite demo database for the inspection and requeue demos.

All SQLite demos share `demo/queue.db` when run from the repository root. The file is retained between runs. Press Ctrl-C to stop the queue and close the database.

See [main.go](main.go) for the complete program and [the shared lifecycle](../internal/demoapp/app.go) for connection and shutdown setup.
