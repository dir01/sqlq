# Retries and job information

Fails twice, then succeeds on the third attempt. Logs the stored retry count and whether each attempt is final.

From the repository root, run:

```sh
go run ./demo/retry
```

The custom backoff waits one second between attempts.

All SQLite demos share `demo/queue.db` when run from the repository root. The file is retained between runs. Press Ctrl-C to stop the queue and close the database.

See [main.go](main.go) for the complete program and [the shared lifecycle](../internal/demoapp/app.go) for connection and shutdown setup.
