# Dead-letter hook

Creates `failed_tasks`, then records an intentional terminal failure in the same transaction as the DLQ move.

From the repository root, run:

```sh
go run ./demo/dead-letter-hook
```

The hook uses its supplied transaction and returns its insert error. It never commits or rolls back that transaction.

All SQLite demos share `demo/queue.db` when run from the repository root. The file is retained between runs. Press Ctrl-C to stop the queue and close the database.

See [main.go](main.go) for the complete program and [the shared lifecycle](../internal/demoapp/app.go) for connection and shutdown setup.
