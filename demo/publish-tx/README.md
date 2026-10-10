# Transactional publish

Inserts an order and publishes its job in one application-owned transaction. After commit, a consumer logs the order ID.

From the repository root, run:

```sh
go run ./demo/publish-tx
```

The application owns this transaction because it calls `db.BeginTx`; `PublishTx` does not commit it.

All SQLite demos share `demo/queue.db` when run from the repository root. The file is retained between runs. Press Ctrl-C to stop the queue and close the database.

See [main.go](main.go) for the complete program and [the shared lifecycle](../internal/demoapp/app.go) for connection and shutdown setup.
