# Delayed job

Publishes a reminder eligible for processing five seconds later.

From the repository root, run:

```sh
go run ./demo/delay
```

The handler may run later than five seconds because workers poll the database.

All SQLite demos share `demo/queue.db` when run from the repository root. The file is retained between runs. Press Ctrl-C to stop the queue and close the database.

See [main.go](main.go) for the complete program and [the shared lifecycle](../internal/demoapp/app.go) for connection and shutdown setup.
