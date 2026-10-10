# SQLite push hints

Publishes a notification and enables local push hints for its consumer.

From the repository root, run:

```sh
go run ./demo/sqlite-push
```

The one-second poll interval remains active as a fallback.

All SQLite demos share `demo/queue.db` when run from the repository root. The file is retained between runs. Press Ctrl-C to stop the queue and close the database.

See [main.go](main.go) for the complete program and [the shared lifecycle](../internal/demoapp/app.go) for connection and shutdown setup.
