# Basic SQLite queue

Publishes a JSON string under `greeting`; its handler logs `received: hello`.

From the repository root, run:

```sh
go run ./demo/basic-sqlite
```

Leave the process running until the message appears, then press Ctrl-C.

All SQLite demos share `demo/queue.db` when run from the repository root. The file is retained between runs. Press Ctrl-C to stop the queue and close the database.

See [main.go](main.go) for the complete program and [the shared lifecycle](../internal/demoapp/app.go) for connection and shutdown setup.
