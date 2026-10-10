# Structured payload

Publishes a `WelcomeEmail` struct and decodes it in the handler. The handler only logs the message; it does not send email.

From the repository root, run:

```sh
go run ./demo/structured-payload
```

Press Ctrl-C after the welcome message appears.

All SQLite demos share `demo/queue.db` when run from the repository root. The file is retained between runs. Press Ctrl-C to stop the queue and close the database.

See [main.go](main.go) for the complete program and [the shared lifecycle](../internal/demoapp/app.go) for connection and shutdown setup.
