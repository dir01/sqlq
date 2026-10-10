# PostgreSQL queue

Runs the same greeting flow against an existing PostgreSQL database.

From the repository root, run:

```sh
export DATABASE_URL='postgres://postgres:postgres@localhost:5432/queue?sslmode=disable'
go run ./demo/postgres
```

Set `DATABASE_URL` to a PostgreSQL DSN before running. The database itself must already exist.

Press Ctrl-C to stop the queue and close the database.

See [main.go](main.go) for the complete program and [the shared lifecycle](../internal/demoapp/app.go) for connection and shutdown setup.
