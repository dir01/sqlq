# Runnable demos

Run each demo from the repository root with `go run ./demo/<name>`. Each subdirectory has its own setup and behavior notes. SQLite demos share `demo/queue.db`; PostgreSQL requires `DATABASE_URL`.

- [Basic SQLite queue](basic-sqlite/README.md)
- [PostgreSQL queue](postgres/README.md)
- [Structured payload](structured-payload/README.md)
- [Delayed job](delay/README.md)
- [Retries and job information](retry/README.md)
- [Concurrent processing](concurrency/README.md)
- [Handler timeout](timeout/README.md)
- [Inspect dead-letter jobs](inspect-dlq/README.md)
- [Requeue a failed job](requeue-dlq/README.md)
- [Dead-letter hook](dead-letter-hook/README.md)
- [SQLite push hints](sqlite-push/README.md)
- [Retention and cleanup](cleanup/README.md)
- [Transactional publish](publish-tx/README.md)
