# SQL Queue

`sqlq` is a SQL-backed job queue for Go applications, with SQLite and PostgreSQL
backends. Publish JSON payloads under a job type, register a handler for that
type, and let background workers fetch and process them.

It supports delayed jobs, configurable worker concurrency, retries with backoff,
handler timeouts, panic recovery, a dead-letter queue (DLQ), transactional
dead-letter hooks, expiring claims with worker-loss recovery, automatic cleanup,
and OpenTelemetry tracing.

See [Current behavior and limitations](#current-behavior-and-limitations) for
claim ownership, transactional completion, and at-least-once execution semantics.

## Installation

Use Go 1.26 or newer. Install the queue and a database driver:

```sh
go get github.com/dir01/sqlq

# SQLite (requires CGO and a C compiler):
go get github.com/mattn/go-sqlite3

# Or PostgreSQL; these examples use pgx v5:
go get github.com/jackc/pgx/v5/stdlib
```

## How the queue runs

1. Your application opens a `*sql.DB` and passes it to `sqlq.New`.
2. `Run` creates the queue tables and indexes. It does not start workers.
3. `Consume` registers a handler and immediately starts workers for its job type.
4. `Publish` JSON-encodes a payload and inserts a job. Publishing does not wait
   for the handler to finish.
5. `Shutdown` cancels consumers and waits for their goroutines. Your application
   closes the database afterward.

There can be one `Consume` registration per job type on a queue instance.
Registering the same type twice returns `ErrDuplicateConsumer`. Increase the
consumer's concurrency to run more workers. Separate instances using the same
database compete for jobs; they do not each receive a copy.

Transaction ownership determines who commits:

| Where `tx` comes from | Who commits or rolls it back? |
| --- | --- |
| The argument passed to a consumer handler | The queue. The handler must never call `Commit` or `Rollback`, including in a `defer`. |
| The argument passed to a dead-letter hook | The queue. The hook must never call `Commit` or `Rollback`, including in a `defer`. |
| Your application's own `db.BeginTx` call | Your application, including when passing that transaction to `PublishTx`. |

In a handler or hook, use the supplied `tx` for database work and return `nil`
on success or an error on failure. The queue completes the transaction after
the callback returns. Returning `nil` permits a commit; cancellation or a
later database error can still fail the attempt.

Calling `PublishTx(ctx, tx, ...)` does not transfer ownership. When you pass a
handler's or hook's transaction, let the queue commit the new job with the
other writes. Do not commit it yourself. A separate transaction you create
inside a callback belongs to you, but its commits cannot be rolled back with
the queue's transaction.

If a callback commits the supplied transaction early, its writes are already
permanent. The queue's subsequent operations fail with `sql.ErrTxDone`, and
retry handling may run the callback again. The queue cannot undo that early
commit or preserve atomic completion in that case.

## 1. Start a SQLite queue

Open a database, start the queue, register a handler, then publish. `Publish`
returns after insertion, not after processing. Shut down the queue before closing
the database.

```go
q, _ := sqlq.New(db, sqlq.DBTypeSQLite)
q.Run()
q.Consume(ctx, "greeting", handleGreeting)
q.Publish(ctx, "greeting", "hello")
// On shutdown: q.Shutdown(), then db.Close()
```

Handlers receive JSON bytes. Return `nil` for success or an error for retry/DLQ
handling. `Run` creates the schema but reports schema errors only through tracing.
[Full SQLite demo](demo/basic-sqlite/README.md).

## 2. Use PostgreSQL

The queue calls stay the same. Open a `pgx` SQL connection and select the
PostgreSQL backend.

```go
db, _ := sql.Open("pgx", os.Getenv("DATABASE_URL"))
q, _ := sqlq.New(db, sqlq.DBTypePostgres)
```

Point `DATABASE_URL` at an existing database. PostgreSQL uses `SKIP LOCKED`
when workers claim jobs. [Full PostgreSQL demo](demo/postgres/README.md).

## 3. Publish a structured payload

`Publish` JSON-encodes the value; the handler decodes it into the same shape.

```go
type WelcomeEmail struct { Address, Name string }
q.Publish(ctx, "welcome_email", WelcomeEmail{"alex@example.com", "Alex"})
// Handler: json.Unmarshal(payload, &email)
```

Register one handler for each job type.
[Full structured-payload demo](demo/structured-payload/README.md).

## 4. Delay a job

```go
q.Publish(ctx, "reminder", "check the oven", sqlq.WithDelay(5*time.Second))
```

The delay is the earliest eligible time; polling and worker availability can
make execution later. [Full delay demo](demo/delay/README.md).

## 5. Retry a failure and inspect the attempt

```go
q.Consume(ctx, "retry_demo", handler,
    sqlq.WithConsumerMaxRetries(2),
    sqlq.WithConsumerBackoffFunc(func(uint16) time.Duration { return time.Second }))
// In handler: info, _ := sqlq.JobInfoFromContext(ctx)
// info.RetryCount is 0 on the first attempt.
```

Two retries allow three attempts total. A failure on `info.IsFinalAttempt()`
moves the job to the DLQ; success completes it. Panics use the same failure
path. [Full retry demo](demo/retry/README.md).

## 6. Process jobs concurrently

```go
q.Consume(ctx, "numbered_job", handler,
    sqlq.WithConsumerConcurrency(3),
    sqlq.WithConsumerPrefetchCount(6))
```

Concurrency caps active handler calls. Prefetch sets the maximum fetch batch and
buffer capacity, and is raised to at least concurrency. Total outstanding claims
are bounded by concurrency plus prefetch, so the buffer can stay supplied while
workers run without an additional batch waiting outside the channel.
[Full concurrency demo](demo/concurrency/README.md).

### Expiring claims and recovery

```go
q.Consume(ctx, "numbered_job", handler,
    sqlq.WithConsumerJobTimeout(10*time.Minute),
    sqlq.WithConsumerClaimTimeout(30*time.Minute))
```

Claims start at fetch time, including time in the local buffer. Another consumer
can recover unfinished jobs after their claims expire. Recovery does not increment
the retry count: a prefetched job may never have started. Polling discovers expired
claims even without a new publish notification.

The claim timeout defaults to twice the job timeout and must be longer than it;
otherwise `Consume` returns `ErrClaimTimeoutTooShort`. Before a worker starts a
handler, it checks a conservative local deadline. If less than the job timeout plus
a margin (half the job timeout, at most one minute) remains, it extends the claim
using its token. Fresh claims need no additional database query. Expired local
copies are discarded; a failed ownership check prevents the handler from starting.

Claims are not renewed while a handler runs, and claim expiry does not cancel it.
The job timeout does, and cancellation is cooperative: a handler that ignores its
context can outlive its claim. Expiry permits reclamation; changing the token
invalidates the previous attempt. Completion can still succeed after expiry if
no other consumer has replaced the token. SQLite's write transaction prevents
competing claim updates while its write protection is held.

Shutdown cancels active handlers, waits for their transactions, and attempts to
release all outstanding claims with a five-second cleanup budget. Expiry is the
fallback after process loss or a failed release. Cancellation remains cooperative.

## 7. Stop work at its deadline

```go
q.Consume(ctx, "slow_job", handler,
    sqlq.WithConsumerJobTimeout(time.Second),
    sqlq.WithConsumerMaxRetries(0))
// Handler: select on ctx.Done() while doing cancellable work.
```

Timeouts cancel the handler context; they cannot interrupt work that ignores
it. A handler returning `nil` after its deadline still fails.
[Full timeout demo](demo/timeout/README.md).

## 8. Inspect dead-letter jobs

```go
jobs, _ := q.GetDeadLetterJobs(ctx, "slow_job", 10)
// Each job includes OriginalID, RetryCount, FailedAt, FailureReason, Payload.
```

An empty type selects all types. Results are newest first; a zero limit returns
no rows. [Full DLQ inspection demo](demo/inspect-dlq/README.md).

## 9. Requeue a failure

```go
jobs, _ := q.GetDeadLetterJobs(ctx, "slow_job", 1)
q.RequeueDeadLetterJob(ctx, jobs[0].OriginalID)
```

Fix the handler first. Requeue removes the DLQ row and inserts a new pending
job with a new ID, zero retries, and no inherited trace context. A missing row
returns `ErrJobNotFound`. [Full requeue demo](demo/requeue-dlq/README.md).

## 10. Record terminal failure atomically

A dead-letter hook shares the transaction that moves the job into the DLQ.

```go
onDeadLetter := func(ctx context.Context, tx *sql.Tx,
    info sqlq.JobInfo, payload []byte, cause error) error {
    _, err := tx.ExecContext(ctx, "INSERT INTO failed_tasks ...", info.ID)
    return err
}
q.Consume(ctx, "terminal_task", handler,
    sqlq.WithConsumerMaxRetries(0),
    sqlq.WithConsumerOnDeadLetter(onDeadLetter))
```

Return an error to roll back the hook's writes and DLQ move. Never commit or
roll back its supplied `tx`. On SQLite, use that `tx` for writes inside the
hook; queue methods such as `Publish` can deadlock there.
[Full dead-letter hook demo](demo/dead-letter-hook/README.md).

## 11. Wake a SQLite consumer on publish

```go
q.Consume(ctx, "notification", handler,
    sqlq.WithAsyncPush(),
    sqlq.WithAsyncPushRateLimit(60))
```

Push hints work only within one SQLite driver instance. Polling still handles
delays, retries, requeues, transactional publications, missed hints, and
external writes. PostgreSQL returns `ErrPushNotSupported`. The current poll
option spelling is `WithConsumerPollInteval`.
[Full SQLite push demo](demo/sqlite-push/README.md).

## 12. Choose retention periods

```go
q.Consume(ctx, "report", handler,
    sqlq.WithConsumerCleanupProcessedAge(24*time.Hour),
    sqlq.WithConsumerCleanupBatch(100),
    sqlq.WithConsumerCleanupDLQInterval(0)) // Disable DLQ cleanup.
```

Cleanup runs only for registered job types. A nonpositive interval disables a
cleanup loop; zero age does not.
[Full cleanup demo](demo/cleanup/README.md).

## 13. Save an order and publish its job together

```go
tx, _ := db.BeginTx(ctx, nil)
defer tx.Rollback()
orderID := insertOrder(ctx, tx)
q.PublishTx(ctx, tx, "process_order", orderID)
tx.Commit()
```

`PublishTx` uses your transaction and never finishes it. If you created it,
you commit or roll it back. A handler's or hook's supplied transaction belongs
to the queue. The job is visible after commit; SQLite discovers transactional
publications through polling. [Full transaction demo](demo/publish-tx/README.md).

## Configuration reference

`sqlq.New` accepts `WithDefault...` options. `Consume` copies those defaults,
then applies `WithConsumer...` overrides. `Publish` and `PublishTx` accept
`WithDelay`. The starter sets concurrency and prefetch to one for clarity;
the library defaults are:

| Setting | Default | Queue option | Consumer option |
| --- | --- | --- | --- |
| Poll interval | 100 ms | `WithDefaultPollInterval` | `WithConsumerPollInteval` |
| Concurrency | min(NumCPU, GOMAXPROCS) | `WithDefaultConcurrency` | `WithConsumerConcurrency` |
| Prefetch | Consumer's concurrency | `WithDefaultPrefetchCount` | `WithConsumerPrefetchCount` |
| Maximum retries | 3 | `WithDefaultMaxRetries` | `WithConsumerMaxRetries` |
| Job timeout | 15 minutes | `WithDefaultJobTimeout` | `WithConsumerJobTimeout` |
| Claim timeout | 2 × job timeout (30 minutes) | `WithDefaultClaimTimeout` | `WithConsumerClaimTimeout` |
| Retry delay | Exponential backoff with jitter | `WithDefaultBackoffFunc` | `WithConsumerBackoffFunc` |
| Processed cleanup interval | 1 hour | `WithDefaultCleanupProcessedInterval` | `WithConsumerCleanupProcessedInterval` |
| Processed retention | 7 days after processing | `WithDefaultCleanupProcessedAge` | `WithConsumerCleanupProcessedAge` |
| DLQ cleanup interval | 6 hours | `WithDefaultCleanupDLQInterval` | `WithConsumerCleanupDLQInterval` |
| DLQ retention | 30 days after failure | `WithDefaultCleanupDLQAge` | `WithConsumerCleanupDLQAge` |
| Cleanup batch | 500 | `WithDefaultCleanupBatch` | `WithConsumerCleanupBatch` |

Consumer-only options include `WithConsumerOnDeadLetter`, `WithAsyncPush`, and
`WithAsyncPushRateLimit`. `WithTracer` is a queue option. Configure a global
OpenTelemetry tracer provider and text-map propagator to export traces and
carry publisher context into handlers; driver tracers use the global provider.
See `./sqlq_testutils_otel_test.go:17` for tracing setup used by the tests.

## Storage

`Run` creates two tables and their indexes using SQL embedded in the drivers.
It also idempotently adds claim columns to existing `jobs` tables. Stop old-version
consumers before upgrading: they do not enforce claim tokens. Legacy unprocessed
jobs without claim metadata are recoverable; processed jobs remain excluded.

| Table | Contents |
| --- | --- |
| `jobs` | `id`, `job_type`, JSON `payload`, `created_at`, `scheduled_at`, `retry_count`, `last_error`, `trace_context`, `consumed_at`, `claim_token`, `claim_expires_at`, `processed_at`. |
| `dead_letter_queue` | `original_job_id` (primary key), `job_type`, `payload`, `created_at`, `failed_at`, `retry_count`, `failure_reason`. |

A due job is claimed by setting `consumed_at`, a token, and an expiry. Success sets `processed_at`;
the row remains until cleanup. A retry increments the retry count, schedules
another attempt, and clears the claim. A DLQ move inserts the failure and
deletes the original job in a transaction. SQLite stores timestamps as Unix
milliseconds; PostgreSQL uses SQL timestamps.

Schema definitions: `./driver_sqlite.go:51` and `./driver_postgres.go:33`.

## Current behavior and limitations

These details describe the implementation in this checkout:

- **Handler writes and completion share a transaction.** Use the supplied `tx`
  for database writes, and let the queue commit or roll it back. Success commits
  those writes together with job completion. Handler errors, panics, timeouts,
  and completion-update failures roll back the attempt. Completion and commit
  errors enter the retry/DLQ path unless ownership has been lost. External side effects are outside this
  transaction and should be safe to repeat.
- **Execution is at least once.** Expiring claims recover abandoned jobs, including
  prefetched jobs. Completion, retry, release, and DLQ operations check ownership
  tokens; a stale attempt cannot settle a newer one. An execution that overlaps
  reclamation cannot commit its transaction-local writes if its completion token
  check fails. External effects need idempotency: local deadline checks cannot
  prevent a paused process from resuming after another worker has taken over.
- **Cancellation is cooperative.** Handler and DLQ hook contexts inherit shutdown
  cancellation. A callback that ignores its context can keep shutdown waiting.
- **Schema setup errors are visible only in tracing.** `Run` records schema
  errors without returning them. See `./sqlq.go:197`.

## Development

CI builds, lints, and tests on the latest patch releases of Go 1.26 and 1.27.
`make lint` uses the golangci-lint version recorded in `go.mod`.

`TestClaimsPostgres` runs the recovery/ownership suite in an isolated PostgreSQL
schema. It starts a Docker container by default, or can use an existing test server:

```sh
SQLQ_TEST_POSTGRES_DSN='postgres://user:password@localhost/testdb?sslmode=disable' \
    go test -race -run '^TestClaimsPostgres$' .
```

The shared behavior tests run against SQLite and PostgreSQL. The PostgreSQL
suite starts a database through Testcontainers and requires Docker. Integration
tests export traces to localhost:4318; the Makefile provides a Jaeger command.

```sh
make build
make start-jaeger
make test-short  # Race detector; skips PostgreSQL.
make test        # Race detector; includes PostgreSQL via Docker.
make lint
```

Public API: `./sqlq.go:23`. Consumer runtime: `./consumer.go:49`.
Dead-letter hook contract: `./options_consumer.go:149`.
Shared DLQ examples and tests: `./sqlq_testcase_dlq_test.go:287`.

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
