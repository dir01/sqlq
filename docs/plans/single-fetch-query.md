# Plan: one fetch query for all consumers

Status: proposal. Nobody has started this work.

## Problem

Each consumer has its own fetch loop. Each loop sends one claim query for each poll.
The default poll delay is 100ms while a consumer finds jobs. Thus, a process with N busy job
types sends 10 × N queries each second. For example, 50 job types send 500 queries each second.
An idle consumer polls less frequently: each poll that finds no jobs makes the delay 10% longer,
to a maximum of 30s (see `defaultPollDelay`).

We do not know how many job types the users of the library have. If the number is large, the
polling causes a large load on the database.

For SQLite, the load is larger, because SQLite has only one writer. Each claim query is a write
transaction, and all write transactions of the process wait for the same lock (`d.lock`).

## Proposal

Replace the fetch loops of the consumers with one dispatcher for each queue:

1. For each tick, the dispatcher reads the capacity of each consumer
   (`prefetchCount + concurrency − len(claims)`) and its claim timeout.
2. The dispatcher sends one claim query for all job types that have a capacity of more than 0.
3. The dispatcher sends each claimed job to the jobs channel of its consumer.

### Postgres

Use `LATERAL`. Each job type has its own limit, and `SKIP LOCKED` continues to work in each part:

```sql
WITH wanted(job_type, n, claim_timeout) AS (VALUES ...)
UPDATE jobs SET ...
FROM (
  SELECT j.id, wanted.claim_timeout
  FROM wanted CROSS JOIN LATERAL (
    SELECT id FROM jobs
    WHERE job_type = wanted.job_type AND <ready>
    ORDER BY ... LIMIT wanted.n
    FOR UPDATE SKIP LOCKED
  ) j
) claimed
WHERE jobs.id = claimed.id
RETURNING ...
```

### SQLite

SQLite has no `LATERAL`. Use one subquery for each job type, with `UNION ALL`:

```sql
UPDATE jobs
SET consumed_at = ..., claim_token = ...,
    claim_expires_at = now + CASE job_type WHEN ? THEN ? WHEN ? THEN ? END
WHERE id IN (
  SELECT id FROM (SELECT id FROM jobs WHERE job_type = ? AND <ready> ORDER BY ... LIMIT ?)
  UNION ALL
  SELECT id FROM (SELECT id FROM jobs WHERE job_type = ? AND <ready> ORDER BY ... LIMIT ?)
)
RETURNING ...
```

Each subquery uses the index of its job type and stops at its limit. Thus, a large backlog of
one job type does not make the query slower. Do not use a window function
(`ROW_NUMBER() OVER (PARTITION BY job_type)`): it reads all ready rows of each job type before
it applies the limit.

SQLite does not need `SKIP LOCKED`, because the driver serializes the writes with `d.lock`.

## Analysis

### Points that are not problems

- **Capacity.** The dispatcher reads the capacity before the query. During the query, workers
  can only complete jobs, thus the capacity can only increase. The worst result is that the
  dispatcher claims fewer jobs than possible. The consumers have the same behavior now.
- **Shutdown.** It is not possible to stop only one consumer: `Shutdown` stops all consumers.
  The dispatcher releases its claims at shutdown, as each consumer does now.
- **Long jobs.** If the jobs of one type take a long time, that consumer has less capacity.
  At the next tick, the dispatcher claims fewer jobs of that type.
- **Query time.** Each part of the query is an index lookup with a small limit. A slow part
  delays the jobs of all types, but in practice each part takes milliseconds.

### Costs

- **API change.** One dispatcher has one poll delay function. `WithConsumerPollDelayFunc`
  must be removed. Only the queue-level poll delay function (`WithDefaultPollDelayFunc`)
  stays. The dispatcher must also count the polls that find no jobs for all job types together.
- **Less error isolation.** Most fetch errors are database errors (lost connection, timeout,
  `SQLITE_BUSY`). Such errors stop all consumers also now. But an error that occurs for only one
  job type, for example a row that the driver cannot read, stops all job types.
- **Dynamic SQL.** The query changes with the set of job types that have capacity. The code
  must build the SQL, and each set of job types is a different statement. The tests must
  examine sets of 1, 2 and many job types.
- **Registration while the dispatcher runs.** `Consume` can add a consumer at any time.
  The dispatcher must add the new job type at the next tick.
- **Fetch retry backoff.** The dispatcher has one backoff for all job types, instead of one
  for each consumer (see `fetchRetryBackoff`).
- **Async push.** With SQLite, a push is for one job type. The dispatcher can fetch all job
  types or only the type of the push. Both are possible.

## Alternatives

These alternatives also decrease the idle polling, and the consumers stay independent:

1. **Async push for Postgres with `LISTEN/NOTIFY`.** Now `PostgresDriver.subscribeForConsumer`
   returns `nil`. With a push, an idle consumer gets new jobs immediately, also when its
   poll delay is long.
2. **Backoff when there are no jobs.** Done: the default poll delay function
   (`defaultPollDelay`) makes the delay longer after each poll that finds no jobs.

## Recommendation

Do this work in a separate PR, after the current stack. The default poll delay function
already decreases the load of idle consumers. Thus, first measure if the remaining load of busy
consumers is a problem.
