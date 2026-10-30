# Real-database simulation and chaos testing

Ordinary Go tests calculate a factorial using a database-backed product tree,
inject faults into real consumer subprocesses, restore healthy operation, and
check every intermediate result and exactly-once **committed** effects. Repeated
handler attempts are expected. There is no application deduplication to hide
duplicate commits.

The tests use the production queue and drivers without replacing their clocks
or transactions. The tests run natively on Linux/macOS with real time.

Everything for this setup lives in this separate Go module:

```text
tests/chaos/
├── go.mod / go.sum        # depends on the local sqlq library via replace ../..
├── internal/chaos/       # ordinary Go simulation tests and shared fault/oracle helpers
├── cmd/sqlq-chaos/        # optional standalone workload/supervisor
└── README.md / Makefile
```

Commands below run from the repository root. `go -C tests/chaos` runs in the
test module; relative artifact paths resolve inside `tests/chaos`. Absolute paths
can be used to store outputs elsewhere.

## Quick start

This module requires Go 1.27+, CGO, and a C compiler. The library's minimum Go
version remains 1.26. From the repository root:

```sh
make test-simulation-sqlite
make test-simulation-psql  # Docker must be running; no manual database setup

# Explore additional reproducible workloads/configurations:
make test-simulation-sqlite SIMULATION_ARGS='-simulation-seed 42 -simulation-seeds 3'
make test-simulation-psql SIMULATION_ARGS='-simulation-seed 42 -simulation-seeds 3'

# Keep exploring consecutive seeds for 20 minutes:
make test-simulation-sqlite SIMULATION_TIMEOUT=25m SIMULATION_ARGS='-simulation-duration 20m'
make test-simulation-psql SIMULATION_TIMEOUT=25m SIMULATION_ARGS='-simulation-duration 20m'

# Larger integers with roughly 2,000 logical jobs:
make test-simulation-sqlite SIMULATION_ARGS='-simulation-n 50000 -simulation-max-range 100 -simulation-chaos 3s'
```

Both targets run `go test -race -count=1` and retain diagnostics under
`tests/chaos/simulation-artifacts/`. Each run has fresh artifact directories.
The SQLite target first runs the library's deterministic readiness gates, then
starts the timed seed exploration. The requested exploration duration excludes
those readiness checks (including the 15-second process-pause/claim-expiry test).
Set `SIMULATION_OUTPUT=/absolute/path` to change the output location.
`-simulation-duration` overrides the fixed seed count and stops starting new
profiles when the duration expires. It finishes recovery/verification/shutdown
for the active profile, so successful completion may take a little longer than
the requested duration. The exploration timer starts after PostgreSQL container
setup. `SIMULATION_TIMEOUT` is Go's hard test-binary timeout (default 5 minutes);
exceeding it aborts the test with a failure. Allow headroom for startup and final
checks, as in the 20-minute examples. Compilation/image downloads are not a
strictly bounded end-to-end shell runtime.
The PostgreSQL target starts one disposable `postgres:17-alpine` Testcontainer,
reuses it across all profiles/seeds, and automatically terminates it on cleanup.
Each profile has a separate database schema. SQLite uses a fresh on-disk WAL
database per profile and needs no Docker.

Each seed runs three profiles: healthy processing, finite retries with DLQ
recovery, and unlimited retries with processed-job cleanup (plus SQLite push
registration). Seeds vary the graph, process/concurrency counts, prefetch, pool
size, lock timeout and fault choices. The default workload is `1000!` with leaf
ranges up to 8, and each chaos profile injects faults for 1.5 seconds. Job timeouts
are 50–125 ms. The queue calculates the claim from the job timeout: the job
timeout plus half of it. Workers poll with a fixed delay. Workers' `TZ` and the PostgreSQL
session `TimeZone` are each one of UTC, America/Los_Angeles, Asia/Tokyo,
America/St_Johns or Pacific/Chatham. Independent recovery, verification
and shutdown budgets bound completion. These are real-time tests, not simulated
clocks, and seeds do not reproduce OS scheduling.

`simulation_test.go` is the entry point. Workers are subprocesses of the same
compiled Go test binary, selected by `TestWorkerProcess`. This keeps real
SIGKILL/SIGSTOP behavior and race instrumentation without a worker script,
separately built executable. Shared
Go helpers retain the supervisor, TCP fault proxy, transactional workload and
independent oracle. PostgreSQL TCP faults affect only worker connections through
the test's own proxy. SQLite integrity/foreign-key checks, goroutine leak checks
and bounded SQL pool drainage remain enabled.

On Linux, worker launches use `/proc/self/exe` so a long exploration can keep
restarting workers even if an external temporary-directory cleanup removes the
original `go test` executable pathname. A regression test unlinks a disposable
copy of the running test binary and verifies that worker execution still works.

For direct Go invocation:

```sh
go -C tests/chaos test -race -count=1 -v -run '^TestSimulationSQLite$' ./internal/chaos
go -C tests/chaos test -race -count=1 -v -run '^TestSimulationPSQL$' ./internal/chaos
```

Direct invocations retain artifacts only when passed Go's `-artifacts` flag;
the Make targets enable it automatically. Short mode skips the Docker-backed
PostgreSQL simulation; `make test-chaos` and `make test-chaos-checks` use short
mode to run SQLite and harness regressions without Docker.

### Optional standalone runner

For explicit configuration/budget controls outside `go test`, use the CLI. Choose
a fresh artifacts directory for every CLI run; it refuses to overwrite one.

```sh
go -C tests/chaos run ./cmd/sqlq-chaos -seed 17 -artifacts chaos-artifacts-17

# Seeded configuration generation; explicit flags override generated options:
go -C tests/chaos run ./cmd/sqlq-chaos -seed 18 -random-config -artifacts chaos-artifacts-18

# Full factorial, longer exploration and a separate recovery deadline:
go -C tests/chaos run ./cmd/sqlq-chaos -n 50000 -max-range 100 -seed 42 \
  -chaos 30s -recovery 2m -artifacts chaos-artifacts-42

# No deliberate faults; useful for debugging the workload itself:
go -C tests/chaos run ./cmd/sqlq-chaos -chaos 0 -artifacts chaos-artifacts-healthy

# PostgreSQL: use TCP and sslmode=disable for the local chaos proxy.
go -C tests/chaos run ./cmd/sqlq-chaos -driver postgres \
  -dsn 'postgres://postgres@127.0.0.1:5432/postgres?sslmode=disable' \
  -seed 43 -artifacts chaos-artifacts-pg

go -C tests/chaos run ./cmd/sqlq-chaos -h
```

PostgreSQL runs create a uniquely named schema and retain it for investigation.
Its name is in `config.json`; drop that specific schema when finished. SQLite
runs create their own WAL database in the artifacts directory. Config files can
contain database credentials and are written with mode `0600` beneath a `0700`
directory.

### Configurations

Use `-processes`, `-workers`, `-prefetch`, `-pool`, `-lock-timeout`, `-poll`, `-job-timeout`,
`-backoff`, `-retries`, `-push`, `-push-rpm`, and `-cleanup` to
explore configurations, and `-app-timezone` and `-db-timezone` for time zones.
`-retries -1` means unlimited retries. The job timeout must be at least 50 ms.
DLQ cleanup runs every fifty polls with a one-hour retention: required work must
remain available for recovery, and nothing in a run is that old. Processed cleanup uses small batches and
retains the independent execution ledger.

`-random-config` generates process/worker counts, prefetch, pool, timing, retry,
push, lock-wait timeout, cleanup, time-zone and shutdown-style settings from the seed. Explicit overrides can produce an
invalid timeout relationship, in which case validation rejects the run.

The workload graph, initial job delays, and fault choices are seeded. Timing and
OS scheduling are not deterministic. Reproduce the same command and seed to
replay a workload, and consult the saved effective configuration. SQLite push
registration/rate-limit options are exercised, but publication is transactional
or from another process, so this workload primarily exercises polling fallback;
it does not prove successful same-instance push delivery.

## What is checked

Leaf jobs calculate products of variable-sized contiguous ranges. Reduction jobs
read two committed child products and multiply them. A transactional coordination
row serializes dependency-readiness checks so simultaneous child completion
publishes their parent once. Computation results, an append-only execution ledger,
and downstream `PublishTx` publication share the handler transaction.

After chaos ends the supervisor resets the network proxy, replaces consumers with
healthy ones, and requeues DLQ jobs through the public API. It does not manufacture
missing work or edit results. A separate recovery deadline bounds convergence.

Verification has its own `-verification` budget, separate from recovery. Phase
and periodic progress records show whether the run is still processing jobs or
checking the result. Summaries retain the last known completed/pending/DLQ counts
and elapsed recovery/verification times, including on deadline failures.

The final oracle requires:

- Exactly one committed ledger entry for every expected logical task.
- No unexpected or failed-hook effects.
- Every intermediate product matches an independent `math/big.MulRange` result.
- The root matches the independent factorial.
- No unprocessed jobs, transactional poison jobs, or DLQ jobs remain, but for the
  kept job, which must still be in the DLQ (below).
- In fault-free runs, no audit job failed for any reason other than a timeout.
- On SQLite, every side-audited task committed exactly one side write, unless its
  worker was killed during the commit (below).

While the run goes on, no task may run more than 20 times at one retry count
between requeues from the DLQ, which reset the count. A
failed attempt raises the count; only a lost claim or a killed worker repeats one,
and each waits for a claim to be released or to expire. More repeats mean the
queue is not recording failures, so retry limits do not hold.

In half the runs (`-shutdown-on-signal`), workers stop as many services do: a
goroutine calls `Shutdown` as soon as SIGTERM comes, even while the main goroutine
is still registering consumers, and `Consume`'s context never ends. Each
registration follows a poll interval of startup work, and the supervisor sends
SIGTERM to every third worker as it registers its second consumer
(`term_at_registration`). No worker may start a handler once its `Shutdown` has
returned (`shutdown_returned`); after that it keeps running for ten polls, as a
service closing other resources would. A `Consume` refused with `ErrShutDown`
after the signal is expected (`registration_refused`).

Each run publishes one `kept` job, whose handler always fails, with one retry.
Injected handler errors, the kept job's among them, end with a NUL byte and invalid
UTF-8, which PostgreSQL text columns reject, and the queue must still record them.
After the run, the kept job must be in the DLQ through the public API: with retry
count at least 1, its handler's error as the reason, and creation and failure times within
this run, whatever the time zones. (A count over 1 is allowed: a failed move to the
DLQ reschedules the job, which counts as a retry.) The kept consumer, like DLQ cleanup,
runs every fifty polls: on SQLite both take the write lock even when they find
nothing, and the fault-free audit oracle is sensitive to lock contention. DLQ cleanup must not have deleted it.

Deliberately failing handlers publish a poison job inside their transaction.
Any leaked publication is therefore detected even if the factorial is correct.

Every committed result also publishes an `audit` job in the same transaction.
Its handler only reads the result back, so it can fail only by timing out. In
SQLite, a transaction that starts with a read is not yet a writer, so it
exercises the queue marking a job processed after other workers have committed.
Retries are not a usable signal, because the short randomized lock timeouts make
factorial jobs retry even in healthy runs. Instead, the oracle checks every
failure recorded against an audit: `last_error` after the run, and DLQ reasons
before recovery requeues them.

About a third of factorial tasks are read-first: they read their children's
results before taking the coordination row, as real handlers often read before
writing. On SQLite their write then fails on a stale snapshot whenever another
connection has committed since (`stale_snapshot` in coverage). That is the
handler's own failure, and it is retried. These attempts also check the queue
never marks a job processed without the handler's writes once it has written;
if it did, the exactly-once oracle would find the result missing.

On SQLite, one task in sixteen also publishes a `side_audit` job. Its handler reads
the result back and then records it in `side.db`, a database attached to its
connection. When another connection commits to the main database between that
read and the processed mark, the mark fails on a stale snapshot although the
transaction did write, so the queue must retry the handler rather than mark the
job in a fresh transaction. Each side-audited task must leave exactly one side
row. There is one exception. In WAL mode, SQLite commits attached databases one
after the other, not atomically: a worker killed between the commits leaves the
job processed without its side row. So a task may have no side row if its last
side audit was its worker's last event and the worker was SIGKILLed within one
claim length of it. Any later event from that worker, such as its next attempt,
shows that the commit returned. Side audits run on a queue of their own, whose connections have the side
database attached, because the queue only marks a job in a fresh transaction on
a connection that uses no other database: plain audits keep connections without
it. Like any SQLite handler that reads and then writes, a side audit may also
fail on `database is locked` in fault-free runs. Recovery waits until the DLQ is
empty as well, since audits of either kind can remain there after every result
is committed.

SQLite runs keep their database in the artifacts directory and expect it on a fast
filesystem: set `SIMULATION_OUTPUT` to a tmpfs directory, such as one under `/tmp`
on most Linux systems. On a disk that syncs each commit, seeds
with the shortest busy timeout (10 ms) can fail the fault-free audit oracle on
`database is locked`: the busy timeout is too short for that disk, not a queue bug.

Each SQLite worker opens its first connection before starting any queue.
SQLite 3.53 initializes its mutex methods on first use and reads them back
without a barrier, so on arm64 concurrent first opens in a fresh process can
crash with SIGSEGV at PC 0; applications should do the same.

SQLite workers open the database through `sqlite3-observed`, a thin wrapper
around the mattn driver. It makes any statement that fails with
`SQLITE_BUSY_SNAPSHOT` fatal unless it belongs to a handler's transaction: the
workload's own `chaos_*` statements, or the queue's processed mark, which the
queue retries in a fresh transaction. Elsewhere a stale snapshot means one of the
queue's own transactions read before writing; no busy timeout explains it.
Recovery applies the same rule to `RequeueDeadLetterJob`. Plain `SQLITE_BUSY`, a
lock wait that ran out under the randomized short timeouts, is still retried.
Logical task identities are independent of queue job IDs, which can change on
DLQ requeue. Cleanup cannot delete the independent correctness evidence.

## Faults and coverage

### Deterministic readiness gates

The gates cover:

- Retry counts crossing 65,535, finite retry limits above that boundary, and
  saturation at the PostgreSQL integer limit, with healthy jobs in the same
  fetch batches. Backoff arguments saturate rather than wrap; default backoff
  is capped at 24 hours.
- Cleanup cancellation while waiting for an exhausted pool, including a held
  SQLite driver mutex, and PostgreSQL cleanup blocked by a real row lock with
  no server-side lock timeout. Locks/connections remain held until shutdown
  has returned.
- Two PostgreSQL callers concurrently requeueing one DLQ row. An advisory-lock
  gate confirms both transactions are blocked before releasing them. Exactly
  one replacement must commit. A failed replacement INSERT must restore the
  DLQ row by transaction rollback.
- A real worker process stopped after its local claim check, before beginning
  its handler transaction. The test observes SIGSTOP, waits for natural claim
  expiry, gives ownership to B, and resumes A while B is still pending. A must
  reach the completion token check and roll back its successful handler writes;
  only then does B complete. This cannot pass merely because A timed out or B
  had already marked the job processed.
- Both sides of commit ambiguity, each with finite retries and immediate DLQ
  handling. SQLite uses a wrapper around the real driver's transaction, either
  rolling back before returning a commit error or committing before hiding
  success. PostgreSQL uses a protocol-aware TCP proxy: one fault drops COMMIT
  before forwarding it; the other observes the server's `CommandComplete(COMMIT)`
  and drops the acknowledgment. Every case requires confirmed fault activation
  and checks handler writes, downstream publication, retry state, and DLQ hooks.
- The same confirmed pre-/post-commit faults at claim acquisition, retry
  settlement, DLQ transfer, and DLQ requeue. Checks cover non-delivery of failed
  claim responses, eventual reclaim with a new token and unchanged retry count,
  retry counts advancing only for committed settlements, no duplicate terminal
  hook effects/publications, and no duplicate replacement after a requeue retry.
- Caller-owned `PublishTx` transactions with two jobs plus a business write:
  independent readers see nothing before commit, rollback leaves neither jobs
  nor business writes, and even ambiguous commit outcomes preserve all-or-none
  atomicity. A publish success inside a transaction does not imply durability.
- Processed/DLQ cleanup across multiple small batches, with historical fixtures
  covering recent completion of old jobs, pending work, live and expired claims,
  delayed jobs, retries, and other job types. Only eligible terminal rows may
  disappear, and a repeated cleanup must delete nothing further.
- Three PostgreSQL outages followed by recovery in the same queue instance and
  pool. The test confirms failed reconnections during each outage, then checks
  exactly one committed effect per publication after healing.
- A PostgreSQL immediate shutdown with a handler held after an uncommitted
  write, then restart using the same data directory. The original consumers
  must recover the interrupted job and accept new work without being restarted.

Native commands for the same gates:

```sh
go test -race -count=1 -timeout 3m -run '^TestReadinessSQLite$' .
SQLQ_TEST_POSTGRES_DSN='postgres://user:password@localhost/testdb?sslmode=disable' \
  go test -race -count=1 -timeout 5m -run '^TestReadinessPostgres$' .
```

The PostgreSQL suite uses a fresh schema and needs an isolated test server.
Its crash/restart subtest runs only when `SQLQ_TEST_POSTGRES_DATA_DIR` is set
to a disposable local server's data directory, which the test stops and restarts
with `runuser -u postgres -- pg_ctl`; runs without it skip the subtest.
Consumer/database process crashes do not simulate storage power loss.

The added SQLite gates passed natively with `-race`, `checkptr=2`, and
`GOEXPERIMENT=cgocheck2`. Subsequent real-time exploration on 2026-10-13 passed:

| Exploration budget | Seeds exercised | Passing profiles | Artifact directory |
| --- | --- | --- | --- |
| 10 minutes | 300042–300111 | 209 | `endurance-extra-10m/` |
| 20 minutes | 400042–400169 | 384 | `endurance-extra-20m/` |
| 30 minutes, full rerun | 500042–500239 | 593 | `endurance-extra-30m-rerun/` |

Each stage first ran the deterministic SQLite gates, then explored healthy,
finite-DLQ, and unlimited-cleanup profiles with race, pointer, cgo, leak, and
database-integrity checks. The duration limit finishes the active profile; the
last seed need not run all three profiles. The first 30-minute attempt stopped
at roughly 21 minutes because the temporary Go worker executable pathname had
disappeared. The Linux procfs launch fix passed an unlink regression, a replay
of seed 500167, and the complete 30-minute rerun. These runs found no queue
correctness failures.

### Randomized factorial faults

First attempts select errors, panics, cooperative timeouts, late `nil` returns,
waits beyond claim expiry, and waits just below the job timeout. Faults after
transactional writes exercise rollback of results and downstream publication.
Selected dead-letter hooks write an effect and then fail; that effect must roll
back. Retry backoff and publishing delays vary by task/attempt.

`-lock-timeout` controls SQLite's busy timeout and PostgreSQL's server-side lock
timeout. Competing handlers holding the coordination row/writer lock produce real
lock waits; `lock_error` events record failures observed at this boundary.

The supervisor requests `SIGKILL`, `SIGTERM`, and `SIGSTOP`/`SIGCONT`, including
one kill requested upon observing a `written` milestone. That is a targeted fault
request, not a guarantee that the process was killed before commit: event delivery
and the signal race with the handler. Pauses may span claim expiry. Healing resumes
paused processes before killing/replacing them.

PostgreSQL workers use a loopback TCP proxy with independently varying request and
response delays per 4 KiB chunk, connection resets, and temporary partitions.
This exercises real SQL protocol traffic, including commit acknowledgment loss.
It is stream-level impairment, not packet-level `netem` loss/reordering. Supervisor
connections bypass impairment so healing/verification can proceed.

`events.jsonl` and summary coverage distinguish started waits from completed waits
(`long_wait_finished`, `late_success_return`). Coverage reports observed events and
requested process/network faults; a seed does not guarantee every boundary case.
A nonzero chaos budget with no observed handler attempts fails. Increase the
workload/budget when more scenario coverage is needed.

## Runtime and build checks

Final consumers now shut down gracefully within `-shutdown` and must finish their
checks before the run can pass. Go's `goroutineleak` profile is collected after
consumer/database shutdown and after supervisor cleanup. Writing the profile
triggers leak-detecting GC; checking the profile count alone would not do that.
Detected leaks fail the run and save their stacks. Unexpected worker exits and
race/sanitizer/fatal-runtime evidence fail even when chaos also requested a kill.
Killed workers cannot perform a final leak check; healthy final workers must.

`DB.Close` does not wait for checked-out connections. Cancellation-triggered
rollback can still be completing when it returns, so the pool check waits for
zero open/in-use connections within half the shutdown budget. Runtime reports
record both the initial sample (`database_before_drain`) and the settled sample
(`database`). A connection that does not drain by the deadline still fails.

Available checks:

| Check | Where it runs |
| --- | --- |
| Goroutine leak profile | Consumers and supervisor, Go 1.27+ by default |
| Graceful shutdown deadline and process reaping | Final consumers |
| Closed SQL pools / no checked-out connections | Shutdown and verification |
| SQLite `integrity_check` and `foreign_key_check` | After convergence |
| `go vet` | Check target |
| Strong pointer checking (`checkptr=2`) | Check target |
| Strong cgo pointer checking (`cgocheck2`) | Check target |
| Race detector | Check target and simulation targets |

Instrumentation changes timing and CPU cost; compare instrumented and plain runs
separately rather than treating them as identical speed benchmarks. Race reports
stay on stderr and halt the worker immediately.

```sh
make test-chaos-checks
make test-checks # All packages, short mode; PostgreSQL integration is separate.
```

Each process writes `*-runtime.json`, `*-goroutineleak.txt`, and
`*-goroutines.txt`. Runtime reports include actual Go version, build flags,
instrumentation availability, memory statistics, and database pool statistics.
`verified` means the database oracle passed; `passed` additionally requires
runtime/resource checks to pass. Leak detection does not prove absence of every
leak, and intentionally persistent global runtime goroutines are not rejected
based on raw goroutine count.

Random process/network disruption starts after the first observed handler attempt,
so instrumented startup is allowed to reach the queue. Network delay units are
capped at one second; increasing compute deadlines does not multiply every SQL
protocol chunk's delay into minutes.

### Validation status

A native SQLite run of `50000!` with seed 42 verified 1,949 tasks in approximately
5.2 seconds, including a 3-second chaos phase. A subsequent native run with race
detection, `checkptr=2`, `cgocheck2`, leak detection, and SQLite integrity checks
also verified all 1,949 tasks, in roughly 6.6 seconds. Final consumers and
supervisor reported zero detected leaks. Intentional leaks, hung shutdown,
unexpected worker exits, and sanitizer evidence are covered by negative
regression tests.

The native Go SQLite simulation passed seeds 42–44 with race and leak checks,
verifying 435/451/451 tasks per profile. All nine profiles passed in about 14
seconds with temporary outputs, including process faults, timeouts, rollback
failures and DLQ recovery. The default Make target also passed with retained
artifacts, taking about 10 seconds for its three seed-42 profiles in this
environment; filesystem costs affect these measurements.
The Testcontainers PostgreSQL path compiles but awaits a Docker-backed run.

## Verification commands

```sh
make test-chaos
make test-simulation-sqlite
make test-simulation-psql
```

The tests use real subprocesses and reject intentionally corrupted results,
duplicated/missing committed work, leaked hook effects, poison publications,
audits charged a failure that wasn't a timeout, and missing or duplicated side
writes.
Native profiles run in this module's Go tests; full endurance profiles are opt-in. Root-module `go test ./...` covers the library; use the simulation
targets for this setup, or `make test-chaos` for Docker-free harness regressions.

Artifacts include `config.json`, `events.jsonl`, per-worker stderr logs,
`database.json`, and `summary.json`. The snapshot retains queue and ledger rows,
including intermediate integers. SQLite additionally retains the actual database.
For failures, start with the summary error, last events, remaining jobs/DLQ, and
the committed ledger.
