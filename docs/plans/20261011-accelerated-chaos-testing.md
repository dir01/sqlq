# Accelerated real-database chaos testing

Date: 2026-10-11
Status: Standard check-enabled `5000!` x86 and ARM64 TCG baselines validated across all three profiles; full `50000!`, HVF, and VM race/ASan profiles pending validation. ARM64 has not demonstrated an overall speedup yet.

Latest VM validation: the check-enabled `5000!` run passed SQLite finite retries
(209 tasks) and PostgreSQL (185 tasks), including leak checks and PostgreSQL's
offline page checksum check (zero bad checksums). SQLite push failed an immediate
post-close pool assertion during cancellation; the harness now allows bounded
asynchronous rollback drainage and tests both successful drainage and retained
connections. The rebuilt v2 standard-profile run passed all three profiles:
SQLite finite retries (209 tasks), SQLite push (207 tasks), and PostgreSQL
(185 tasks), with zero residual jobs, detected leaks, or open database
connections. PostgreSQL's offline check found zero bad checksums across 2,891
blocks. Host runtime was about 154 seconds, with idle probe ratios about 148x
and 88x. VM race/ASan profiles and full `50000!` convergence remain pending.

ARM64 follow-up: all three standard `5000!` profiles passed, with zero residual
work, detected leaks, or bad PostgreSQL page checksums (2,892 blocks scanned).
Host runtime was 174.6 seconds versus 153.7 seconds for the preceding x86 run.
Idle probes improved to about 300x/139x, but no end-to-end speedup was observed.
Per-profile host measurements are now recorded; controlled no-chaos comparisons
between ARM TCG and macOS HVF are needed to isolate CPU/SQL overhead.

## Implementation status

- `tests/chaos/`: self-contained Go 1.27 test module using `replace ../..` to exercise the local library checkout.
- `tests/chaos/cmd/sqlq-chaos`: standalone seeded real-database workload and chaos supervisor.
- `tests/chaos/internal/chaos`: factorial product tree, append-only transactional ledger, oracle, subprocess faults, PostgreSQL TCP proxy, clock probe, and regression tests.
- `tests/chaos/cmd/sqlq-chaos-vm`: isolated QEMU launcher, independent host watchdog, serial artifacts, architecture/mode selection, and per-profile host timing.
- `tests/chaos/vm`: native ARM64/AMD64 image builder and guest profiles for both drivers. TCG accelerated/realtime and macOS HVF realtime modes are supported; ARM/HVF boot and speed need validation.
- Usage and current coverage boundaries: [Chaos testing](../../tests/chaos/README.md).

Verified locally: race-enabled SQLite profiles, oracle corruption rejection, TCP proxy delay/partition/healing, and full `50000!` runs (1,949 tasks, seed 42), including a run with race detection, `checkptr=2`, `cgocheck2`, goroutine leak detection and SQLite integrity checks. Negative tests reject leaked goroutines, hung shutdown and unexpected worker exits. The original guest ran on the user's Apple Silicon host and measured 148x/105x acceleration on idle probes, but all full workload profiles timed out. Snapshots show correct, nonduplicated intermediate results with SQLite root tasks unfinished and substantial PostgreSQL lock-timeout churn. New guest profiles use Go 1.27, configurable timeouts, optional race/ASan builds and offline PostgreSQL checksum checks; these need a rebuilt-image validation run. Docker, QEMU, and PostgreSQL are unavailable in the implementation environment. Exact replay, guaranteed stale-claim/boundary coverage, packet-level impairment, and VM power-loss persistence remain future work. The implemented network impairment uses a guest-local TCP proxy rather than network namespaces/`netem`.

## Goal

Exercise real SQLite and PostgreSQL queues under randomized faults and varied configurations, then require convergence to an independently verified result. Run the system inside an isolated VM whose time can advance faster than host time and whose internal network can be impaired.

Target 100–1000× guest-time acceleration for wait-heavy scenarios, subject to measurement. This is not a promised throughput improvement: SQL execution, polling, and computation still require real work. Measure useful scenario throughput as well as elapsed guest time.

## Existing implementation constraints

- Consumers use Go timers, context deadlines, and real `*sql.Tx` transactions.
- Lease handling depends on both local Go time and database time. PostgreSQL uses `clock_timestamp()` and `NOW()`; SQLite uses `julianday('now')` in claim statements.
- SQLite deliberately evaluates claim time after waiting for the database write lock. Replacing that with a timestamp captured before the query would change production semantics.
- Handler writes, downstream `PublishTx` calls, and job completion can share the queue-owned transaction.
- Graceful shutdown releases claims; abrupt process death is necessary to test recovery of abandoned claims.
- Async push is supported by SQLite, not PostgreSQL.

Faking only Go's clock is insufficient. The proposed environment exercises the existing database engines and queue implementation with coherent guest time.

## Architecture

```text
Host: launcher, real-time watchdog, artifact collection
└── VM: virtual clock; no external network interface
    ├── Test supervisor / seeded chaos controller
    ├── Independently killable consumer processes
    ├── PostgreSQL
    │   └── Guest-internal network paths with fault injection
    └── SQLite on guest-local storage
```

Bake runtime dependencies into the guest image. Runtime test networking must remain entirely within the VM. Export logs and results through a serial channel or disk artifacts.

For PostgreSQL, use guest network namespaces and virtual interfaces to separate consumer/database paths. Apply delay, jitter, packet loss, and bandwidth limits using Linux `tc netem` and related traffic-control facilities. Add explicit connection interruption and partition scenarios. Keep supervisor control communication separate from impaired database paths.

For SQLite, use a shared guest-local database file and multiple consumer processes. Exercise lock contention, process death, pauses, and timeouts; there is no database network connection to impair.

## Virtual-time feasibility prototype

Investigate QEMU TCG with instruction-count-based virtual time (`icount`), without synchronization to host wall time. Validate exact options against the selected QEMU version and guest kernel. Conventional hardware virtualization or changing the wall-clock date alone does not establish accelerated, coherent timers.

The prototype must establish that:

1. Go timers and context deadlines advance with guest time.
2. PostgreSQL and SQLite timestamps advance coherently with guest time.
3. Guest network delays use the same time domain.
4. Idle waits accelerate without making ordinary database operations invariably miss deadlines.
5. A host-time watchdog can terminate a hung guest independently of guest time.

Measure both guest seconds per host second and completed jobs/fault scenarios per host second. Include polling-heavy and long-idle profiles. Frequent polling limits useful acceleration because each timer wakeup still executes work.

Treat acceleration as an early feasibility gate. Record measured limits before committing the full harness to a particular VM configuration. Keep the correctness harness runnable directly on a host for development and comparison.

Reference: https://www.qemu.org/docs/master/system/invocation.html

## Correctness workload: factorial via a product tree

Use a real computation rather than throughput-only load. The full endurance profile computes `50000!`; smaller profiles use the same structure with fewer inputs.

1. Partition `1…N` into seeded, variable-sized ranges.
2. Publish jobs that compute each range's product.
3. Schedule reduction jobs that combine intermediate products.
4. Continue until the root contains `N!`.
5. Independently compute the expected integer with Go's `math/big` and compare the complete result.

Use stable logical task IDs independent of queue job IDs, since dead-letter requeue can create a new queue job ID. Define the expected graph before execution so an independent oracle knows every expected task, edge, and intermediate value.

Each successful handler uses the transaction supplied by the queue to:

- Store its result.
- Append its committed execution record.
- Publish any newly ready downstream work using `PublishTx`.

Dependency readiness and downstream scheduling must be transactionally coordinated under concurrent parent completion. The workload must itself be correct under the database isolation levels used; duplicate scheduling must be observable rather than silently ignored.

### Independent invariants

Maintain an append-only committed execution ledger and verify:

- Every expected logical task committed exactly once.
- No unexpected task committed.
- Every intermediate product is correct.
- Every expected dependency was consumed correctly.
- Failed, panicking, canceled, and timed-out attempts left no committed transactional effects.
- Downstream publication and parent completion are atomic with handler effects.
- No required work remains pending, claimed, or stranded in the DLQ after recovery.
- The root equals the independently calculated factorial.

Repeated handler attempts are expected. Repeated committed success effects for one logical task are a failure. Avoid `ON CONFLICT DO NOTHING` or equivalent application deduplication that would conceal queue correctness bugs. The final factorial alone is not sufficient to detect all duplicate or orchestration errors.

Preserve the independent ledger when queue cleanup deletes processed rows, so cleanup cannot conceal lost or duplicated work.

## Configuration matrix

Use mandatory baseline profiles plus seeded combinations of supported settings:

- SQLite and PostgreSQL.
- Consumer process counts and workers per consumer.
- Prefetch counts and polling intervals.
- Connection pool sizes.
- SQLite push disabled/enabled, including push rate limits.
- Finite and unlimited retries, varied backoff, and delayed publishing.
- Job and claim timeouts with valid relationships.
- Cleanup intervals, retention ages, and batch sizes.

Record the complete effective configuration. Invalid-option behavior can be checked separately; convergence scenarios require valid configurations.

## Chaos scenarios

Combine random exploration with mandatory scenario coverage. Generate timing near relevant boundaries and occasionally choose long waits.

| Area | Scenarios |
| --- | --- |
| Consumer lifecycle | `SIGKILL`, restart, graceful shutdown, `SIGSTOP`/`SIGCONT` across claim expiry |
| Handler behavior | Errors, panics, slow success, cooperative cancellation, success returned after context expiry |
| Timeout boundaries | Delays immediately below/above job timeout, claim expiry, and database lock-wait timeout |
| Transactions | Failure after writing a result, failure after downstream publication, process death before completion |
| PostgreSQL network | Latency, asymmetric delay, restricted bandwidth, dropped connections, temporary partitions |
| Retry and DLQ | Finite/unlimited retries, long backoff, failing dead-letter hooks, requeue |
| Maintenance | Cleanup while work is active, preserving independent audit evidence |

Explicitly exercise handler duration greater than job timeout but less than claim timeout. Confirm that a handler returning `nil` after its deadline cannot commit its writes.

Pause a consumer across claim expiry, allow another consumer to recover its work, then resume the original consumer to test stale-claim fencing.

Use observable handler milestones to target faults at meaningful transaction stages where possible. Record scenarios actually observed, not merely faults scheduled. Boundary races may have multiple valid outcomes; assert atomicity and eventual convergence rather than a particular scheduler winner.

## Run lifecycle and budgets

Every run has three phases:

1. **Exercise:** execute the workload with randomized faults.
2. **Heal:** stop fault injection, restore connectivity, resume or replace consumers, disable deliberate handler failures, and requeue eligible workload jobs.
3. **Verify:** require convergence within a bounded recovery period, stop consumers, and inspect the final database state.

Healing must use ordinary queue operations. Do not manually repair missing jobs or intermediate results.

Support:

- Guest-time chaos duration.
- A separate guest-time recovery deadline.
- A host-time watchdog covering hangs and overall execution.
- Optional fixed operation/fault counts for regression runs.

Reserve recovery and verification time rather than consuming the entire budget on chaos. A recovery timeout is a test failure with diagnostics, not a successful budget exhaustion. Choose workload size and budgets so useful work overlaps the chaos phase; finishing before planned coverage is exercised must be reported.

## Reproduction and diagnostics

Save:

- Seed and generated configuration.
- Expected workload graph.
- Ordered fault history and guest timestamps.
- Observed handler/transaction milestones.
- Process starts, stops, signals, and exit statuses.
- Coverage of required scenarios.
- Queue, DLQ, result, and execution-ledger state on failure.
- Guest/host timing and throughput measurements.

A seed reproduces workload and fault choices, not OS scheduling or every low-level execution. Exact QEMU record/replay is a separate capability to investigate under supported configurations, not a prerequisite for the first useful harness. Pin relevant VM/image/tool versions in artifacts.

## Implementation milestones

### 1. VM feasibility probe

Build an isolated runtime image and launcher. Validate coherent accelerated clocks, internal network impairment, artifact extraction, and host watchdog behavior. Measure actual speedup on representative workloads and document limitations.

### 2. Correctness workload

Implement the product-tree workload, transactional execution ledger, independent oracle, and bounded recovery/verification. Run it directly against isolated SQLite and PostgreSQL databases before introducing VM chaos.

### 3. Chaos supervisor

Add independently controlled consumer processes, seeded fault schedules, guest-internal PostgreSQL network faults, threshold-focused scenarios, and failure artifacts. Prove that deliberately broken outcomes are rejected by the oracle.

### 4. Execution profiles and documentation

Provide small regression profiles, medium randomized profiles, and opt-in full `50000!` endurance runs. Document host and VM execution, budget controls, seed replay, required dependencies, artifact inspection, and measured acceleration.

## Completion criteria

- Both database drivers execute the same independently checked workload.
- Required configuration and fault scenarios have observable coverage.
- All networking during VM tests stays within the guest.
- Faults stop and healthy consumers converge without manual data repair.
- Missing work, duplicate committed effects, leaked failed-attempt effects, incorrect intermediate values, and incorrect final results cause failures.
- Runs have bounded recovery and a host-time watchdog.
- Failures retain sufficient configuration, history, and database evidence for investigation.
- Virtual-time behavior and achieved acceleration are measured and documented rather than assumed.
