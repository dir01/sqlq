package chaos

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dir01/sqlq"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/mattn/go-sqlite3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type Event struct {
	At     time.Time `json:"at"`
	Worker int       `json:"worker"`
	Kind   string    `json:"kind"`
	Task   int       `json:"task"`
	Detail string    `json:"detail,omitempty"`
}

type emitter struct {
	mu     sync.Mutex
	out    io.Writer
	worker int
}

func (e *emitter) emit(kind string, task int, detail string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	_ = json.NewEncoder(e.out).Encode(Event{At: time.Now(), Worker: e.worker, Kind: kind, Task: task, Detail: detail})
}

func openDB(c Config, worker bool) (*sql.DB, error) {
	dsn := c.DSN
	if worker && c.WorkerDSN != "" {
		dsn = c.WorkerDSN
	}
	var db *sql.DB
	var err error
	if c.Driver == "postgres" {
		var pc *pgx.ConnConfig
		pc, err = pgx.ParseConfig(dsn)
		if err != nil {
			return nil, err
		}
		if c.Schema != "" {
			pc.RuntimeParams["search_path"] = c.Schema
		}
		pc.RuntimeParams["lock_timeout"] = fmt.Sprint(c.LockTimeout.Milliseconds())
		if c.DBTimeZone != "" {
			pc.RuntimeParams["timezone"] = c.DBTimeZone
		}
		db = stdlib.OpenDB(*pc)
	} else if worker {
		db, err = sql.Open(observedSQLiteDriver, dsn)
	} else {
		db, err = sql.Open("sqlite3", dsn)
	}
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(c.Pool)
	db.SetMaxIdleConns(c.Pool)
	return db, nil
}

// dlqRetention is far longer than any run.
const dlqRetention = time.Hour

// faultText ends each injected error message. A handler's error can be any text: PostgreSQL
// text columns reject NUL and invalid UTF-8, yet the failure must still be recorded.
const faultText = ": \x00 \xff"

func queue(c Config, db *sql.DB, tracers ...trace.Tracer) (sqlq.JobsQueue, error) {
	opts := []sqlq.NewOption{
		sqlq.WithDefaultPollInterval(c.Poll), sqlq.WithDefaultConcurrency(c.Concurrency),
		sqlq.WithDefaultPrefetchCount(c.Prefetch), sqlq.WithDefaultJobTimeout(c.JobTimeout),
		sqlq.WithDefaultClaimTimeout(c.ClaimTimeout), sqlq.WithDefaultMaxRetries(c.MaxRetries),
		sqlq.WithDefaultBackoffFunc(func(retry uint16) time.Duration {
			return c.Backoff * time.Duration(1+int(retry)%4)
		}),
		sqlq.WithDefaultCleanupProcessedInterval(c.Cleanup),
		sqlq.WithDefaultCleanupProcessedAge(c.ClaimTimeout), sqlq.WithDefaultCleanupBatch(7),
		// Required work must remain recoverable from the DLQ throughout chaos. No run lasts
		// dlqRetention, so DLQ cleanup must never delete anything, in any time zone. On SQLite each
		// cleanup takes the write lock even when it deletes nothing, and every consumer of every
		// process runs one: they add to the lock contention that the audit oracle is sensitive to.
		sqlq.WithDefaultCleanupDLQInterval(50 * c.Poll), sqlq.WithDefaultCleanupDLQAge(dlqRetention)}
	if len(tracers) > 0 {
		opts = append(opts, sqlq.WithTracer(tracers[0]))
	}
	return sqlq.New(db, sqlq.DBType(c.Driver), opts...)
}

// shutdown stops q and waits with no time limit, as services that rely on Shutdown alone do.
// Shutdown returns an error only when its context ends, so it returns nil here.
func shutdown(q sqlq.JobsQueue) {
	_ = q.Shutdown(context.Background())
}

func initialize(ctx context.Context, c Config, db *sql.DB, q sqlq.JobsQueue) error {
	if err := q.Run(ctx); err != nil {
		return err
	}
	if c.SideDB != "" {
		if err := createSideDB(ctx, c); err != nil {
			return err
		}
	}
	for _, statement := range []string{
		`CREATE TABLE chaos_mutex (id INTEGER PRIMARY KEY, version INTEGER NOT NULL)`,
		`INSERT INTO chaos_mutex VALUES (1, 0)`,
		// Intentionally no uniqueness constraint: duplicate commits must be visible.
		`CREATE TABLE chaos_ledger (task INTEGER NOT NULL, value TEXT NOT NULL, outcome TEXT NOT NULL)`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if err := q.Publish(ctx, keptJobType, -1); err != nil {
		return err
	}
	for _, task := range c.Tasks {
		if task.Left >= 0 {
			continue
		}
		// Vary availability and exercise delayed jobs even in healthy profiles.
		delay := time.Duration(task.ID%5) * c.Poll
		if err := q.Publish(ctx, jobType, task.ID, sqlq.WithDelay(delay)); err != nil {
			return err
		}
	}
	return nil
}

func placeholders(c Config, query string) string {
	if c.Driver != "postgres" {
		return query
	}
	for i := 1; strings.Contains(query, "?"); i++ {
		query = strings.Replace(query, "?", fmt.Sprintf("$%d", i), 1)
	}
	return query
}

// Worker runs in a separate process. Healthy workers use precisely the same
// transaction protocol, with only deliberate failures disabled.
func Worker(ctx context.Context, c Config, id int, healthy bool, out io.Writer) (resultErr error) {
	e := &emitter{out: out, worker: id}
	var db *sql.DB
	defer func() {
		_, err := checkRuntime(c.Artifacts, fmt.Sprintf("worker-%d", id), db, c.RequireLeakCheck, c.Shutdown/2)
		resultErr = errors.Join(resultErr, err)
		if resultErr != nil {
			e.emit("check_failure", -1, resultErr.Error())
		} else {
			e.emit("checks_passed", -1, "")
		}
	}()
	report := func(query string, err error) {
		e.emit("fatal", -1, fmt.Sprintf("queue transaction upgraded a stale SQLite read snapshot: %s: %v", query, err))
	}
	snapshotConflict.Store(&report)
	var err error
	db, err = openDB(c, true)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, db.Close()) }()
	if c.Driver == "sqlite" {
		// Open the first connection before any goroutine does. SQLite 3.53 initializes its mutex
		// methods on first use and reads them back without a barrier: on arm64, threads opening
		// concurrently in a fresh process can call a method not yet visible (a SIGSEGV at PC 0).
		if err := db.PingContext(ctx); err != nil {
			return err
		}
	}
	metrics, provider, tracer := diagnosticTracer()
	// Each Worker is its own process; driver tracers use the global provider.
	otel.SetTracerProvider(provider)
	defer func() { resultErr = errors.Join(resultErr, provider.Shutdown(context.Background())) }()
	q, err := queue(c, db, tracer)
	if err != nil {
		return err
	}
	var sideQ sqlq.JobsQueue
	if c.SideDB != "" {
		// Side audits run on a queue of their own, whose connections have the side database
		// attached, so that plain audits keep connections without it (see side_db.go).
		sideDB := openSideDB(c)
		defer func() { resultErr = errors.Join(resultErr, sideDB.Close()) }()
		if sideQ, err = queue(c, sideDB, tracer); err != nil {
			return err
		}
		defer shutdown(sideQ)
	}
	// Application database work outlives the queue: services usually stop the
	// queue first and release their own connections afterwards.
	appCtx, stopApp := context.WithCancel(context.WithoutCancel(ctx))
	var app sync.WaitGroup
	defer app.Wait()
	defer stopApp()
	defer shutdown(q)
	defer func() { metrics.emit(e, db) }()
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	// Some services never cancel Consume's context and rely on Shutdown alone.
	consumeCtx := ctx
	if c.SaturatePoolOnShutdown {
		consumeCtx = appCtx
	}
	// Services that stop from a signal handler call Shutdown at once, even while main is still
	// registering consumers, and rely on it alone: Consume's context never ends.
	shutdownDone := make(chan struct{})
	if c.ShutdownOnSignal {
		consumeCtx = appCtx
		go func() {
			defer close(shutdownDone)
			<-ctx.Done()
			shutdown(q)
			if sideQ != nil {
				shutdown(sideQ)
			}
			e.emit("shutdown_returned", -1, "")
		}()
		defer func() { stop(); <-shutdownDone }()
	}
	consume := func(q sqlq.JobsQueue, jobType string, handler func(context.Context, *sql.Tx, []byte) error, opts ...sqlq.ConsumerOption) error {
		if c.ShutdownOnSignal {
			// Startup work before each registration, during which a signal can come.
			e.emit("registering", -1, jobType)
			time.Sleep(c.Poll)
		}
		err := q.Consume(consumeCtx, jobType, handler, opts...)
		if errors.Is(err, sqlq.ErrShutDown) && c.ShutdownOnSignal && ctx.Err() != nil {
			// The signal came: the service stops registering and goes on shutting down.
			e.emit("registration_refused", -1, jobType)
			return nil
		}
		return err
	}
	opts := []sqlq.ConsumerOption{sqlq.WithConsumerOnDeadLetter(func(ctx context.Context, tx *sql.Tx, info sqlq.JobInfo, payload []byte, _ error) error {
		var task int
		if err := json.Unmarshal(payload, &task); err != nil {
			return err
		}
		e.emit("dlq_hook", task, "")
		if !healthy && task%3 == 0 && int(info.RetryCount) <= max(0, int(c.MaxRetries))+1 {
			if _, err := tx.ExecContext(ctx, placeholders(c, `INSERT INTO chaos_ledger VALUES (?, '', 'failed-hook')`), task); err != nil {
				return err
			}
			e.emit("hook_failure", task, "")
			return errors.New("injected dead-letter hook failure")
		}
		return nil
	})}
	if c.Push {
		opts = append(opts, sqlq.WithAsyncPush(), sqlq.WithAsyncPushRateLimit(c.PushRPM))
	}
	err = consume(q, jobType, func(ctx context.Context, tx *sql.Tx, payload []byte) error {
		var id int
		if err := json.Unmarshal(payload, &id); err != nil || id < 0 || id >= len(c.Tasks) {
			return fmt.Errorf("invalid task payload %s", payload)
		}
		task := c.Tasks[id]
		info, _ := sqlq.JobInfoFromContext(ctx)
		mode := -1
		if !healthy && info.RetryCount == 0 {
			mode = (id + int(c.Seed%8) + 8) % 8
		}
		e.emit("attempt", id, fmt.Sprintf("retry=%d mode=%d", info.RetryCount, mode))
		if mode == 4 {
			e.emit("long_wait", id, "")
			time.Sleep(c.ClaimTimeout + c.Poll)
			e.emit("long_wait_finished", id, fmt.Sprintf("context=%v", ctx.Err()))
		} else if mode == 5 {
			e.emit("below_timeout", id, "")
			time.Sleep(c.JobTimeout - c.JobTimeout/10)
		}
		// The product reads both committed children. Those rows never change once
		// committed, so it may be read before or after the coordination write.
		product := func() (string, error) {
			if task.Left < 0 {
				return expected(task), nil
			}
			values := make([]string, 2)
			for i, child := range []int{task.Left, task.Right} {
				rows, err := tx.QueryContext(ctx, placeholders(c, `SELECT value FROM chaos_ledger WHERE task=? AND outcome='success'`), child)
				if err != nil {
					return "", err
				}
				count := 0
				for rows.Next() {
					if err = rows.Scan(&values[i]); err != nil {
						_ = rows.Close()
						return "", err
					}
					count++
				}
				err = rows.Err()
				_ = rows.Close()
				if err != nil {
					return "", err
				}
				if count != 1 {
					return "", fmt.Errorf("task %d has %d results for dependency %d", id, count, child)
				}
			}
			a, okA := new(big.Int).SetString(values[0], 10)
			b, okB := new(big.Int).SetString(values[1], 10)
			if !okA || !okB {
				return "", errors.New("invalid intermediate integer")
			}
			return new(big.Int).Mul(a, b).String(), nil
		}
		// Most attempts write first: SQLite then waits for its writer lock instead
		// of upgrading a read transaction. Read-first attempts read the children
		// first, as handlers often do. SQLite then fails their write with a stale
		// snapshot whenever another connection committed in between: a handler
		// failure, retried like any other. They also check that a handler which
		// did write is never marked processed without its writes.
		// PostgreSQL locks the coordination row. This serializes
		// readiness/publication, not attempts.
		readFirst := task.Left >= 0 && (id+int(c.Seed%3))%3 == 0
		var value string
		if readFirst {
			e.emit("read_first", id, "")
			var err error
			if value, err = product(); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE chaos_mutex SET version = version+1 WHERE id=1`); err != nil {
			var sqliteErr sqlite3.Error
			if errors.As(err, &sqliteErr) && sqliteErr.ExtendedCode == sqlite3.ErrBusySnapshot {
				e.emit("stale_snapshot", id, "") // Coverage: the read-first upgrade lost to a commit.
			}
			e.emit("lock_error", id, err.Error())
			return err
		}
		if !readFirst {
			var err error
			if value, err = product(); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, placeholders(c, `INSERT INTO chaos_ledger VALUES (?, ?, 'success')`), id, value); err != nil {
			return err
		}
		e.emit("written", id, "")
		if err := q.PublishTx(ctx, tx, auditJobType, id); err != nil {
			return err
		}
		if sideAudited(c, id) {
			if err := q.PublishTx(ctx, tx, sideAuditJobType, id); err != nil {
				return err
			}
		}
		if task.Parent >= 0 {
			p := c.Tasks[task.Parent]
			var count int
			if err := tx.QueryRowContext(ctx, placeholders(c, `SELECT COUNT(*) FROM chaos_ledger WHERE task IN (?, ?) AND outcome='success'`), p.Left, p.Right).Scan(&count); err != nil {
				return err
			}
			if count == 2 {
				if err := q.PublishTx(ctx, tx, jobType, p.ID, sqlq.WithDelay(time.Duration(id%3)*c.Poll)); err != nil {
					return err
				}
				e.emit("published", id, "")
			}
		}
		if mode >= 0 && mode <= 3 {
			// A published poison job and a ledger entry must both roll back.
			if err := q.PublishTx(ctx, tx, "poison", id); err != nil {
				return err
			}
			if mode == 2 {
				e.emit("above_timeout", id, "")
				// Deliberately ignore cancellation and return nil after deadline.
				time.Sleep(c.JobTimeout + c.JobTimeout/10)
				e.emit("late_success_return", id, fmt.Sprintf("context=%v", ctx.Err()))
				return nil
			}
			if mode == 3 {
				e.emit("cooperative_timeout", id, "")
				<-ctx.Done()
				return ctx.Err()
			}
			e.emit("rollback_fault", id, fmt.Sprintf("mode=%d", mode))
			if mode == 1 {
				panic("injected panic after transactional writes" + faultText)
			}
			return errors.New("injected error after transactional writes" + faultText)
		}
		return nil
	}, opts...)
	if err != nil {
		return err
	}
	// Audits only read in their transaction, like a notifier reading committed
	// state. SQLite's deferred transaction then starts as a reader, and the queue
	// must still be able to mark the job processed after other workers commit.
	audit := func(ctx context.Context, tx *sql.Tx, payload []byte) (int, error) {
		var id int
		if err := json.Unmarshal(payload, &id); err != nil || id < 0 || id >= len(c.Tasks) {
			return id, fmt.Errorf("invalid audit payload %s", payload)
		}
		var count int
		if err := tx.QueryRowContext(ctx, placeholders(c, `SELECT COUNT(*) FROM chaos_ledger WHERE task=? AND outcome='success'`), id).Scan(&count); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return id, fmt.Errorf("audit read: %w", ctxErr) // Timeouts are told apart by their context error.
			}
			return id, err
		}
		if count != 1 {
			return id, fmt.Errorf("audit of task %d found %d results; its result was published with it", id, count)
		}
		return id, nil
	}
	err = consume(q, auditJobType, func(ctx context.Context, tx *sql.Tx, payload []byte) error {
		id, err := audit(ctx, tx, payload)
		if err == nil {
			e.emit("audit", id, "")
		}
		return err
	}, opts...)
	if err != nil {
		return err
	}
	// The kept job fails each attempt, with text PostgreSQL cannot store as is, and so ends in the
	// DLQ after one retry. Cleanup must then keep it, whatever the time zones (see verifyKept).
	err = consume(q, keptJobType, func(ctx context.Context, _ *sql.Tx, _ []byte) error {
		info, _ := sqlq.JobInfoFromContext(ctx)
		e.emit("kept_attempt", -1, fmt.Sprintf("retry=%d", info.RetryCount))
		return errors.New(keptFailure + faultText)
		// Each SQLite poll takes the write lock too, even when it finds nothing (see DLQ cleanup).
	}, sqlq.WithConsumerMaxRetries(1), sqlq.WithConsumerPollInterval(50*c.Poll))
	if err != nil {
		return err
	}
	if sideQ != nil {
		err = consume(sideQ, sideAuditJobType, func(ctx context.Context, tx *sql.Tx, payload []byte) error {
			id, err := audit(ctx, tx, payload)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO side.chaos_side_effects VALUES (?)`, id); err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return fmt.Errorf("side audit write: %w", ctxErr)
				}
				return err
			}
			e.emit("side_audit", id, "")
			return nil
		}, opts...)
		if err != nil {
			return err
		}
	}
	e.emit("ready", -1, "")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if c.SaturatePoolOnShutdown {
				saturatePool(appCtx, c, db, q, &app, e)
			}
			if c.ShutdownOnSignal {
				<-shutdownDone
				// Services go on closing other resources: no handler may start by then.
				time.Sleep(10 * c.Poll)
			}
			return nil
		case <-ticker.C:
			metrics.emit(e, db)
		}
	}
}

// saturatePool holds every pool connection and starts an unrelated queue call
// whose context ends only after Shutdown, like a draining service whose
// in-flight requests still use the database.
func saturatePool(appCtx context.Context, c Config, db *sql.DB, q sqlq.JobsQueue, app *sync.WaitGroup, e *emitter) {
	for range c.Pool {
		conn, err := db.Conn(appCtx)
		if err != nil {
			return
		}
		app.Go(func() {
			<-appCtx.Done()
			_ = conn.Close()
		})
	}
	app.Go(func() { _, _ = q.GetDeadLetterJobs(appCtx, jobType, 1) })
	e.emit("pool_saturated", -1, "")
	time.Sleep(4 * c.Poll) // Let the consumer reach its next poll while the pool is held.
}

func verify(ctx context.Context, c Config, db *sql.DB, interrupted map[int]bool, progress ...func(int)) error {
	rows, err := db.QueryContext(ctx, `SELECT task, value, outcome FROM chaos_ledger`)
	if err != nil {
		return err
	}
	counts := make([]int, len(c.Tasks))
	checked := 0
	for rows.Next() {
		var id int
		var value, outcome string
		if err = rows.Scan(&id, &value, &outcome); err != nil {
			break
		}
		if id < 0 || id >= len(c.Tasks) || outcome != "success" {
			err = fmt.Errorf("unexpected committed effect: task=%d outcome=%s", id, outcome)
			break
		}
		counts[id]++
		if value != expected(c.Tasks[id]) {
			err = fmt.Errorf("incorrect intermediate product for task %d", id)
			break
		}
		checked++
		for _, notify := range progress {
			notify(checked)
		}
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil {
		return err
	}
	for id, count := range counts {
		if count != 1 {
			return fmt.Errorf("task %d committed %d times; want exactly once", id, count)
		}
	}
	root := c.Tasks[len(c.Tasks)-1]
	var value string
	if err := db.QueryRowContext(ctx, placeholders(c, `SELECT value FROM chaos_ledger WHERE task=?`), root.ID).Scan(&value); err != nil {
		return err
	}
	if value != new(big.Int).MulRange(1, int64(c.N)).String() {
		return errors.New("incorrect factorial")
	}
	for _, query := range []string{
		`SELECT COUNT(*) FROM jobs WHERE processed_at IS NULL`,
		`SELECT COUNT(*) FROM jobs WHERE job_type NOT IN ('factorial', 'audit', 'side_audit')`,
		`SELECT COUNT(*) FROM dead_letter_queue WHERE job_type <> 'kept'`, // See verifyKept.
	} {
		var count int
		if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return fmt.Errorf("residual queue state (%d rows): %s", count, query)
		}
	}
	if err := verifyAudits(ctx, c, db); err != nil {
		return err
	}
	if err := verifySideEffects(ctx, c, interrupted); err != nil {
		return err
	}
	return checkDatabase(ctx, c, db)
}

// keptFailure starts the kept job's failure reason.
const keptFailure = "kept in the DLQ"

// verifyKept checks the kept job through the public API: dead-lettered after its retry, with its
// failure recorded, and with times from this run, read back in any time zone.
func verifyKept(ctx context.Context, q sqlq.JobsQueue, started time.Time) error {
	jobs, err := q.GetDeadLetterJobs(ctx, keptJobType, 10)
	if err != nil {
		return err
	}
	if len(jobs) != 1 {
		return fmt.Errorf("DLQ has %d kept jobs, want 1: cleanup must keep a job far younger than its retention", len(jobs))
	}
	j := jobs[0]
	// A failed move to the DLQ reschedules the job instead, which counts as a retry too.
	if j.RetryCount < 1 || !strings.HasPrefix(j.FailureReason, keptFailure) {
		return fmt.Errorf("kept job dead-lettered with retry count %d and reason %q; want at least 1 and its handler's error", j.RetryCount, j.FailureReason)
	}
	for name, at := range map[string]time.Time{"created": j.CreatedAt, "failed": j.FailedAt} {
		// A minute either way allows for clock differences between the database and the run.
		if at.Before(started.Add(-time.Minute)) || at.After(time.Now().Add(time.Minute)) {
			return fmt.Errorf("kept job %s at %s, outside this run (started %s)", name, at, started)
		}
	}
	return nil
}

// verifyAudits checks that, without injected faults, no audit was charged a
// failure other than a timeout. Retries are not counted, because short lock
// timeouts make factorial jobs retry even in healthy runs. Audit handlers only read,
// so any other recorded failure came from the queue, not the handler.
// last_error survives on processed jobs; failures that were overwritten or
// requeued from the DLQ are checked during recovery instead.
func verifyAudits(ctx context.Context, c Config, db *sql.DB) error {
	if c.Chaos > 0 {
		return nil // Network faults and process pauses legitimately fail completions.
	}
	rows, err := db.QueryContext(ctx, placeholders(c, `SELECT job_type, id, last_error FROM jobs WHERE job_type IN (?, ?) AND last_error IS NOT NULL`), auditJobType, sideAuditJobType)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var kind, reason string
		var id int64
		if err := rows.Scan(&kind, &id, &reason); err != nil {
			return err
		}
		if err := checkAuditFailure(kind, id, reason); err != nil {
			return err
		}
	}
	return rows.Err()
}
