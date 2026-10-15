package chaos

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/mattn/go-sqlite3"
)

// Command builds a consumer command. Tests supply a test-binary helper; the CLI
// supplies its own worker subcommand. Each worker is an actual OS process.
type Command func(configPath string, id int, healthy bool) *exec.Cmd

type Summary struct {
	Phase               string         `json:"phase"`
	CompletedTasks      int            `json:"completed_tasks"`
	PendingJobs         int            `json:"pending_jobs"`
	DLQJobs             int            `json:"dlq_jobs"`
	RecoveryElapsed     time.Duration  `json:"recovery_elapsed_ns"`
	VerificationElapsed time.Duration  `json:"verification_elapsed_ns"`
	Seed                int64          `json:"seed"`
	Driver              string         `json:"driver"`
	N                   int            `json:"n"`
	Tasks               int            `json:"tasks"`
	Elapsed             time.Duration  `json:"elapsed_ns"`
	ChaosAttempts       int            `json:"chaos_attempts"`
	TasksPerSecond      float64        `json:"tasks_per_guest_second"`
	Coverage            map[string]int `json:"coverage"`
	Verified            bool           `json:"verified"`
	Passed              bool           `json:"passed"`
	RuntimeChecks       RuntimeChecks  `json:"runtime_checks"`
	Error               string         `json:"error,omitempty"`
}

// maxAttemptsPerRetry bounds how often a task can run at one retry count (see attemptsAt in Run).
const maxAttemptsPerRetry = 20

type child struct {
	cmd           *exec.Cmd
	done          chan error
	resume        time.Time
	log           *os.File
	forcedKill    bool
	requestedTerm bool
	checked       bool
	ready         bool
}

// Run requires a fresh artifacts directory. Database state and diagnostics are
// retained on both success and failure; PostgreSQL uses a unique schema.
func Run(ctx context.Context, c Config, command Command, observers ...func(Event)) (summary Summary, resultErr error) {
	if err := c.Validate(); err != nil {
		return summary, err
	}
	if err := os.Mkdir(c.Artifacts, 0700); err != nil {
		return summary, fmt.Errorf("create fresh artifacts directory: %w", err)
	}
	started := time.Now()
	var db *sql.DB
	var checkErr error
	summary = Summary{Phase: "initialization", PendingJobs: -1, DLQJobs: -1, Seed: c.Seed, Driver: c.Driver, N: c.N, Coverage: make(map[string]int)}
	defer func() {
		resultErr = errors.Join(resultErr, checkErr)
		summary.Passed = summary.Verified && resultErr == nil
		summary.Elapsed = time.Since(started)
		if summary.Elapsed > 0 {
			summary.TasksPerSecond = float64(summary.Tasks) / summary.Elapsed.Seconds()
		}
		if resultErr != nil {
			resultErr = fmt.Errorf("%s phase (committed=%d/%d pending=%d dlq=%d): %w", summary.Phase, summary.CompletedTasks, summary.Tasks, summary.PendingJobs, summary.DLQJobs, resultErr)
			summary.Error = resultErr.Error()
		}
		if err := writeJSON(filepath.Join(c.Artifacts, "summary.json"), summary); err != nil {
			resultErr = errors.Join(resultErr, err)
		}
	}()
	defer func() {
		var err error
		summary.RuntimeChecks, err = checkRuntime(c.Artifacts, "supervisor", db, c.RequireLeakCheck, c.Shutdown/2)
		summary.RuntimeChecks.Memory = nil // Full memory statistics stay in the artifact.
		resultErr = errors.Join(resultErr, err)
	}()
	c.Tasks = buildTasks(c)
	summary.Tasks = len(c.Tasks)
	if c.Driver == "sqlite" {
		if c.DSN != "" {
			return summary, errors.New("sqlite DSN is generated in the fresh artifacts directory")
		}
		abs, err := filepath.Abs(filepath.Join(c.Artifacts, "queue.db"))
		if err != nil {
			return summary, err
		}
		c.DSN = fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=%d&_synchronous=FULL", abs, c.LockTimeout.Milliseconds())
		c.SideDB = filepath.Join(filepath.Dir(abs), "side.db")
	} else {
		admin, err := openDB(c, false)
		if err != nil {
			return summary, err
		}
		defer func() { resultErr = errors.Join(resultErr, admin.Close()) }()
		c.Schema = fmt.Sprintf("chaos_%x_%x", uint64(c.Seed), uint64(time.Now().UnixNano()))
		if _, err := admin.ExecContext(ctx, `CREATE SCHEMA `+c.Schema); err != nil {
			return summary, err
		}
	}
	var err error
	db, err = openDB(c, false)
	if err != nil {
		return summary, err
	}
	defer func() { resultErr = errors.Join(resultErr, db.Close()) }()
	defer func() {
		snapshotCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := snapshot(snapshotCtx, db, filepath.Join(c.Artifacts, "database.json")); err != nil {
			resultErr = errors.Join(resultErr, err)
		}
	}()
	q, err := queue(c, db)
	if err != nil {
		return summary, err
	}
	defer shutdown(q)
	if err := initialize(ctx, c, db, q); err != nil {
		return summary, err
	}
	var network *proxy
	if c.Driver == "postgres" {
		network, c.WorkerDSN, err = newProxy(ctx, c.DSN)
		if err != nil {
			return summary, err
		}
		defer network.close()
	}
	configPath := filepath.Join(c.Artifacts, "config.json")
	if err := writeJSON(configPath, c); err != nil {
		return summary, err
	}
	eventLog, err := os.OpenFile(filepath.Join(c.Artifacts, "events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return summary, err
	}
	defer eventLog.Close()
	events := make(chan Event, 4096)
	children := make(map[int]*child)
	exercising := c.Chaos > 0
	// attemptsAt counts a task's attempts at each retry count since it was last requeued from the
	// DLQ (task -1 is the kept job). An attempt that ends with an error raises the count; only a
	// lost claim or a killed worker repeats one, and each of those waits for a claim to be
	// released or to expire. Far more repeats mean a failure was not recorded: the retry limit
	// does not hold.
	type attemptKey struct{ task, retry int }
	attemptsAt := make(map[attemptKey]int)
	commits := newCommitWatch()
	// shutDown holds the workers whose Shutdown returned: none may start a handler after it.
	shutDown := make(map[int]bool)
	var protocolErr error
	logEvent := func(e Event) {
		switch e.Kind {
		case "shutdown_returned":
			shutDown[e.Worker] = true
		case "attempt", "kept_attempt", "audit", "side_audit":
			// A worker's own events arrive in order.
			if shutDown[e.Worker] && protocolErr == nil {
				protocolErr = fmt.Errorf("worker %d ran a handler (%s, task %d) after its Shutdown returned", e.Worker, e.Kind, e.Task)
			}
		}
		commits.observe(e)
		if e.Kind == "attempt" || e.Kind == "kept_attempt" {
			var key attemptKey
			if _, err := fmt.Sscanf(e.Detail, "retry=%d", &key.retry); err == nil {
				key.task = e.Task
				attemptsAt[key]++
				if attemptsAt[key] == maxAttemptsPerRetry+1 && protocolErr == nil {
					protocolErr = fmt.Errorf("task %d ran %d times at retry count %d without being requeued; its failures are not being recorded", key.task, attemptsAt[key], key.retry)
				}
			}
		}
		if e.Kind == "ready" {
			if ch := children[e.Worker]; ch != nil {
				ch.ready = true
			}
		}
		if e.Kind == "check_failure" || e.Kind == "fatal" || e.Kind == "invalid_event" {
			checkErr = errors.Join(checkErr, fmt.Errorf("worker %d: %s", e.Worker, e.Detail))
		}
		if e.Kind == "checks_passed" {
			if ch := children[e.Worker]; ch != nil {
				ch.checked = true
			}
		}
		summary.Coverage[e.Kind]++
		if exercising && e.Kind == "attempt" {
			summary.ChaosAttempts++
		}
		if err := json.NewEncoder(eventLog).Encode(e); err != nil {
			checkErr = errors.Join(checkErr, fmt.Errorf("write events: %w", err))
		}
		for _, observe := range observers {
			observe(e)
		}
	}
	summary.Phase = "chaos"
	logEvent(Event{At: time.Now(), Worker: -1, Kind: "phase_start", Task: -1, Detail: "chaos"})
	nextID := 0
	registrations := 0
	start := func(healthy bool) error {
		id := nextID
		nextID++
		cmd := command(configPath, id, healthy)
		if cmd.Env == nil {
			cmd.Env = os.Environ()
		}
		// Keep findings on stderr so a chaos kill cannot erase evidence or
		// turn a sanitizer failure into an accepted intentional process exit.
		cmd.Env = append(cmd.Env, "GORACE=halt_on_error=1 atexit_sleep_ms=0 log_path=stderr exitcode=66", "GOTRACEBACK=all")
		if c.AppTimeZone != "" {
			cmd.Env = append(cmd.Env, "TZ="+c.AppTimeZone)
		}
		log, err := os.OpenFile(filepath.Join(c.Artifacts, fmt.Sprintf("worker-%d.log", id)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		cmd.Stderr = log
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			_ = log.Close()
			return err
		}
		if err := cmd.Start(); err != nil {
			_ = log.Close()
			return err
		}
		ch := &child{cmd: cmd, done: make(chan error, 1), log: log}
		children[id] = ch
		go func() {
			scanner := bufio.NewScanner(stdout)
			// A bounded metrics snapshot can exceed Scanner's default 64 KiB.
			scanner.Buffer(make([]byte, 4096), 1024*1024)
			for scanner.Scan() {
				var e Event
				if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
					e = Event{At: time.Now(), Worker: id, Kind: "invalid_event", Detail: err.Error()}
				}
				select {
				case events <- e:
				case <-ctx.Done():
				}
			}
			scanErr := scanner.Err()
			waitErr := cmd.Wait()
			_ = log.Close()
			ch.done <- errors.Join(scanErr, waitErr, instrumentationFailure(log.Name()))
		}()
		logEvent(Event{At: time.Now(), Worker: id, Kind: "start", Task: -1, Detail: fmt.Sprintf("healthy=%t", healthy)})
		return nil
	}
	stopAll := func() {
		for _, ch := range children {
			ch.forcedKill = true
			_ = ch.cmd.Process.Signal(syscall.SIGCONT)
			_ = ch.cmd.Process.Kill()
		}
		for id, ch := range children {
		wait:
			for {
				select {
				case err := <-ch.done:
					if errors.Is(err, errInstrumentation) {
						checkErr = errors.Join(checkErr, err)
					}
					delete(children, id)
					break wait
				case e := <-events:
					logEvent(e)
				}
			}
		}
	}
	shutdownAll := func() error {
		deadline := time.NewTimer(c.Shutdown)
		defer deadline.Stop()
		// A newly started process may still have the OS's default SIGTERM
		// action. Wait for signal handler/consumer registration first.
		readyPoll := time.NewTicker(10 * time.Millisecond)
		defer readyPoll.Stop()
		for {
			allReady := true
			for id, ch := range children {
				select {
				case err := <-ch.done:
					delete(children, id)
					return fmt.Errorf("worker %d exited before final shutdown: %v", id, err)
				default:
				}
				allReady = allReady && ch.ready
			}
			if allReady {
				break
			}
			select {
			case e := <-events:
				logEvent(e)
			case <-readyPoll.C:
			case <-deadline.C:
				return fmt.Errorf("workers did not become ready within %s", c.Shutdown)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		for _, ch := range children {
			_ = ch.cmd.Process.Signal(syscall.SIGCONT)
			_ = ch.cmd.Process.Signal(syscall.SIGTERM)
		}
		for id, ch := range children {
		wait:
			for {
				select {
				case err := <-ch.done:
					for len(events) > 0 {
						logEvent(<-events)
					}
					if err != nil {
						checkErr = errors.Join(checkErr, fmt.Errorf("graceful worker %d exit: %w", id, err))
					}
					if !ch.checked {
						checkErr = errors.Join(checkErr, fmt.Errorf("worker %d did not finish runtime checks", id))
					}
					delete(children, id)
					break wait
				case e := <-events:
					logEvent(e)
				case <-deadline.C:
					// SIGQUIT asks the Go runtime for stacks before the fallback kill.
					for _, remaining := range children {
						_ = remaining.cmd.Process.Signal(syscall.SIGQUIT)
					}
					// Let the dumps reach the worker logs; stopAll would otherwise
					// kill the workers before they finish writing them.
					grace := time.NewTimer(2 * time.Second)
					defer grace.Stop()
				dump:
					for id, remaining := range children {
						for {
							select {
							case <-remaining.done:
								delete(children, id)
								continue dump
							case e := <-events:
								logEvent(e)
							case <-grace.C:
								break dump
							}
						}
					}
					return fmt.Errorf("consumer shutdown exceeded %s; stacks are in the worker logs", c.Shutdown)
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		return checkErr
	}
	defer stopAll()
	if c.Chaos > 0 {
		for range c.Processes {
			if err := start(false); err != nil {
				return summary, err
			}
		}
	}
	r := rand.New(rand.NewSource(c.Seed))
	ticker := time.NewTicker(c.FaultInterval)
	defer ticker.Stop()
	// Give workers a bounded opportunity to reach a transactional write before
	// randomized signals can starve startup/long-wait attempts. The full chaos
	// budget starts at the targeted write-boundary kill, not process launch.
	chaosEnd := time.NewTimer(max(c.Shutdown, c.JobTimeout+c.ClaimTimeout))
	defer chaosEnd.Stop()
	// Target a write boundary once, then use independent randomized signals.
	targetKilled := false
chaosLoop:
	for c.Chaos > 0 {
		select {
		case <-ctx.Done():
			return summary, ctx.Err()
		case <-chaosEnd.C:
			if !targetKilled {
				return summary, errors.New("warm-up ended without a transactional write; no write-boundary fault exercised")
			}
			break chaosLoop
		case e := <-events:
			logEvent(e)
			if e.Kind == "fatal" || e.Kind == "invalid_event" {
				return summary, fmt.Errorf("worker protocol failure: %s", e.Detail)
			}
			if protocolErr != nil {
				return summary, protocolErr
			}
			if !targetKilled && e.Kind == "written" {
				if ch := children[e.Worker]; ch != nil {
					_ = ch.cmd.Process.Kill()
					ch.forcedKill = true
					targetKilled = true
					chaosEnd.Reset(c.Chaos)
					logEvent(Event{At: time.Now(), Worker: -1, Kind: "warmup_complete", Task: -1})
					logEvent(Event{At: time.Now(), Worker: e.Worker, Kind: "kill_at_write", Task: e.Task})
				}
			}
			// Stop every third worker that shuts down on a signal while it registers consumers: after
			// its first, before the rest (see ShutdownOnSignal).
			if targetKilled && e.Kind == "registering" && e.Detail == auditJobType {
				registrations++
				if ch := children[e.Worker]; ch != nil && registrations%3 == 1 && !ch.forcedKill && !ch.requestedTerm {
					ch.requestedTerm = true
					_ = ch.cmd.Process.Signal(syscall.SIGTERM)
					logEvent(Event{At: time.Now(), Worker: e.Worker, Kind: "term_at_registration", Task: -1})
				}
			}
		case <-ticker.C:
			for id, ch := range children {
				select {
				case exitErr := <-ch.done:
					delete(children, id)
					if errors.Is(exitErr, errInstrumentation) || exitErr != nil && !ch.forcedKill && !(ch.requestedTerm && signalExit(exitErr, syscall.SIGTERM)) {
						return summary, fmt.Errorf("unexpected worker %d exit: %w", id, exitErr)
					}
					logEvent(Event{At: time.Now(), Worker: id, Kind: "exit", Task: -1, Detail: fmt.Sprint(exitErr)})
					delete(children, id)
				default:
					if !ch.resume.IsZero() && !time.Now().Before(ch.resume) {
						_ = ch.cmd.Process.Signal(syscall.SIGCONT)
						ch.resume = time.Time{}
						logEvent(Event{At: time.Now(), Worker: id, Kind: "resume", Task: -1})
					}
				}
			}
			for len(children) < c.Processes {
				if err := start(false); err != nil {
					return summary, err
				}
			}
			// Exercise running handlers rather than repeatedly killing slow
			// instrumented process startups before they can touch the queue.
			if !targetKilled {
				continue
			}
			// Sort-free deterministic selection: choose from monotonically assigned IDs.
			ids := make([]int, 0, len(children))
			for id := 0; id < nextID; id++ {
				if children[id] != nil {
					ids = append(ids, id)
				}
			}
			id := ids[r.Intn(len(ids))]
			ch := children[id]
			switch r.Intn(5) {
			case 0:
				ch.forcedKill = true
				_ = ch.cmd.Process.Kill()
				logEvent(Event{At: time.Now(), Worker: id, Kind: "kill", Task: -1})
			case 1:
				if ch.resume.IsZero() {
					_ = ch.cmd.Process.Signal(syscall.SIGSTOP)
					ch.resume = time.Now().Add(c.ClaimTimeout + 2*c.Poll)
					logEvent(Event{At: time.Now(), Worker: id, Kind: "pause", Task: -1})
				}
			case 2:
				ch.requestedTerm = true
				_ = ch.cmd.Process.Signal(syscall.SIGTERM)
				logEvent(Event{At: time.Now(), Worker: id, Kind: "shutdown", Task: -1})
			}
			if network != nil {
				mode := r.Intn(5)
				// Compute deadlines may be minutes under emulation. Do not turn
				// that into minute-long delays on every protocol chunk.
				delayUnit := min(c.JobTimeout, time.Second)
				upload := time.Duration(r.Intn(3)) * delayUnit / 3
				download := time.Duration(r.Intn(3)) * delayUnit / 3
				network.set(upload, download, mode == 0, mode == 1)
				logEvent(Event{At: time.Now(), Worker: -1, Kind: "network", Task: -1, Detail: fmt.Sprintf("upload=%s download=%s partition=%t reset=%t", upload, download, mode == 0, mode == 1)})
			}
		}
	}
	// Healing never manufactures tasks or edits results. Restart with healthy
	// callbacks and recover DLQ work using the public queue API.
	logEvent(Event{At: time.Now(), Worker: -1, Kind: "heal", Task: -1})
	if network != nil {
		network.set(0, 0, false, true)
	}
	stopAll()
	exercising = false
	if c.Chaos > 0 && summary.ChaosAttempts == 0 {
		return summary, errors.New("chaos budget ended without any observed handler attempts; increase it")
	}
	for range c.Processes {
		if err := start(true); err != nil {
			return summary, err
		}
	}
	summary.Phase = "recovery"
	logEvent(Event{At: time.Now(), Worker: -1, Kind: "phase_start", Task: -1, Detail: "recovery"})
	recoveryStarted := time.Now()
	verificationStarted := time.Time{}
	defer func() {
		if verificationStarted.IsZero() {
			summary.RecoveryElapsed = time.Since(recoveryStarted)
		} else {
			summary.VerificationElapsed = time.Since(verificationStarted)
		}
	}()
	lastProgress := time.Time{}
	recoveryCtx, cancel := context.WithTimeout(ctx, c.Recovery)
	defer cancel()
	poll := time.NewTicker(c.Poll)
	defer poll.Stop()
	for {
		select {
		case <-recoveryCtx.Done():
			return summary, fmt.Errorf("convergence deadline: %w", recoveryCtx.Err())
		case e := <-events:
			logEvent(e)
			if e.Kind == "fatal" || e.Kind == "invalid_event" {
				return summary, fmt.Errorf("worker protocol failure: %s", e.Detail)
			}
			if protocolErr != nil {
				return summary, protocolErr
			}
		case <-poll.C:
			for id, ch := range children {
				select {
				case err := <-ch.done:
					delete(children, id)
					return summary, fmt.Errorf("healthy worker %d exited: %v", id, err)
				default:
				}
			}
			jobs, err := q.GetDeadLetterJobs(recoveryCtx, jobType, 1000)
			if err != nil {
				return summary, err
			}
			for _, kind := range []string{auditJobType, sideAuditJobType} {
				audits, err := q.GetDeadLetterJobs(recoveryCtx, kind, 1000)
				if err != nil {
					return summary, err
				}
				if c.Chaos == 0 {
					// Requeueing discards the reason, so check it first (see verifyAudits).
					for _, j := range audits {
						if err := checkAuditFailure(kind, j.OriginalID, j.FailureReason); err != nil {
							return summary, err
						}
					}
				}
				jobs = append(jobs, audits...)
			}
			summary.DLQJobs = len(jobs)
			for _, j := range jobs {
				if err := q.RequeueDeadLetterJob(recoveryCtx, j.OriginalID); err != nil {
					// A stale read snapshot is the queue's own transaction upgrading a read, which no
					// busy timeout explains (see observedSQLiteDriver).
					var sqliteErr sqlite3.Error
					if errors.As(err, &sqliteErr) && sqliteErr.ExtendedCode == sqlite3.ErrBusySnapshot {
						return summary, fmt.Errorf("requeue of DLQ job %d upgraded a stale SQLite read snapshot: %w", j.OriginalID, err)
					}
					// Lock waits can time out under the randomized short lock timeouts. Retry
					// the public operation on the next recovery poll; never repair its tables directly.
					logEvent(Event{At: time.Now(), Worker: -1, Kind: "requeue_error", Task: -1, Detail: err.Error()})
					break
				}
				// Requeueing resets the retry count, so the task starts counting again.
				var task int
				if j.JobType == jobType && json.Unmarshal(j.Payload, &task) == nil {
					for key := range attemptsAt {
						if key.task == task {
							delete(attemptsAt, key)
						}
					}
				}
				logEvent(Event{At: time.Now(), Worker: -1, Kind: "requeue", Task: -1})
			}
			var count, pending int
			if err := db.QueryRowContext(recoveryCtx, `SELECT COUNT(*) FROM chaos_ledger`).Scan(&count); err != nil {
				return summary, err
			}
			summary.CompletedTasks = count
			if count > len(c.Tasks) {
				return summary, fmt.Errorf("too many committed effects: %d > %d", count, len(c.Tasks))
			}
			if err := db.QueryRowContext(recoveryCtx, `SELECT COUNT(*) FROM jobs WHERE processed_at IS NULL`).Scan(&pending); err != nil {
				return summary, err
			}
			summary.PendingJobs = pending
			// The kept job stays in the DLQ (see verifyKept).
			if err := db.QueryRowContext(recoveryCtx, placeholders(c, `SELECT COUNT(*) FROM dead_letter_queue WHERE job_type <> ?`), keptJobType).Scan(&summary.DLQJobs); err != nil {
				return summary, err
			}
			if time.Since(lastProgress) >= time.Second {
				logEvent(Event{At: time.Now(), Worker: -1, Kind: "progress", Task: -1, Detail: fmt.Sprintf("recovery committed=%d/%d pending=%d dlq=%d", count, len(c.Tasks), pending, summary.DLQJobs)})
				state, err := backlog(recoveryCtx, c, db)
				if err != nil {
					return summary, err
				}
				logEvent(Event{At: time.Now(), Worker: -1, Kind: "backlog", Task: -1, Detail: state})
				lastProgress = time.Now()
			}
			// Requeued DLQ jobs must also run: audits and side audits can still be in the DLQ
			// after every result is committed.
			if count == len(c.Tasks) && pending == 0 && summary.DLQJobs == 0 {
				summary.Phase = "shutdown"
				logEvent(Event{At: time.Now(), Worker: -1, Kind: "phase_start", Task: -1, Detail: "shutdown"})
				if err := shutdownAll(); err != nil {
					return summary, err
				}
				// Flush process events after stdout readers have finished.
				for len(events) > 0 {
					logEvent(<-events)
				}
				if protocolErr != nil {
					return summary, protocolErr
				}
				summary.RecoveryElapsed = time.Since(recoveryStarted)
				cancel()
				summary.Phase = "verification"
				verificationStarted = time.Now()
				logEvent(Event{At: time.Now(), Worker: -1, Kind: "phase_start", Task: -1, Detail: "verification"})
				verificationCtx, stopVerification := context.WithTimeout(ctx, c.Verification)
				lastCheck := time.Time{}
				err := verify(verificationCtx, c, db, commits.interrupted(c.ClaimTimeout), func(checked int) {
					if time.Since(lastCheck) >= time.Second || checked == len(c.Tasks) {
						logEvent(Event{At: time.Now(), Worker: -1, Kind: "progress", Task: -1, Detail: fmt.Sprintf("verification checked=%d/%d", checked, len(c.Tasks))})
						lastCheck = time.Now()
					}
				})
				if err == nil {
					err = verifyKept(verificationCtx, q, started)
				}
				stopVerification()
				if err != nil {
					return summary, err
				}
				summary.Verified = true
				summary.Phase = "complete"
				return summary, nil
			}
		}
	}
}

func writeJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0600)
}

func snapshot(ctx context.Context, db *sql.DB, path string) error {
	data := make(map[string][]map[string]any)
	for _, table := range []string{"jobs", "dead_letter_queue", "chaos_ledger"} {
		rows, err := db.QueryContext(ctx, `SELECT * FROM `+table)
		if err != nil {
			return err
		}
		columns, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			return err
		}
		data[table] = nil
		for rows.Next() {
			values := make([]any, len(columns))
			dest := make([]any, len(columns))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := rows.Scan(dest...); err != nil {
				_ = rows.Close()
				return err
			}
			record := make(map[string]any)
			for i, name := range columns {
				if b, ok := values[i].([]byte); ok {
					record[name] = string(b)
				} else {
					record[name] = values[i]
				}
			}
			data[table] = append(data[table], record)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return err
		}
	}
	return writeJSON(path, data)
}
