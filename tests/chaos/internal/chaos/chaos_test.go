package chaos

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestWorkerProcess(t *testing.T) {
	if os.Getenv("SQLQ_CHAOS_HELPER") != "1" {
		return
	}
	if os.Getenv("SQLQ_CHAOS_START_FAILURE") == "1" {
		os.Exit(7)
	}
	b, err := os.ReadFile(os.Getenv("SQLQ_CHAOS_CONFIG"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	id, _ := strconv.Atoi(os.Getenv("SQLQ_CHAOS_ID"))
	if os.Getenv("SQLQ_CHAOS_INJECT_LEAK") == "1" {
		intentionalLeak()
	}
	err = Worker(context.Background(), c, id, os.Getenv("SQLQ_CHAOS_HEALTHY") == "true", os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if os.Getenv("SQLQ_CHAOS_HANG_SHUTDOWN") == "1" {
		for {
			time.Sleep(time.Hour)
		}
	}
	os.Exit(0)
}

func intentionalLeak() {
	ready := make(chan struct{})
	go func() { unreachable := make(chan struct{}); close(ready); <-unreachable }()
	<-ready
}

func helperCommand(path string, id int, healthy bool) *exec.Cmd {
	executable := os.Args[0]
	if runtime.GOOS == "linux" {
		// Long runs can outlive external cleanup of go test's /tmp/go-build
		// directory. The running executable inode remains available through
		// procfs even after its original pathname has been removed.
		executable = "/proc/self/exe"
	}
	cmd := exec.Command(executable, "-test.run=^TestWorkerProcess$")
	cmd.Env = append(os.Environ(), "SQLQ_CHAOS_HELPER=1", "SQLQ_CHAOS_CONFIG="+path, "SQLQ_CHAOS_ID="+strconv.Itoa(id), "SQLQ_CHAOS_HEALTHY="+strconv.FormatBool(healthy))
	return cmd
}

func TestChaosWarmupBeforeRandomFaults(t *testing.T) {
	c := DefaultConfig()
	c.Seed, c.N, c.MaxRange = 4, 20, 1
	c.Processes, c.Concurrency, c.Prefetch = 1, 1, 1
	c.JobTimeout, c.ClaimTimeout = 40*time.Millisecond, 250*time.Millisecond
	c.Chaos, c.FaultInterval = 100*time.Millisecond, 20*time.Millisecond
	c.Artifacts = filepath.Join(t.TempDir(), "warmup")
	var warmup, healed time.Time
	targeted := false
	summary, err := Run(t.Context(), c, helperCommand, func(e Event) {
		switch e.Kind {
		case "kill_at_write":
			targeted = true
		case "warmup_complete":
			warmup = e.At
		case "heal":
			healed = e.At
		case "kill", "pause", "shutdown", "network":
			if !targeted {
				t.Error("random fault preceded targeted write-boundary kill")
			}
		}
	})
	if err != nil || !summary.Passed {
		t.Fatalf("run failed: %v; %+v", err, summary)
	}
	if warmup.IsZero() || healed.Sub(warmup) < c.Chaos {
		t.Fatalf("chaos budget was consumed during warmup: warmup=%s heal=%s", warmup, healed)
	}
}

func TestOracleRejectsCorruption(t *testing.T) {
	for _, kind := range []string{"intermediate", "duplicate", "failed-effect", "poison", "missing", "charged-audit", "charged-side-audit", "lost-side-effect", "duplicate-side-effect"} {
		t.Run(kind, func(t *testing.T) {
			c := DefaultConfig()
			c.N = 20
			c.MaxRange = 4
			c.Chaos = 0 // Fault-free runs also check audit failures.
			c.Seed = 16 // Side audits task 0, so the small fixture has one.
			c.Tasks = buildTasks(c)
			dir := t.TempDir()
			c.DSN = "file:" + filepath.Join(dir, "oracle.db")
			c.SideDB = filepath.Join(dir, "side.db")
			db, err := openDB(c, false)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			q, err := queue(c, db)
			if err != nil {
				t.Fatal(err)
			}
			defer shutdown(q)
			if err := initialize(t.Context(), c, db, q); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`DELETE FROM jobs`); err != nil {
				t.Fatal(err)
			}
			side, err := sql.Open("sqlite3", "file:"+c.SideDB)
			if err != nil {
				t.Fatal(err)
			}
			defer side.Close()
			for _, task := range c.Tasks {
				if _, err := db.Exec(`INSERT INTO chaos_ledger VALUES (?, ?, 'success')`, task.ID, expected(task)); err != nil {
					t.Fatal(err)
				}
				if !sideAudited(c, task.ID) {
					continue
				}
				if _, err := side.Exec(`INSERT INTO chaos_side_effects VALUES (?)`, task.ID); err != nil {
					t.Fatal(err)
				}
			}
			// A side audit that once lost a lock wait is a valid outcome too.
			if err := q.Publish(t.Context(), sideAuditJobType, 0); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE jobs SET processed_at=1, last_error='failed to mark job 2 processed: database is locked' WHERE job_type='side_audit'`); err != nil {
				t.Fatal(err)
			}
			// A processed audit that once timed out is a valid outcome.
			if err := q.Publish(t.Context(), auditJobType, 0); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE jobs SET processed_at=1, last_error='context deadline exceeded' WHERE job_type='audit'`); err != nil {
				t.Fatal(err)
			}
			if err := verify(t.Context(), c, db, nil); err != nil {
				t.Fatalf("valid fixture rejected: %v", err)
			}
			var query string
			switch kind {
			case "intermediate":
				query = `UPDATE chaos_ledger SET value='123' WHERE task=0`
			case "duplicate":
				query = `INSERT INTO chaos_ledger SELECT * FROM chaos_ledger WHERE task=0`
			case "failed-effect":
				query = `INSERT INTO chaos_ledger VALUES (0, '', 'failed-hook')`
			case "missing":
				query = `DELETE FROM chaos_ledger WHERE task=0`
			case "charged-audit":
				query = `UPDATE jobs SET last_error='failed to mark job 1 processed: database is locked' WHERE job_type='audit'`
			case "charged-side-audit":
				query = `UPDATE jobs SET last_error='invalid audit payload' WHERE job_type='side_audit'`
			case "lost-side-effect", "duplicate-side-effect":
				sideTask := 0
				for !sideAudited(c, sideTask) {
					sideTask++
				}
				sideQuery := `DELETE FROM chaos_side_effects WHERE task=?`
				if kind == "duplicate-side-effect" {
					sideQuery = `INSERT INTO chaos_side_effects VALUES (?)`
				}
				if _, err := side.Exec(sideQuery, sideTask); err != nil {
					t.Fatal(err)
				}
			case "poison":
				if err := q.Publish(t.Context(), "poison", 0); err != nil {
					t.Fatal(err)
				}
			}
			if query != "" {
				if _, err := db.Exec(query); err != nil {
					t.Fatal(err)
				}
			}
			if err := verify(t.Context(), c, db, nil); err == nil {
				t.Fatalf("oracle accepted %s corruption", kind)
			}
		})
	}
}

func TestClockProbeSQLite(t *testing.T) {
	c := DefaultConfig()
	if err := ClockProbe(t.Context(), c, 20*time.Millisecond, os.Stdout); err != nil {
		t.Fatal(err)
	}
}

func TestVerificationHasSeparateDeadline(t *testing.T) {
	c := DefaultConfig()
	c.N = 100
	c.MaxRange = 20
	c.Chaos = 0
	c.Verification = time.Nanosecond
	c.Artifacts = filepath.Join(t.TempDir(), "run")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	summary, err := Run(ctx, c, helperCommand)
	if err == nil || summary.Phase != "verification" || summary.CompletedTasks != summary.Tasks || summary.PendingJobs != 0 {
		t.Fatalf("want verification timeout after complete recovery, got summary=%+v err=%v", summary, err)
	}
}

func TestRuntimeRejectsLeakedWorker(t *testing.T) {
	if pprof.Lookup("goroutineleak") == nil {
		t.Skip("requires Go 1.27 or the Go 1.26 leak-profile experiment")
	}
	c := DefaultConfig()
	c.Chaos = 0
	c.N = 100
	c.MaxRange = 20
	c.Processes = 1
	c.RequireLeakCheck = true
	c.Artifacts = filepath.Join(t.ArtifactDir(), "leaked-worker")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	summary, err := Run(ctx, c, func(path string, id int, healthy bool) *exec.Cmd {
		cmd := helperCommand(path, id, healthy)
		cmd.Env = append(cmd.Env, "SQLQ_CHAOS_INJECT_LEAK=1")
		return cmd
	})
	if err == nil || summary.Passed || summary.Coverage["check_failure"] == 0 {
		t.Fatalf("leak accepted: summary=%+v err=%v", summary, err)
	}
	b, err := os.ReadFile(filepath.Join(c.Artifacts, "worker-0-runtime.json"))
	if err != nil {
		t.Fatal(err)
	}
	var checks RuntimeChecks
	if err := json.Unmarshal(b, &checks); err != nil {
		t.Fatal(err)
	}
	if checks.Leaks == 0 || checks.Passed {
		t.Fatalf("leak was not detected: %+v", checks)
	}
}

func TestInstrumentationEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker.log")
	if err := os.WriteFile(path, []byte("WARNING: DATA RACE\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := instrumentationFailure(path); err == nil {
		t.Fatal("race report accepted")
	}
	if err := os.WriteFile(path, []byte("sqlq: panic recovered in handler: injected panic\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := instrumentationFailure(path); err != nil {
		t.Fatalf("expected injected failure rejected: %v", err)
	}
}

func TestUnexpectedWorkerExitIsReaped(t *testing.T) {
	c := DefaultConfig()
	c.N = 100
	c.MaxRange = 20
	c.Processes = 1
	c.Artifacts = filepath.Join(t.ArtifactDir(), "early-exit")
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, err := Run(ctx, c, func(path string, id int, healthy bool) *exec.Cmd {
		cmd := helperCommand(path, id, healthy)
		cmd.Env = append(cmd.Env, "SQLQ_CHAOS_START_FAILURE=1")
		return cmd
	})
	if err == nil || !strings.Contains(err.Error(), "unexpected worker") {
		t.Fatalf("expected early-exit failure, got %v", err)
	}
}

func TestShutdownDeadlineRejectsHungProcess(t *testing.T) {
	c := DefaultConfig()
	c.N = 100
	c.MaxRange = 20
	c.Processes = 1
	c.Chaos = 0
	c.Shutdown = 200 * time.Millisecond
	c.Artifacts = filepath.Join(t.ArtifactDir(), "hung-shutdown")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	summary, err := Run(ctx, c, func(path string, id int, healthy bool) *exec.Cmd {
		cmd := helperCommand(path, id, healthy)
		cmd.Env = append(cmd.Env, "SQLQ_CHAOS_HANG_SHUTDOWN=1")
		return cmd
	})
	if err == nil || summary.Passed || !strings.Contains(err.Error(), "shutdown exceeded") {
		t.Fatalf("hung shutdown accepted: summary=%+v err=%v", summary, err)
	}
}
