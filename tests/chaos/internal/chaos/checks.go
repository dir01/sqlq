package chaos

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"
)

var errInstrumentation = errors.New("runtime instrumentation failure")

func signalExit(err error, signal syscall.Signal) bool {
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		return false
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == signal
}

// RuntimeChecks records the enabled instrumentation and shutdown diagnostics.
// Leak detection is reachability-based, not an assertion about raw goroutine counts.
type RuntimeChecks struct {
	GoVersion            string            `json:"go_version"`
	GOOS                 string            `json:"goos"`
	GOARCH               string            `json:"goarch"`
	Race                 bool              `json:"race"`
	BuildSettings        map[string]string `json:"build_settings"`
	LeakProfileAvailable bool              `json:"leak_profile_available"`
	Leaks                int               `json:"leaks"`
	Goroutines           int               `json:"goroutines"`
	Memory               *runtime.MemStats `json:"memory,omitempty"`
	Database             *sql.DBStats      `json:"database,omitempty"`
	InitialDatabase      *sql.DBStats      `json:"database_before_drain,omitempty"`
	Passed               bool              `json:"passed"`
	Error                string            `json:"error,omitempty"`
}

func checkRuntime(directory, prefix string, db *sql.DB, requireLeaks bool, drainBudget ...time.Duration) (report RuntimeChecks, resultErr error) {
	report = RuntimeChecks{GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, Race: raceEnabled, BuildSettings: make(map[string]string)}
	if build, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range build.Settings {
			report.BuildSettings[setting.Key] = setting.Value
		}
	}
	defer func() {
		report.Goroutines = runtime.NumGoroutine()
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		report.Memory = &memory
		report.Passed = resultErr == nil
		if resultErr != nil {
			report.Error = resultErr.Error()
		}
		resultErr = errors.Join(resultErr, writeJSON(filepath.Join(directory, prefix+"-runtime.json"), report))
	}()
	if db != nil {
		stats := db.Stats()
		report.InitialDatabase = &stats
		budget := 10 * time.Second
		if len(drainBudget) > 0 {
			budget = drainBudget[0]
		}
		drainCtx, cancel := context.WithTimeout(context.Background(), budget)
		final, err := waitClosedPool(drainCtx, db)
		cancel()
		report.Database = &final
		resultErr = errors.Join(resultErr, err)
	}
	leaks := pprof.Lookup("goroutineleak")
	report.LeakProfileAvailable = leaks != nil
	if leaks == nil {
		if requireLeaks {
			resultErr = errors.Join(resultErr, errors.New("goroutineleak profile unavailable: use Go 1.27+ or Go 1.26 with GOEXPERIMENT=goroutineleakprofile"))
		}
	} else {
		// WriteTo, unlike Count, runs the leak-detecting GC. debug=1 contains
		// only leak stacks; debug=2 would include all goroutines.
		resultErr = errors.Join(resultErr, writeProfile(leaks, filepath.Join(directory, prefix+"-goroutineleak.txt"), 1))
		report.Leaks = leaks.Count()
		if report.Leaks != 0 {
			resultErr = errors.Join(resultErr, fmt.Errorf("detected %d leaked goroutines", report.Leaks))
		}
	}
	resultErr = errors.Join(resultErr, writeProfile(pprof.Lookup("goroutine"), filepath.Join(directory, prefix+"-goroutines.txt"), 2))
	return report, resultErr
}

// DB.Close prevents new queries but does not wait for checked-out connections.
// In particular, context cancellation can still be finishing Tx.awaitDone's
// asynchronous rollback. Require eventual closure within a bounded deadline.
func waitClosedPool(ctx context.Context, db *sql.DB) (sql.DBStats, error) {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		stats := db.Stats()
		if stats.OpenConnections == 0 && stats.InUse == 0 {
			return stats, nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			stats = db.Stats()
			if stats.OpenConnections == 0 && stats.InUse == 0 {
				return stats, nil
			}
			return stats, fmt.Errorf("database pool did not drain: open=%d in-use=%d: %w", stats.OpenConnections, stats.InUse, ctx.Err())
		}
	}
}

func writeProfile(profile *pprof.Profile, path string, verbosity int) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	err = profile.WriteTo(f, verbosity)
	return errors.Join(err, f.Close())
}

func instrumentationFailure(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for _, marker := range []string{"WARNING: DATA RACE", "fatal error:", "ERROR: AddressSanitizer", "WARNING: ThreadSanitizer", "runtime error: load of", "runtime error: store to"} {
		if strings.Contains(string(b), marker) {
			return fmt.Errorf("%w: %s in %s", errInstrumentation, marker, path)
		}
	}
	return nil
}

func checkDatabase(ctx context.Context, c Config, db *sql.DB) error {
	if c.Driver == "sqlite" {
		rows, err := db.QueryContext(ctx, `PRAGMA integrity_check`)
		if err != nil {
			return err
		}
		count := 0
		for rows.Next() {
			var result string
			if err = rows.Scan(&result); err != nil {
				break
			}
			count++
			if result != "ok" {
				err = fmt.Errorf("SQLite integrity_check: %s", result)
				break
			}
		}
		if err == nil {
			err = rows.Err()
		}
		_ = rows.Close()
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("SQLite integrity_check returned %d rows", count)
		}
		rows, err = db.QueryContext(ctx, `PRAGMA foreign_key_check`)
		if err != nil {
			return err
		}
		violations := rows.Next()
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return err
		}
		if violations {
			return errors.New("SQLite foreign_key_check found violations")
		}
	}
	if stats := db.Stats(); stats.InUse != 0 {
		return fmt.Errorf("database connections still in use after verification: %d", stats.InUse)
	}
	return nil
}
