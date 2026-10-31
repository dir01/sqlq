package chaos

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	simulationSeed     = flag.Int64("simulation-seed", 42, "first simulation seed")
	simulationSeeds    = flag.Int("simulation-seeds", 1, "number of consecutive simulation seeds")
	simulationN        = flag.Int("simulation-n", 1000, "factorial input (1..50000)")
	simulationMaxRange = flag.Int("simulation-max-range", 8, "maximum factorial leaf range size")
	simulationChaos    = flag.Duration("simulation-chaos", 1500*time.Millisecond, "fault injection duration per chaos profile")
	simulationDuration = flag.Duration("simulation-duration", 0, "keep exploring seeds for this duration, then finish the active profile; overrides simulation-seeds")
	simulationRecovery = flag.Duration("simulation-recovery", 30*time.Second, "maximum recovery duration per profile")
)

func TestSimulationSQLite(t *testing.T) {
	runSimulation(t, "sqlite", "")
}

func TestSimulationPSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("PostgreSQL simulation requires Docker; run make test-simulation-psql")
	}
	// One container serves all profiles/seeds. Each Run creates a unique schema;
	// terminating the container removes all database state, including failed runs.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:17-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "simulation",
				"POSTGRES_PASSWORD": "simulation",
				"POSTGRES_DB":       "simulation",
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).
				WithStartupTimeout(time.Minute),
		},
		Started: true,
	})
	// A failed start can still return a container that needs cleanup.
	if container != nil {
		testcontainers.CleanupContainer(t, container)
	}
	if err != nil {
		t.Fatalf("start PostgreSQL container (Docker must be running): %v", err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	dsn := (&url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword("simulation", "simulation"),
		Host:     net.JoinHostPort(host, port.Port()),
		Path:     "/simulation",
		RawQuery: "sslmode=disable",
	}).String()
	runSimulation(t, "postgres", dsn)
}

func runSimulation(t *testing.T, driver, dsn string) {
	t.Helper()
	if *simulationDuration < 0 {
		t.Fatal("simulation-duration must be nonnegative")
	}
	if *simulationDuration == 0 && (*simulationSeeds < 1 || *simulationSeeds > 1000) {
		t.Fatal("simulation-seeds must be between 1 and 1000")
	}
	if *simulationChaos <= 0 {
		t.Fatal("simulation-chaos must be positive; the healthy profile already runs without faults")
	}
	started := time.Now()
	expired := func() bool {
		return *simulationDuration > 0 && time.Since(started) >= *simulationDuration
	}
	defer func() { t.Logf("simulation runtime: %s", time.Since(started)) }()
	for offset := 0; *simulationDuration > 0 || offset < *simulationSeeds; offset++ {
		if expired() {
			break
		}
		seed := *simulationSeed + int64(offset)
		passed := t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			for _, profile := range []struct {
				name    string
				retries int32
				chaos   bool
				cleanup bool
			}{
				{"healthy", 2, false, false},
				{"finite-dlq", 0, true, false},
				{"unlimited-cleanup", -1, true, true},
			} {
				if expired() || t.Failed() {
					return
				}
				t.Run(profile.name, func(t *testing.T) {
					c := DefaultConfig()
					c.Driver, c.DSN, c.Seed = driver, dsn, seed
					c = RandomizedConfig(c)
					c.N, c.MaxRange = *simulationN, *simulationMaxRange
					// Keep competing workers alive when another is killed/paused.
					c.Processes = max(2, c.Processes)
					// Native real-time budgets: enough time for SQL work, short
					// enough that intentionally abandoned claims recover quickly.
					c.JobTimeout /= 4
					c.Poll = 5 * time.Millisecond
					c.Backoff = 5 * time.Millisecond
					c.MaxRetries = profile.retries
					c.Push = driver == "sqlite" && profile.cleanup
					c.Cleanup = 0
					if profile.cleanup {
						c.Cleanup = 20 * time.Millisecond
					}
					c.Chaos = 0
					if profile.chaos {
						c.Chaos = *simulationChaos
					}
					c.FaultInterval = 80 * time.Millisecond
					c.Recovery = *simulationRecovery
					c.Verification = 10 * time.Second
					// Above the library's own 5s claim-release budget, which a
					// saturated pool can use in full, plus process exit checks.
					c.Shutdown = 8 * time.Second
					c.RequireLeakCheck = true
					c.Artifacts = filepath.Join(t.ArtifactDir(), "run")
					t.Logf("seed=%d driver=%s n=%d processes=%d concurrency=%d prefetch=%d pool=%d job-timeout=%s claim=%s app-tz=%s db-tz=%s artifacts=%s",
						seed, driver, c.N, c.Processes, c.Concurrency, c.Prefetch, c.Pool, c.JobTimeout, c.claimLength(), c.AppTimeZone, c.DBTimeZone, c.Artifacts)
					ctx, cancel := context.WithTimeout(t.Context(), c.Chaos+c.Recovery+c.Verification+2*c.Shutdown+5*time.Second)
					defer cancel()
					// The worker is this same go test binary, selected by -test.run.
					// Separate processes let SIGKILL/SIGSTOP hit actual workers.
					summary, err := Run(ctx, c, helperCommand)
					if err != nil {
						t.Fatalf("simulation failed: %v; summary=%+v; artifacts=%s", err, summary, c.Artifacts)
					}
					if !summary.Verified || !summary.Passed || !summary.RuntimeChecks.Passed {
						t.Fatalf("verification/runtime checks failed: %+v", summary)
					}
					if !profile.chaos && summary.Coverage["start"] != c.Processes {
						t.Fatal("healthy run unnecessarily replaced consumers")
					}
					if profile.chaos && (summary.ChaosAttempts == 0 || summary.Coverage["kill_at_write"] == 0) {
						t.Fatalf("missing handler/write-boundary fault coverage: %+v", summary.Coverage)
					}
					if summary.Coverage["read_first"] == 0 {
						t.Fatal("no read-first handler attempt")
					}
					if driver == "sqlite" && summary.Coverage["side_audit"] == 0 {
						t.Fatal("no side audit committed")
					}
					if profile.chaos && driver == "postgres" && summary.Coverage["network"] == 0 {
						t.Fatal("no PostgreSQL network fault requested")
					}
					t.Logf("verified %d tasks in %s; recovery=%s coverage=%v", summary.Tasks, summary.Elapsed, summary.RecoveryElapsed, summary.Coverage)
				})
			}
		})
		if !passed {
			return
		}
	}
}
