// Package chaos implements an opt-in real-database correctness and chaos harness.
package chaos

import (
	"fmt"
	"math/big"
	"math/rand"
	"runtime/pprof"
	"time"
	_ "time/tzdata" // Workers load AppTimeZone by name, whatever zone files the host has.
)

const jobType = "factorial"

// auditJobType jobs are published with each committed result and only read it back.
const auditJobType = "audit"

// keptJobType is one job that always fails, and must stay in the DLQ until the run ends.
const keptJobType = "kept"

// sideAuditJobType jobs, on SQLite only, read a result back and then record it in an attached database.
const sideAuditJobType = "side_audit"

// Config is persisted so a workload and its fault choices can be reproduced.
// Durations are guest-clock durations when running inside the VM.
type Config struct {
	Driver           string
	DSN              string
	Schema           string
	Seed             int64
	N                int
	MaxRange         int
	Processes        int
	Concurrency      uint16
	Prefetch         uint16
	Pool             int
	LockTimeout      time.Duration
	Poll             time.Duration
	JobTimeout       time.Duration
	ClaimTimeout     time.Duration
	Backoff          time.Duration
	MaxRetries       int32
	Push             bool
	PushRPM          int
	Cleanup          time.Duration
	Chaos            time.Duration
	Recovery         time.Duration
	Verification     time.Duration
	Shutdown         time.Duration
	RequireLeakCheck bool
	// SaturatePoolOnShutdown holds the whole pool while the worker shuts down.
	SaturatePoolOnShutdown bool
	// ShutdownOnSignal makes workers call Shutdown from a goroutine as soon as a signal comes,
	// as services often do, even while they are still registering consumers; Consume's context
	// never ends then. Without it, a worker shuts down after its main loop.
	ShutdownOnSignal bool
	FaultInterval    time.Duration
	Artifacts        string
	WorkerDSN        string
	// SideDB is the SQLite database that side audits write, attached to the main one.
	SideDB string
	// AppTimeZone is the workers' local time zone (their TZ), and DBTimeZone is the PostgreSQL
	// session's TimeZone. Empty means the environment's. The queue must not depend on either.
	AppTimeZone string
	DBTimeZone  string
	Tasks       []Task
}

// Task describes a node of the independently generated product tree.
type Task struct {
	ID     int
	Lo     int
	Hi     int
	Left   int
	Right  int
	Parent int
}

func DefaultConfig() Config {
	return Config{Driver: "sqlite", Seed: 1, N: 5000, MaxRange: 100,
		Processes: 3, Concurrency: 2, Prefetch: 8, Pool: 8, LockTimeout: 100 * time.Millisecond,
		Poll: 20 * time.Millisecond, JobTimeout: 200 * time.Millisecond,
		ClaimTimeout: time.Second, Backoff: 30 * time.Millisecond, MaxRetries: 2,
		Chaos: 3 * time.Second, Recovery: 30 * time.Second, Verification: 30 * time.Second, Shutdown: 10 * time.Second, FaultInterval: 100 * time.Millisecond,
		Artifacts: "chaos-artifacts"}
}

// RandomizedConfig selects a valid, seeded configuration while preserving
// database, workload, budgets and artifact settings. Explicit CLI flags override
// these choices. Keep delays long enough for actual database computation.
func RandomizedConfig(c Config) Config {
	r := rand.New(rand.NewSource(c.Seed ^ 0x517cc1b727220a95))
	c.Processes = 1 + r.Intn(4)
	c.Concurrency = uint16(1 + r.Intn(4))
	c.Prefetch = uint16(1 + r.Intn(16))
	c.Pool = 1 + r.Intn(8)
	c.Poll = time.Duration(5+r.Intn(96)) * time.Millisecond
	c.JobTimeout = time.Duration(200+r.Intn(301)) * time.Millisecond
	c.ClaimTimeout = RandomClaimTimeout(r, c.JobTimeout)
	c.Backoff = time.Duration(r.Intn(101)) * time.Millisecond
	c.MaxRetries = []int32{-1, 0, 1, 3}[r.Intn(4)]
	c.Push = c.Driver == "sqlite" && r.Intn(2) == 0
	c.PushRPM = []int{0, 60, 600}[r.Intn(3)]
	c.Cleanup = 0
	if r.Intn(2) == 0 {
		c.Cleanup = 2 * c.JobTimeout
	}
	c.LockTimeout = []time.Duration{10 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond}[r.Intn(3)]
	c.SaturatePoolOnShutdown = r.Intn(2) == 0
	// Zones west and east of UTC, with whole and fractional offsets.
	zones := []string{"UTC", "America/Los_Angeles", "Asia/Tokyo", "America/St_Johns", "Pacific/Chatham"}
	c.AppTimeZone = zones[r.Intn(len(zones))]
	c.DBTimeZone = zones[r.Intn(len(zones))]
	c.ShutdownOnSignal = r.Intn(2) == 0
	return c
}

func (c Config) Validate() error {
	if c.RequireLeakCheck && pprof.Lookup("goroutineleak") == nil {
		return fmt.Errorf("goroutineleak profile unavailable: use Go 1.27+ or Go 1.26 with GOEXPERIMENT=goroutineleakprofile")
	}
	if c.Driver != "sqlite" && c.Driver != "postgres" {
		return fmt.Errorf("unsupported driver %q", c.Driver)
	}
	if c.Driver == "postgres" && c.DSN == "" {
		return fmt.Errorf("postgres requires -dsn")
	}
	if c.Push && c.Driver != "sqlite" {
		return fmt.Errorf("push requires sqlite")
	}
	if c.N < 1 || c.N > 50000 || c.MaxRange < 1 || c.MaxRange > c.N {
		return fmt.Errorf("require 1 <= max-range <= n <= 50000")
	}
	if c.Processes < 1 || c.Concurrency < 1 || c.Pool < 1 || c.LockTimeout < time.Millisecond {
		return fmt.Errorf("processes, concurrency and pool must be positive; lock-timeout must be >= 1ms")
	}
	if c.Poll <= 0 || c.JobTimeout < 10*time.Millisecond || c.ClaimTimeout <= c.JobTimeout || c.Backoff < 0 {
		return fmt.Errorf("require positive poll, job-timeout >= 10ms, claim-timeout > job-timeout, nonnegative backoff")
	}
	if c.Chaos < 0 || c.Recovery <= 0 || c.Verification <= 0 || c.Shutdown <= 0 || c.FaultInterval <= 0 || c.Cleanup < 0 || c.MaxRetries < -1 || c.MaxRetries > 100 {
		return fmt.Errorf("invalid budgets, cleanup or retries")
	}
	if c.Artifacts == "" {
		return fmt.Errorf("artifacts directory is required")
	}
	return nil
}

func buildTasks(c Config) []Task {
	r := rand.New(rand.NewSource(c.Seed)) // Only the supervisor generates the graph.
	var tasks []Task
	var level []int
	for lo := 1; lo <= c.N; {
		hi := min(c.N, lo+r.Intn(c.MaxRange))
		id := len(tasks)
		tasks = append(tasks, Task{ID: id, Lo: lo, Hi: hi, Left: -1, Right: -1, Parent: -1})
		level = append(level, id)
		lo = hi + 1
	}
	for len(level) > 1 {
		var next []int
		for i := 0; i < len(level); i += 2 {
			if i+1 == len(level) {
				next = append(next, level[i])
				continue
			}
			a, b := level[i], level[i+1]
			id := len(tasks)
			tasks = append(tasks, Task{ID: id, Lo: tasks[a].Lo, Hi: tasks[b].Hi, Left: a, Right: b, Parent: -1})
			tasks[a].Parent, tasks[b].Parent = id, id
			next = append(next, id)
		}
		level = next
	}
	return tasks
}

func expected(t Task) string { return new(big.Int).MulRange(int64(t.Lo), int64(t.Hi)).String() }

// RandomClaimTimeout returns a claim timeout from just over jobTimeout to four times it.
// Short claims are as much a part of the queue's contract as long ones: see AcceptedClaimTimeout.
func RandomClaimTimeout(r *rand.Rand, jobTimeout time.Duration) time.Duration {
	return jobTimeout + time.Millisecond + time.Duration(r.Int63n(int64(3*jobTimeout)))
}
