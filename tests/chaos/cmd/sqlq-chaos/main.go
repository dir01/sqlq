// sqlq-chaos runs a seeded, real-database factorial correctness workload.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/dir01/sqlq/tests/chaos/internal/chaos"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) > 0 && args[0] == "worker" {
		f := flag.NewFlagSet("worker", flag.ContinueOnError)
		path := f.String("config", "", "configuration file")
		id := f.Int("id", 0, "worker ID")
		healthy := f.Bool("healthy", false, "disable deliberate handler faults")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		b, err := os.ReadFile(*path)
		if err != nil {
			return err
		}
		var c chaos.Config
		if err := json.Unmarshal(b, &c); err != nil {
			return err
		}
		if err := chaos.Worker(ctx, c, *id, *healthy, os.Stdout); err != nil {
			_ = json.NewEncoder(os.Stdout).Encode(chaos.Event{At: time.Now(), Worker: *id, Kind: "fatal", Task: -1, Detail: err.Error()})
			return err
		}
		return nil
	}
	c := chaos.DefaultConfig()
	f := flag.NewFlagSet("sqlq-chaos", flag.ContinueOnError)
	f.StringVar(&c.Driver, "driver", c.Driver, "sqlite or postgres")
	f.StringVar(&c.DSN, "dsn", "", "PostgreSQL DSN (fresh schema created and retained; sslmode=disable)")
	f.Int64Var(&c.Seed, "seed", c.Seed, "workload/fault seed (scheduling is not deterministic)")
	f.IntVar(&c.N, "n", c.N, "factorial input, up to 50000")
	f.IntVar(&c.MaxRange, "max-range", c.MaxRange, "maximum random leaf range size")
	f.IntVar(&c.Processes, "processes", c.Processes, "consumer subprocesses")
	workers := f.Uint("workers", uint(c.Concurrency), "workers per process")
	prefetch := f.Uint("prefetch", uint(c.Prefetch), "prefetch per consumer")
	f.IntVar(&c.Pool, "pool", c.Pool, "connections per process")
	f.DurationVar(&c.LockTimeout, "lock-timeout", c.LockTimeout, "SQLite busy timeout / PostgreSQL lock_timeout")
	f.DurationVar(&c.Poll, "poll", c.Poll, "poll interval")
	f.DurationVar(&c.JobTimeout, "job-timeout", c.JobTimeout, "handler timeout")
	f.DurationVar(&c.Backoff, "backoff", c.Backoff, "retry backoff unit")
	retries := f.Int("retries", int(c.MaxRetries), "maximum retries (-1 means unlimited)")
	f.BoolVar(&c.Push, "push", false, "enable SQLite async push hints")
	f.IntVar(&c.PushRPM, "push-rpm", 0, "push rate limit, 0 means unlimited")
	f.DurationVar(&c.Cleanup, "cleanup", 0, "processed cleanup interval; 0 disables")
	f.DurationVar(&c.Chaos, "chaos", c.Chaos, "fault injection budget; 0 for healthy run")
	f.DurationVar(&c.Recovery, "recovery", c.Recovery, "separate recovery budget")
	f.DurationVar(&c.Verification, "verification", c.Verification, "separate correctness verification budget")
	f.DurationVar(&c.Shutdown, "shutdown", c.Shutdown, "graceful shutdown/runtime-check deadline")
	f.BoolVar(&c.RequireLeakCheck, "require-leak-check", true, "fail if the runtime goroutineleak profile is unavailable")
	f.DurationVar(&c.FaultInterval, "fault-interval", c.FaultInterval, "process/network fault interval")
	f.StringVar(&c.AppTimeZone, "app-timezone", "", "workers' TZ; empty keeps the environment's")
	f.StringVar(&c.DBTimeZone, "db-timezone", "", "PostgreSQL session TimeZone; empty keeps the server's")
	f.BoolVar(&c.ShutdownOnSignal, "shutdown-on-signal", false, "workers call Shutdown from a signal goroutine, even mid-registration")
	f.StringVar(&c.Artifacts, "artifacts", c.Artifacts, "new directory for config, logs, database and summary")
	randomConfig := f.Bool("random-config", false, "seeded options matrix; explicit option flags override generated choices")
	probe := f.Duration("clock-probe", 0, "probe Go/database clocks for this duration instead of running chaos")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", f.Args())
	}
	if *randomConfig {
		generated := chaos.RandomizedConfig(c)
		explicit := make(map[string]bool)
		f.Visit(func(flag *flag.Flag) { explicit[flag.Name] = true })
		choices := map[string]string{
			"processes": strconv.Itoa(generated.Processes), "workers": strconv.Itoa(int(generated.Concurrency)),
			"prefetch": strconv.Itoa(int(generated.Prefetch)), "pool": strconv.Itoa(generated.Pool),
			"lock-timeout": generated.LockTimeout.String(),
			"poll":         generated.Poll.String(), "job-timeout": generated.JobTimeout.String(),
			"backoff": generated.Backoff.String(),
			"retries": strconv.Itoa(int(generated.MaxRetries)), "push": strconv.FormatBool(generated.Push),
			"push-rpm": strconv.Itoa(generated.PushRPM), "cleanup": generated.Cleanup.String(),
			"app-timezone": generated.AppTimeZone, "db-timezone": generated.DBTimeZone,
			"shutdown-on-signal": strconv.FormatBool(generated.ShutdownOnSignal),
		}
		for name, value := range choices {
			if !explicit[name] {
				if err := f.Set(name, value); err != nil {
					return err
				}
			}
		}
	}
	if *workers > 65535 || *prefetch > 65535 || *retries < -1 || *retries > 100 || c.PushRPM < 0 || c.PushRPM > 65535 {
		return fmt.Errorf("workers/prefetch/push-rpm must fit uint16; retries must be -1..100")
	}
	c.Concurrency, c.Prefetch, c.MaxRetries = uint16(*workers), uint16(*prefetch), int32(*retries)
	if *probe > 0 {
		return chaos.ClockProbe(ctx, c, *probe, os.Stdout)
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	summary, err := chaos.Run(ctx, c, func(path string, id int, healthy bool) *exec.Cmd {
		return exec.Command(executable, "worker", "-config", path, "-id", strconv.Itoa(id), "-healthy="+strconv.FormatBool(healthy))
	}, func(e chaos.Event) {
		if e.Kind == "phase_start" || e.Kind == "progress" {
			_ = json.NewEncoder(os.Stdout).Encode(e)
		}
	})
	encodeErr := json.NewEncoder(os.Stdout).Encode(summary)
	if err != nil {
		return err
	}
	return encodeErr
}
