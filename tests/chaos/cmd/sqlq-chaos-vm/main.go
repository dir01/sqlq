// sqlq-chaos-vm launches a network-isolated QEMU guest and collects serial artifacts.
package main

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type options struct {
	image, output, qemu                               string
	arch, accel, clock                                string
	seed                                              int64
	n, shift, memory                                  int
	chaos, recovery, verification, probe, hostTimeout time.Duration
	jobTimeout, claimTimeout, lockTimeout             time.Duration
}

type measurement struct {
	Driver       string  `json:"driver"`
	HostSeconds  float64 `json:"host_seconds"`
	GuestSeconds float64 `json:"guest_seconds"`
	Acceleration float64 `json:"acceleration"`
}

type report struct {
	Execution       execution             `json:"execution"`
	Workloads       []workloadMeasurement `json:"workloads,omitempty"`
	HostSeconds     float64               `json:"host_seconds"`
	Result          *int                  `json:"result"`
	ArchiveComplete bool                  `json:"archive_complete"`
	Clocks          []measurement         `json:"clock_measurements"`
	Error           string                `json:"error,omitempty"`
	Note            string                `json:"note"`
}

type workloadMeasurement struct {
	Profile            string  `json:"profile"`
	Driver             string  `json:"driver,omitempty"`
	Seed               int64   `json:"seed"`
	N                  int     `json:"n"`
	Tasks              int     `json:"tasks"`
	HostSeconds        float64 `json:"host_seconds"`
	GuestSeconds       float64 `json:"guest_seconds"`
	TasksPerHostSecond float64 `json:"tasks_per_host_second"`
	Passed             bool    `json:"passed"`
	Result             int     `json:"result"`
	SummaryPresent     bool    `json:"summary_present"`
}

func main() {
	f := flag.NewFlagSet("sqlq-chaos-vm", flag.ExitOnError)
	var o options
	f.StringVar(&o.image, "image", "chaos-vm-image", "image directory containing kernel and initramfs.gz")
	f.StringVar(&o.output, "output", "chaos-vm-results", "new results directory")
	f.StringVar(&o.qemu, "qemu", "", "QEMU executable override; selected automatically from image architecture")
	f.StringVar(&o.arch, "arch", "auto", "guest architecture: auto (image metadata), arm64 or amd64")
	f.StringVar(&o.accel, "accel", "tcg", "tcg for software emulation; hvf for native macOS virtualization")
	f.StringVar(&o.clock, "clock", "auto", "auto, accelerated (TCG only), or realtime")
	f.Int64Var(&o.seed, "seed", 1, "guest workload seed")
	f.IntVar(&o.n, "n", 5000, "factorial input, up to 50000")
	f.IntVar(&o.shift, "shift", 3, "icount ns/instruction exponent, NOT a speed multiplier")
	f.IntVar(&o.memory, "memory", 2048, "guest RAM in MiB")
	f.DurationVar(&o.chaos, "chaos", 30*time.Second, "guest chaos duration per profile")
	f.DurationVar(&o.recovery, "recovery", 5*time.Minute, "guest recovery deadline per profile")
	f.DurationVar(&o.verification, "verification", 5*time.Minute, "separate guest correctness-check deadline per profile")
	f.DurationVar(&o.probe, "probe", 10*time.Second, "guest clock probe duration")
	f.DurationVar(&o.hostTimeout, "host-timeout", 10*time.Minute, "independent host watchdog")
	f.DurationVar(&o.jobTimeout, "job-timeout", 2*time.Second, "guest handler timeout")
	f.DurationVar(&o.claimTimeout, "claim-timeout", 10*time.Second, "guest claim timeout, greater than twice job timeout")
	f.DurationVar(&o.lockTimeout, "lock-timeout", 100*time.Millisecond, "guest SQLite busy / PostgreSQL lock timeout")
	_ = f.Parse(os.Args[1:])
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := launch(ctx, o, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func launch(ctx context.Context, o options, out io.Writer) (resultErr error) {
	if o.shift < 0 || o.shift > 10 || o.n < 100 || o.n > 50000 || o.memory < 512 || o.hostTimeout <= 0 || o.probe <= 0 || o.chaos < 0 || o.recovery <= 0 || o.verification <= 0 {
		return errors.New("require shift 0..10, n 100..50000, memory >= 512, positive timeouts/probe/recovery and nonnegative chaos")
	}
	if o.jobTimeout <= 0 || o.claimTimeout <= 2*o.jobTimeout || o.lockTimeout < time.Millisecond {
		return errors.New("require positive job-timeout, claim-timeout > 2*job-timeout and lock-timeout >= 1ms")
	}
	for _, name := range []string{"kernel", "initramfs.gz"} {
		info, err := os.Stat(filepath.Join(o.image, name))
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("image file is not regular: %s", name)
		}
	}
	metadata, err := imageArchitecture(o.image, o.arch)
	if err != nil {
		return err
	}
	engine, qemu, args, err := resolveExecution(o, metadata, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		return err
	}
	if err := os.Mkdir(o.output, 0700); err != nil {
		return err
	}
	console := "ttyS0"
	if metadata.Architecture == "arm64" {
		console = "ttyAMA0"
	}
	args = append(args, "-m", strconv.Itoa(o.memory), "-display", "none", "-serial", "stdio", "-monitor", "none",
		"-nic", "none", "-no-reboot", "-rtc", "base=2026-01-01T00:00:00,clock=vm",
		"-kernel", filepath.Join(o.image, "kernel"), "-initrd", filepath.Join(o.image, "initramfs.gz"),
		"-append", fmt.Sprintf("console=%s rdinit=/init panic=-1 chaos.seed=%d chaos.n=%d chaos.duration=%s chaos.recovery=%s chaos.verification=%s chaos.probe=%s chaos.job_timeout=%s chaos.claim_timeout=%s chaos.lock_timeout=%s",
			console, o.seed, o.n, o.chaos, o.recovery, o.verification, o.probe, o.jobTimeout, o.claimTimeout, o.lockTimeout))
	if err := saveJSON(filepath.Join(o.output, "launch.json"), append([]string{qemu}, args...)); err != nil {
		return err
	}
	if err := saveJSON(filepath.Join(o.output, "execution.json"), engine); err != nil {
		return err
	}
	started := time.Now()
	r := report{Execution: engine, Note: "clock acceleration does not measure useful job throughput"}
	defer func() {
		r.HostSeconds = time.Since(started).Seconds()
		if resultErr != nil {
			r.Error = resultErr.Error()
		}
		resultErr = errors.Join(resultErr, saveJSON(filepath.Join(o.output, "host-summary.json"), r))
		_ = json.NewEncoder(out).Encode(r)
	}()
	ctx, cancel := context.WithTimeout(ctx, o.hostTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, qemu, args...)
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = cmd.Stdout
	log, err := os.OpenFile(filepath.Join(o.output, "console.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer log.Close()
	archivePath := filepath.Join(o.output, "artifacts.tar.gz")
	archive, err := os.OpenFile(archivePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer archive.Close()
	if err := cmd.Start(); err != nil {
		return err
	}
	// Always reap QEMU, including malformed serial output and canceled runs.
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	scanner := bufio.NewScanner(pipe)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	exporting := false
	probes := make(map[string]time.Time)
	var active *workloadMeasurement
	var workloadStarted time.Time
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if _, err := fmt.Fprintln(log, scanner.Text()); err != nil {
			return err
		}
		switch {
		case line == "SQLQ_ARTIFACTS_BEGIN":
			if exporting || r.ArchiveComplete {
				return errors.New("duplicate artifacts stream")
			}
			exporting = true
		case line == "SQLQ_ARTIFACTS_END":
			if !exporting {
				return errors.New("artifacts end without start")
			}
			exporting = false
			r.ArchiveComplete = true
		case exporting:
			if line == "" {
				continue
			}
			decoded, err := base64.StdEncoding.Strict().DecodeString(line)
			if err != nil {
				return fmt.Errorf("invalid artifacts encoding: %w", err)
			}
			if _, err := archive.Write(decoded); err != nil {
				return err
			}
		case strings.HasPrefix(line, "SQLQ_VM_RESULT="):
			status, err := strconv.Atoi(strings.TrimPrefix(line, "SQLQ_VM_RESULT="))
			if err != nil {
				return err
			}
			r.Result = &status
			// The marker is authoritative; do not wait for a stuck guest poweroff.
			goto completed
		case strings.HasPrefix(line, "{"):
			var event struct {
				Kind      string `json:"kind"`
				Profile   string `json:"profile"`
				Result    int    `json:"result"`
				Seed      int64  `json:"seed"`
				N         int    `json:"n"`
				Tasks     int    `json:"tasks"`
				Elapsed   int64  `json:"elapsed_ns"`
				Passed    *bool  `json:"passed"`
				Probe     string `json:"clock_probe"`
				Driver    string `json:"driver"`
				Monotonic int64  `json:"monotonic_ns"`
			}
			if json.Unmarshal([]byte(line), &event) != nil {
				continue
			}
			_, _ = fmt.Fprintln(out, line)
			if event.Kind == "profile_start" {
				if active != nil {
					return errors.New("profile start before previous profile ended")
				}
				active = &workloadMeasurement{Profile: event.Profile}
				workloadStarted = time.Now()
			} else if event.Passed != nil && active != nil {
				active.Driver = event.Driver
				active.Seed = event.Seed
				active.N = event.N
				active.Tasks = event.Tasks
				active.GuestSeconds = float64(event.Elapsed) / 1e9
				active.Passed = *event.Passed
				active.SummaryPresent = true
			} else if event.Kind == "profile_end" {
				if active == nil || active.Profile != event.Profile {
					return errors.New("profile end without matching start")
				}
				active.HostSeconds = time.Since(workloadStarted).Seconds()
				active.Result = event.Result
				active.Passed = active.Passed && active.Result == 0
				if active.Passed {
					active.TasksPerHostSecond = float64(active.Tasks) / active.HostSeconds
				}
				r.Workloads = append(r.Workloads, *active)
				active = nil
			}
			if event.Probe == "start" {
				probes[event.Driver] = time.Now()
			}
			if event.Probe == "end" {
				start, ok := probes[event.Driver]
				if !ok {
					return errors.New("clock probe end without start")
				}
				host := time.Since(start).Seconds()
				guest := float64(event.Monotonic) / 1e9
				m := measurement{Driver: event.Driver, HostSeconds: host, GuestSeconds: guest, Acceleration: guest / host}
				r.Clocks = append(r.Clocks, m)
				_ = json.NewEncoder(out).Encode(struct {
					Kind string `json:"kind"`
					measurement
				}{Kind: "clock_measurement", measurement: m})
			}
		}
	}
completed:
	if ctx.Err() != nil {
		return fmt.Errorf("host watchdog or cancellation: %w", ctx.Err())
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if err := archive.Close(); err != nil {
		return err
	}
	if r.ArchiveComplete {
		if err := extract(archivePath, o.output); err != nil {
			return err
		}
	}
	if active != nil {
		return errors.New("VM ended before the current workload profile finished")
	}
	if r.Result == nil || *r.Result != 0 || !r.ArchiveComplete {
		return errors.New("VM failed or omitted complete artifacts; inspect console.log")
	}
	for _, workload := range r.Workloads {
		if !workload.Passed {
			return fmt.Errorf("profile %s did not report a successful checked result", workload.Profile)
		}
	}
	return nil
}

func saveJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0600)
}

func extract(archivePath, directory string) error {
	f, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	reader := tar.NewReader(gz)
	for {
		h, err := reader.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		// Only the guest's artifacts subtree is accepted. Links and device nodes
		// are rejected, and extraction never trusts archive permissions.
		name := filepath.Clean(h.Name)
		if !filepath.IsLocal(name) || name != "artifacts" && !strings.HasPrefix(name, "artifacts"+string(filepath.Separator)) {
			return fmt.Errorf("invalid archive path: %s", h.Name)
		}
		path := filepath.Join(directory, name)
		switch h.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(path, 0700); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				return err
			}
			output, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(output, reader)
			closeErr := output.Close()
			if err := errors.Join(copyErr, closeErr); err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid archive member type: %s", h.Name)
		}
	}
}
