package chaos

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// Reproduce the endurance failure without waiting for external /tmp cleanup:
// run a copy of this binary, unlink it, and use the real worker command builder.
func TestWorkerCommandSurvivesUnlink(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux procfs executable fallback")
	}
	if os.Getenv("SQLQ_CHAOS_UNLINK_HELPER") == "1" {
		if filepath.Base(os.Args[0]) != "unlink-helper.test" {
			t.Fatal("refusing to unlink anything except the disposable helper copy")
		}
		if err := os.Remove(os.Args[0]); err != nil {
			t.Fatal(err)
		}
		cmd := helperCommand("", 0, true)
		// Exit 7 is an existing worker-protocol fixture, before database setup.
		cmd.Env = append(cmd.Env, "SQLQ_CHAOS_START_FAILURE=1")
		out, err := cmd.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 7 {
			t.Fatalf("worker failed to execute after unlink: %v\n%s", err, out)
		}
		return
	}
	source, err := os.Open("/proc/self/exe")
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	path := filepath.Join(t.TempDir(), "unlink-helper.test")
	destination, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(destination, source)
	if err := errors.Join(copyErr, destination.Close()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, "-test.run=^TestWorkerCommandSurvivesUnlink$", "-test.timeout=20s")
	cmd.Env = append(os.Environ(), "SQLQ_CHAOS_UNLINK_HELPER=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("unlink regression: %v\n%s", err, out)
	}
}
