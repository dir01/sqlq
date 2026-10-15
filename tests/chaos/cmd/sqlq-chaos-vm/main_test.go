package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if mode := os.Getenv("SQLQ_VM_FAKE"); mode != "" {
		if mode == "hang" {
			time.Sleep(time.Hour)
			os.Exit(0)
		}
		fmt.Println(`{"clock_probe":"start","driver":"sqlite"}`)
		time.Sleep(10 * time.Millisecond)
		fmt.Println(`{"clock_probe":"end","driver":"sqlite","monotonic_ns":1000000000}`)
		fmt.Println(`{"kind":"profile_start","profile":"sqlite-finite"}`)
		if mode == "profile-fail" {
			fmt.Println(`{"driver":"sqlite","seed":42,"n":100,"tasks":9,"elapsed_ns":1000000000,"passed":false}`)
		} else {
			fmt.Println(`{"driver":"sqlite","seed":42,"n":100,"tasks":9,"elapsed_ns":1000000000,"passed":true}`)
		}
		fmt.Println(`{"kind":"profile_end","profile":"sqlite-finite","result":0}`)
		fmt.Println("SQLQ_ARTIFACTS_BEGIN")
		b, _ := os.ReadFile(os.Getenv("SQLQ_VM_FAKE_ARCHIVE"))
		fmt.Println(base64.StdEncoding.EncodeToString(b))
		fmt.Println("SQLQ_ARTIFACTS_END")
		fmt.Println("SQLQ_VM_RESULT=0")
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func archiveFixture(t *testing.T, name string, kind byte) string {
	t.Helper()
	var buffer bytes.Buffer
	gz := gzip.NewWriter(&buffer)
	writer := tar.NewWriter(gz)
	header := &tar.Header{Name: name, Mode: 0600, Size: 2, Typeflag: kind}
	if kind != tar.TypeReg {
		header.Size = 0
		header.Linkname = "../../outside"
	}
	if err := writer.WriteHeader(header); err != nil {
		t.Fatal(err)
	}
	if kind == tar.TypeReg {
		if _, err := io.WriteString(writer, "ok"); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "fixture.tar.gz")
	if err := os.WriteFile(path, buffer.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func fakeOptions(t *testing.T) options {
	t.Helper()
	dir := t.TempDir()
	image := filepath.Join(dir, "image")
	if err := os.Mkdir(image, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"kernel", "initramfs.gz"} {
		if err := os.WriteFile(filepath.Join(image, name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveJSON(filepath.Join(image, "image.json"), imageMetadata{Architecture: "arm64", CheckProfile: "standard"}); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return options{image: image, output: filepath.Join(dir, "results"), qemu: executable, n: 100, shift: 3, memory: 1024,
		probe: time.Second, recovery: time.Second, verification: time.Second, hostTimeout: 5 * time.Second,
		jobTimeout: 2 * time.Second, claimTimeout: 10 * time.Second, lockTimeout: 100 * time.Millisecond}
}

func TestLaunchCollectsArtifacts(t *testing.T) {
	t.Setenv("SQLQ_VM_FAKE", "success")
	t.Setenv("SQLQ_VM_FAKE_ARCHIVE", archiveFixture(t, "artifacts/summary.json", tar.TypeReg))
	o := fakeOptions(t)
	if err := launch(t.Context(), o, io.Discard); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(o.output, "artifacts", "summary.json"))
	if err != nil || string(b) != "ok" {
		t.Fatalf("archive not extracted: %q %v", b, err)
	}
	b, err = os.ReadFile(filepath.Join(o.output, "launch.json"))
	if err != nil {
		t.Fatal(err)
	}
	var args []string
	if err := json.Unmarshal(b, &args); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "-nic none") || !strings.Contains(joined, "shift=3,align=off,sleep=off") {
		t.Fatalf("missing isolation/virtual time: %s", joined)
	}
	if !strings.Contains(joined, "virt,gic-version=3") || !strings.Contains(joined, "console=ttyAMA0") {
		t.Fatalf("missing ARM platform/console: %s", joined)
	}
	b, err = os.ReadFile(filepath.Join(o.output, "host-summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report report
	if err := json.Unmarshal(b, &report); err != nil {
		t.Fatal(err)
	}
	if report.Execution.GuestArchitecture != "arm64" || len(report.Workloads) != 1 || !report.Workloads[0].Passed || report.Workloads[0].Tasks != 9 || report.Workloads[0].HostSeconds <= 0 {
		t.Fatalf("missing architecture/workload measurement: %+v", report)
	}
}

func TestHostWatchdog(t *testing.T) {
	t.Setenv("SQLQ_VM_FAKE", "hang")
	o := fakeOptions(t)
	o.hostTimeout = 50 * time.Millisecond
	start := time.Now()
	if err := launch(context.Background(), o, io.Discard); err == nil || !strings.Contains(err.Error(), "watchdog") {
		t.Fatalf("expected watchdog failure: %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("watchdog did not bound host time")
	}
}

func TestLaunchRejectsUncheckedSuccess(t *testing.T) {
	t.Setenv("SQLQ_VM_FAKE", "profile-fail")
	t.Setenv("SQLQ_VM_FAKE_ARCHIVE", archiveFixture(t, "artifacts/summary.json", tar.TypeReg))
	o := fakeOptions(t)
	if err := launch(t.Context(), o, io.Discard); err == nil || !strings.Contains(err.Error(), "successful checked result") {
		t.Fatalf("unchecked profile accepted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(o.output, "artifacts", "summary.json")); err != nil {
		t.Fatal("failed run did not retain artifacts")
	}
}

func TestLaunchRealTimeLegacyImage(t *testing.T) {
	t.Setenv("SQLQ_VM_FAKE", "success")
	t.Setenv("SQLQ_VM_FAKE_ARCHIVE", archiveFixture(t, "artifacts/summary.json", tar.TypeReg))
	o := fakeOptions(t)
	o.clock = "realtime"
	if err := os.Remove(filepath.Join(o.image, "image.json")); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, 0x206)
	copy(header[0x202:], "HdrS")
	if err := os.WriteFile(filepath.Join(o.image, "kernel"), header, 0600); err != nil {
		t.Fatal(err)
	}
	if err := launch(t.Context(), o, io.Discard); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(o.output, "launch.json"))
	if err != nil {
		t.Fatal(err)
	}
	var args []string
	if err := json.Unmarshal(b, &args); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "-machine q35") || !strings.Contains(joined, "console=ttyS0") || strings.Contains(joined, "-icount") {
		t.Fatalf("incorrect legacy realtime launch: %s", joined)
	}
}

func TestArchiveRejectsTraversalAndLinks(t *testing.T) {
	for _, fixture := range []struct {
		name string
		kind byte
	}{
		{"../outside", tar.TypeReg}, {"/absolute", tar.TypeReg}, {"artifacts/link", tar.TypeSymlink}, {"console.log", tar.TypeReg},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			archive := archiveFixture(t, fixture.name, fixture.kind)
			if err := extract(archive, t.TempDir()); err == nil {
				t.Fatalf("accepted unsafe member %s", fixture.name)
			}
		})
	}
}
