package main

import (
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExecutionModes(t *testing.T) {
	for _, tc := range []struct {
		name, arch, hostOS, hostArch, accel, clock, wantClock, wantQEMU string
		wantFlags                                                       []string
		wantError                                                       bool
	}{
		{"arm-accelerated", "arm64", "darwin", "arm64", "tcg", "auto", "accelerated", "qemu-system-aarch64", []string{"virt,gic-version=3,highmem=off", "cortex-a72", "-icount"}, false},
		{"arm-hvf", "arm64", "darwin", "arm64", "hvf", "auto", "realtime", "qemu-system-aarch64", []string{"-accel hvf", "-cpu host"}, false},
		{"arm-tcg-realtime", "arm64", "darwin", "arm64", "tcg", "realtime", "realtime", "qemu-system-aarch64", []string{"tcg,thread=single"}, false},
		{"legacy-x86-on-arm", "amd64", "darwin", "arm64", "tcg", "auto", "accelerated", "qemu-system-x86_64", []string{"-machine q35", "-icount"}, false},
		{"hvf-icount-incompatible", "arm64", "darwin", "arm64", "hvf", "accelerated", "", "", nil, true},
		{"hvf-wrong-architecture", "amd64", "darwin", "arm64", "hvf", "realtime", "", "", nil, true},
		{"hvf-linux", "arm64", "linux", "arm64", "hvf", "auto", "", "", nil, true},
		{"invalid-clock", "arm64", "darwin", "arm64", "tcg", "fast", "", "", nil, true},
		{"invalid-accelerator", "arm64", "darwin", "arm64", "unknown", "auto", "", "", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := options{accel: tc.accel, clock: tc.clock, shift: 3, memory: 2048}
			execution, qemu, args, err := resolveExecution(o, imageMetadata{Architecture: tc.arch, CheckProfile: "standard"}, tc.hostOS, tc.hostArch)
			if tc.wantError {
				if err == nil {
					t.Fatal("invalid mode accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if execution.Clock != tc.wantClock || qemu != tc.wantQEMU {
				t.Fatalf("wrong execution: %+v %s", execution, qemu)
			}
			joined := strings.Join(args, " ")
			for _, flag := range tc.wantFlags {
				if !strings.Contains(joined, flag) {
					t.Fatalf("missing %q in %s", flag, joined)
				}
			}
			if tc.wantClock == "realtime" && strings.Contains(joined, "-icount") {
				t.Fatal("realtime mode enabled icount")
			}
			if !strings.Contains(joined, "-smp 1") {
				t.Fatal("comparison changed CPU count")
			}
		})
	}
}

func TestImageArchitecture(t *testing.T) {
	t.Run("metadata", func(t *testing.T) {
		dir := t.TempDir()
		if err := saveJSON(filepath.Join(dir, "image.json"), imageMetadata{Architecture: "arm64", CheckProfile: "race"}); err != nil {
			t.Fatal(err)
		}
		metadata, err := imageArchitecture(dir, "auto")
		if err != nil || metadata.Architecture != "arm64" || metadata.CheckProfile != "race" {
			t.Fatalf("metadata not honored: %+v %v", metadata, err)
		}
		if _, err := imageArchitecture(dir, "amd64"); err == nil {
			t.Fatal("mismatched architecture accepted")
		}
	})
	for _, kind := range []string{"legacy-x86", "arm-image", "arm-gzip"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			header := make([]byte, 0x206)
			want := "arm64"
			if kind == "legacy-x86" {
				copy(header[0x202:], "HdrS")
				want = "amd64"
			} else {
				copy(header[56:], "ARM\x64")
			}
			if kind == "arm-gzip" {
				var compressed bytes.Buffer
				writer := gzip.NewWriter(&compressed)
				if _, err := writer.Write(header); err != nil {
					t.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}
				header = compressed.Bytes()
			}
			if err := os.WriteFile(filepath.Join(dir, "kernel"), header, 0600); err != nil {
				t.Fatal(err)
			}
			metadata, err := imageArchitecture(dir, "auto")
			if err != nil || metadata.Architecture != want {
				t.Fatalf("kernel not recognized: %+v %v", metadata, err)
			}
		})
	}
	t.Run("unknown-not-guessed", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "kernel"), []byte("unknown"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := imageArchitecture(dir, "auto"); err == nil {
			t.Fatal("unknown image architecture guessed")
		}
		if metadata, err := imageArchitecture(dir, "arm64"); err != nil || metadata.Architecture != "arm64" {
			t.Fatalf("explicit architecture not honored: %+v %v", metadata, err)
		}
	})
}

func TestARMLayoutRejectsExcessiveMemory(t *testing.T) {
	_, _, _, err := resolveExecution(options{memory: 4096}, imageMetadata{Architecture: "arm64"}, "darwin", "arm64")
	if err == nil {
		t.Fatal("ARM highmem=off accepted RAM extending beyond its layout")
	}
}
