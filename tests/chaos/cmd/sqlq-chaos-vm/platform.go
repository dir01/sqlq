package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

type imageMetadata struct {
	Architecture string `json:"architecture"`
	CheckProfile string `json:"check_profile"`
}

type execution struct {
	HostOS            string `json:"host_os"`
	HostArchitecture  string `json:"host_architecture"`
	GuestArchitecture string `json:"guest_architecture"`
	Accelerator       string `json:"accelerator"`
	Clock             string `json:"clock"`
	CheckProfile      string `json:"check_profile,omitempty"`
	VCPUs             int    `json:"vcpus"`
}

func normalizeArchitecture(value string) (string, error) {
	switch value {
	case "amd64", "x86_64":
		return "amd64", nil
	case "arm64", "aarch64":
		return "arm64", nil
	default:
		return "", fmt.Errorf("unsupported guest architecture %q; use arm64 or amd64", value)
	}
}

func imageArchitecture(directory, requested string) (imageMetadata, error) {
	var metadata imageMetadata
	b, err := os.ReadFile(filepath.Join(directory, "image.json"))
	if err == nil {
		if err := json.Unmarshal(b, &metadata); err != nil {
			return metadata, fmt.Errorf("invalid image.json: %w", err)
		}
		metadata.Architecture, err = normalizeArchitecture(metadata.Architecture)
		if err != nil {
			return metadata, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return metadata, err
	} else {
		// Images built before architecture metadata existed were x86 bzImages.
		// Recognize those on ARM hosts instead of silently choosing ARM QEMU.
		metadata.Architecture, err = kernelArchitecture(filepath.Join(directory, "kernel"))
		if err != nil && requested != "" && requested != "auto" {
			metadata.Architecture, err = normalizeArchitecture(requested)
		}
		if err != nil {
			return metadata, err
		}
	}
	if requested != "" && requested != "auto" {
		arch, err := normalizeArchitecture(requested)
		if err != nil {
			return metadata, err
		}
		if arch != metadata.Architecture {
			return metadata, fmt.Errorf("requested %s guest but image contains %s; rebuild for the requested architecture", arch, metadata.Architecture)
		}
	}
	return metadata, nil
}

func kernelArchitecture(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	header := make([]byte, 0x206)
	n, err := io.ReadFull(f, header)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return "", err
	}
	header = header[:n]
	if len(header) >= 0x206 && bytes.Equal(header[0x202:0x206], []byte("HdrS")) {
		return "amd64", nil
	}
	if len(header) >= 60 && bytes.Equal(header[56:60], []byte("ARM\x64")) {
		return "arm64", nil
	}
	if len(header) >= 2 && header[0] == 0x1f && header[1] == 0x8b {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return "", err
		}
		gz, err := gzip.NewReader(f)
		if err != nil {
			return "", err
		}
		defer gz.Close()
		header = make([]byte, 60)
		if _, err := io.ReadFull(gz, header); err == nil && bytes.Equal(header[56:60], []byte("ARM\x64")) {
			return "arm64", nil
		}
	}
	return "", errors.New("cannot identify guest architecture; rebuild with image.json metadata or specify -arch")
}

// resolveExecution takes host attributes explicitly so compatibility checks are
// testable on Linux even though HVF itself is only available on macOS.
func resolveExecution(o options, metadata imageMetadata, hostOS, hostArch string) (execution, string, []string, error) {
	arch := metadata.Architecture
	accel := o.accel
	if accel == "" {
		accel = "tcg"
	}
	clock := o.clock
	if clock == "" || clock == "auto" {
		if accel == "hvf" {
			clock = "realtime"
		} else {
			clock = "accelerated"
		}
	}
	if accel != "tcg" && accel != "hvf" {
		return execution{}, "", nil, fmt.Errorf("unsupported accelerator %q; use tcg or hvf", accel)
	}
	if clock != "accelerated" && clock != "realtime" {
		return execution{}, "", nil, fmt.Errorf("unsupported clock mode %q", clock)
	}
	if accel == "hvf" && (hostOS != "darwin" || arch != hostArch) {
		return execution{}, "", nil, errors.New("HVF requires macOS and a guest matching the host CPU architecture")
	}
	if accel == "hvf" && clock == "accelerated" {
		return execution{}, "", nil, errors.New("HVF does not support icount; use -clock realtime or -accel tcg")
	}
	qemu := o.qemu
	if qemu == "" {
		if arch == "arm64" {
			qemu = "qemu-system-aarch64"
		} else {
			qemu = "qemu-system-x86_64"
		}
	}
	var args []string
	if arch == "arm64" {
		// Avoid high MMIO regions outside older Apple Silicon's IPA range.
		// This minimal guest has no PCI devices and fits below 4 GiB.
		if o.memory > 3072 {
			return execution{}, "", nil, errors.New("ARM virt/highmem=off supports at most 3072 MiB in this runner")
		}
		args = []string{"-machine", "virt,gic-version=3,highmem=off", "-cpu", "cortex-a72"}
		if accel == "hvf" {
			args[3] = "host"
		}
	} else if arch == "amd64" {
		args = []string{"-machine", "q35"}
	} else {
		return execution{}, "", nil, fmt.Errorf("unsupported image architecture %q", arch)
	}
	if accel == "tcg" {
		args = append(args, "-accel", "tcg,thread=single")
	} else {
		args = append(args, "-accel", "hvf")
	}
	args = append(args, "-smp", "1") // Hold CPU count constant for comparisons.
	if clock == "accelerated" {
		args = append(args, "-icount", "shift="+strconv.Itoa(o.shift)+",align=off,sleep=off")
	}
	return execution{HostOS: hostOS, HostArchitecture: hostArch, GuestArchitecture: arch, Accelerator: accel, Clock: clock, CheckProfile: metadata.CheckProfile, VCPUs: 1}, qemu, args, nil
}
