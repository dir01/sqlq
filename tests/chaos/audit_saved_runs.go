package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

type summary struct {
	Phase string `json:"phase"`
	Passed bool `json:"passed"`
	Verified bool `json:"verified"`
	Seed int `json:"seed"`
	Tasks int `json:"tasks"`
	Completed int `json:"completed_tasks"`
	Pending int `json:"pending_jobs"`
	DLQ int `json:"dlq_jobs"`
	Error string `json:"error"`
	Runtime struct {
		Passed bool `json:"passed"`
		Race bool `json:"race"`
		Leaks int `json:"leaks"`
		Database struct { OpenConnections, InUse int } `json:"database"`
	} `json:"runtime_checks"`
}

type event struct {
	At time.Time `json:"at"`
	Kind string `json:"kind"`
}

type result struct {
	Directory string `json:"directory"`
	Profiles int `json:"profile_directories"`
	Passed int `json:"validated_passes"`
	Tasks int `json:"verified_task_effects"`
	MinimumSeed int `json:"minimum_seed"`
	MaximumSeed int `json:"maximum_seed"`
	Issues []string `json:"issues"`
	Incomplete []string `json:"incomplete"`
	FirstEvent time.Time `json:"first_event"`
	LastIncompleteEvent *event `json:"last_incomplete_event,omitempty"`
	ObservedMinutes float64 `json:"observed_minutes,omitempty"`
}

func load(path string, target any) error {
	b, err := os.ReadFile(path)
	if err != nil { return err }
	return json.Unmarshal(b, target)
}

func validate(run string, cache map[[2]int]string) (summary, error) {
	var s summary
	if err := load(filepath.Join(run, "summary.json"), &s); err != nil { return s, err }
	if !s.Passed || !s.Verified || s.Phase != "complete" || s.Tasks != s.Completed || s.Pending != 0 || s.DLQ != 0 {
		return s, fmt.Errorf("summary failed/incomplete: %+v", s)
	}
	if !s.Runtime.Passed || !s.Runtime.Race || s.Runtime.Leaks != 0 || s.Runtime.Database.OpenConnections != 0 || s.Runtime.Database.InUse != 0 {
		return s, fmt.Errorf("runtime checks failed: %+v", s.Runtime)
	}
	var config struct { Tasks []struct { ID, Lo, Hi int } }
	if err := load(filepath.Join(run, "config.json"), &config); err != nil { return s, err }
	if len(config.Tasks) != s.Tasks { return s, fmt.Errorf("task count mismatch") }
	expected := make(map[int]string, len(config.Tasks))
	for _, task := range config.Tasks {
		key := [2]int{task.Lo, task.Hi}
		value, ok := cache[key]
		if !ok {
			value = new(big.Int).MulRange(int64(task.Lo), int64(task.Hi)).String()
			cache[key] = value
		}
		expected[task.ID] = value
	}
	var snapshot struct {
		Ledger []struct {
			Task int `json:"task"`
			Value string `json:"value"`
			Outcome string `json:"outcome"`
		} `json:"chaos_ledger"`
		Jobs []struct {
			Type string `json:"job_type"`
			Processed json.RawMessage `json:"processed_at"`
		} `json:"jobs"`
		DLQ []json.RawMessage `json:"dead_letter_queue"`
	}
	if err := load(filepath.Join(run, "database.json"), &snapshot); err != nil { return s, err }
	if len(expected) != s.Tasks || len(snapshot.Ledger) != s.Tasks { return s, fmt.Errorf("manifest/ledger count mismatch") }
	counts := make(map[int]int, len(expected))
	for _, row := range snapshot.Ledger {
		want, known := expected[row.Task]
		if !known || row.Outcome != "success" || row.Value != want {
			return s, fmt.Errorf("incorrect/unexpected effect for task %d", row.Task)
		}
		counts[row.Task]++
	}
	if len(counts) != len(expected) { return s, fmt.Errorf("missing committed tasks") }
	for id, count := range counts { if count != 1 { return s, fmt.Errorf("task %d committed %d times", id, count) } }
	if len(snapshot.DLQ) != 0 { return s, fmt.Errorf("residual DLQ jobs") }
	for _, j := range snapshot.Jobs {
		if j.Type != "factorial" || len(j.Processed) == 0 || string(j.Processed) == "null" {
			return s, fmt.Errorf("pending or poison publication")
		}
	}
	return s, nil
}

func main() {
	cache := make(map[[2]int]string)
	failed := false
	for _, name := range []string{"endurance-psql-10m", "endurance-psql-20m", "endurance-psql-50m", "endurance-psql-120m"} {
		r := result{Directory: name, Issues: []string{}, Incomplete: []string{}}
		runs, err := filepath.Glob(filepath.Join("tests", "chaos", name, "_artifacts", "internal", "chaos", "TestSimulationPSQL__*", "*", "run"))
		if err != nil { panic(err) }
		r.Profiles = len(runs)
		for _, run := range runs {
			_, statErr := os.Stat(filepath.Join(run, "summary.json"))
			incomplete := os.IsNotExist(statErr)
			stream, openErr := os.Open(filepath.Join(run, "events.jsonl"))
			if openErr == nil {
				scanner := bufio.NewScanner(stream)
				scanner.Buffer(make([]byte, 4096), 1024*1024)
				first := true
				for scanner.Scan() {
					var e event
					if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
						r.Issues = append(r.Issues, run+": invalid event: "+err.Error())
						break
					}
					if first && (r.FirstEvent.IsZero() || e.At.Before(r.FirstEvent)) { r.FirstEvent = e.At }
					first = false
					if !incomplete { break }
					r.LastIncompleteEvent = &e
				}
				if err := scanner.Err(); err != nil { r.Issues = append(r.Issues, run+": "+err.Error()) }
				_ = stream.Close()
			}
			if incomplete { r.Incomplete = append(r.Incomplete, run); continue }
			s, err := validate(run, cache)
			if err != nil { r.Issues = append(r.Issues, run+": "+err.Error()); continue }
			if r.Passed == 0 || s.Seed < r.MinimumSeed { r.MinimumSeed = s.Seed }
			if s.Seed > r.MaximumSeed { r.MaximumSeed = s.Seed }
			r.Passed++
			r.Tasks += s.Tasks
		}
		if r.LastIncompleteEvent != nil { r.ObservedMinutes = r.LastIncompleteEvent.At.Sub(r.FirstEvent).Minutes() }
		b, err := json.MarshalIndent(r, "", "  ")
		if err != nil { panic(err) }
		fmt.Println(string(b))
		failed = failed || len(r.Issues) > 0 || r.Profiles == 0
	}
	if failed { os.Exit(1) }
}
