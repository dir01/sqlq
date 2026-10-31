package chaos

import (
	"reflect"
	"testing"
	"time"
)

func TestCommitWatch(t *testing.T) {
	t.Parallel()
	at := time.Unix(0, 0)
	ms := func(n int) time.Time { return at.Add(time.Duration(n) * time.Millisecond) }
	// Supervisor events arrive before the worker's buffered output, as they do in a run.
	events := []Event{
		{At: ms(10), Worker: 1, Kind: "kill", Task: -1},           // Task 0's commit: killed mid-way.
		{At: ms(9), Worker: 1, Kind: "side_audit", Task: 0},       //
		{At: ms(5), Worker: 2, Kind: "side_audit", Task: 16},      // Task 16's commit returned
		{At: ms(6), Worker: 2, Kind: "side_committed", Task: 16},  // before the kill.
		{At: ms(8), Worker: 2, Kind: "kill", Task: -1},            //
		{At: ms(1), Worker: 3, Kind: "side_audit", Task: 32},      // Task 32's worker was killed
		{At: ms(500), Worker: 3, Kind: "kill", Task: -1},          // long after it.
		{At: ms(2), Worker: 4, Kind: "side_audit", Task: 48},      // Task 48 ran again elsewhere.
		{At: ms(3), Worker: 4, Kind: "kill", Task: -1},            //
		{At: ms(4), Worker: 5, Kind: "side_audit", Task: 48},      //
		{At: ms(4), Worker: 6, Kind: "side_audit", Task: 64},      // Task 64's worker was stopped,
		{At: ms(5), Worker: 6, Kind: "shutdown", Task: -1},        // not killed.
		{At: ms(4), Worker: 7, Kind: "side_audit", Task: 80},      // Task 80's worker was killed
		{At: ms(5), Worker: 7, Kind: "pause", Task: -1},           // after a pause, its last event
		{At: ms(20), Worker: 7, Kind: "kill", Task: -1},           // still the side audit.
		{At: ms(1), Worker: -1, Kind: "phase_start", Task: -1},    //
		{At: ms(30), Worker: 8, Kind: "kill_at_write", Task: 9},   // Killed at a write of a different
		{At: ms(29), Worker: 8, Kind: "side_audit", Task: 96},     // goroutine, after a side audit.
		{At: ms(30), Worker: 8, Kind: "written", Task: 9},         //
		{At: ms(40), Worker: 9, Kind: "side_audit", Task: 112},    // Task 112's worker was killed
		{At: ms(41), Worker: 9, Kind: "attempt", Task: 5},         // mid-way. An event of a different
		{At: ms(41), Worker: 9, Kind: "worker_metrics", Task: -1}, // goroutine does not show that
		{At: ms(42), Worker: 9, Kind: "kill", Task: -1},           // the commit returned.
		{At: ms(50), Worker: 10, Kind: "side_audit", Task: 128},   // Task 128\'s worker was killed
		{At: ms(51), Worker: 10, Kind: "kill_at_stop", Task: -1},  // when the chaos phase ended.
	}
	w := newCommitWatch()
	for _, e := range events {
		w.observe(e)
	}
	want := map[int]bool{0: true, 80: true, 96: true, 112: true, 128: true}
	if got := w.interrupted(100 * time.Millisecond); !reflect.DeepEqual(got, want) {
		t.Fatalf("interrupted = %v; want %v", got, want)
	}
}
