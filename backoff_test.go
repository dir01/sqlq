package sqlq

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestExponentialBackoff(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		retryNum uint16
		minExp   time.Duration
		maxExp   time.Duration
	}{
		{name: "retry 0", retryNum: 0, minExp: time.Second, maxExp: 1200 * time.Millisecond},
		{name: "retry 1", retryNum: 1, minExp: 2 * time.Second, maxExp: 2400 * time.Millisecond},
		{name: "retry 5", retryNum: 5, minExp: 32 * time.Second, maxExp: 38400 * time.Millisecond},
		{name: "retry 10", retryNum: 10, minExp: 1024 * time.Second, maxExp: 1228800 * time.Millisecond},
		{name: "retry 32", retryNum: 32, minExp: (1 << 32) * time.Second, maxExp: (1 << 32) * time.Second / 5 * 6},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Run multiple times to account for randomness
			durations := make(map[time.Duration]bool)
			for range 100 {
				duration := exponentialBackoff(tc.retryNum)
				if duration < tc.minExp || duration >= tc.maxExp {
					t.Errorf("exponentialBackoff(%d) = %v; want between [%v, %v)",
						tc.retryNum, duration, tc.minExp, tc.maxExp)
				}
				durations[duration] = true
			}
			// Jobs that failed together must not all retry together, from the first retry on.
			if len(durations) < 2 {
				t.Errorf("exponentialBackoff(%d) returned only %v; want jitter", tc.retryNum, durations)
			}
		})
	}
}

func TestExponentialBackoffDoesNotOverflow(t *testing.T) {
	t.Parallel()

	previous := time.Duration(0)
	for retryNum := range uint16(math.MaxUint16) {
		got := exponentialBackoff(retryNum)
		if got < previous {
			t.Fatalf("exponentialBackoff(%d) = %v; want at least %v", retryNum, got, previous)
		}
		previous = got
	}
}

type rescheduleRecordingDriver struct {
	driver
	backoff time.Duration
}

func (d *rescheduleRecordingDriver) markJobFailedAndReschedule(_ context.Context, _ int64, _ string, _ string, backoff time.Duration) error {
	d.backoff = backoff
	return nil
}

func TestRetryJobCapsBackoff(t *testing.T) {
	t.Parallel()

	for name, backoffFunc := range map[string]func(uint16) time.Duration{
		"default":  exponentialBackoff,
		"custom":   func(uint16) time.Duration { return 1000 * time.Hour },
		"overflow": func(uint16) time.Duration { return math.MaxInt64 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			d := &rescheduleRecordingDriver{} //nolint:exhaustruct
			cons := &consumer{                //nolint:exhaustruct // Only dependencies used by retryJob are needed.
				driver: d, tracer: noop.NewTracerProvider().Tracer("backoff-test"), metrics: noopMetrics(),
				backoffFunc: backoffFunc, maxBackoff: time.Minute,
			}
			require.NoError(t, cons.retryJob(t.Context(), &job{RetryCount: 100}, "failed")) //nolint:exhaustruct
			require.Equal(t, time.Minute, d.backoff)
		})
	}
}
