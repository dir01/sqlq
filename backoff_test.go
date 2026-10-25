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
		{
			name:     "retry 0",
			retryNum: 0,
			minExp:   time.Second, // 2^0 = 1s
			// Adjust maxExp slightly for the zero case to make the range check work.
			// We expect exactly 1s, so the range should be [1s, 1s + epsilon).
			maxExp: time.Second + time.Nanosecond,
		},
		{
			name:     "retry 1",
			retryNum: 1,
			minExp:   2 * time.Second, // 2^1 = 2s
			maxExp:   3 * time.Second, // 2s + 1s jitter = 3s
		},
		{
			name:     "retry 5",
			retryNum: 5,
			minExp:   time.Duration(math.Pow(2, 5)) * time.Second,   // 32s
			maxExp:   time.Duration(math.Pow(2, 5)+5) * time.Second, // 32s + 5s jitter = 37s
		},
		{
			name:     "retry 10",
			retryNum: 10,
			minExp:   time.Duration(math.Pow(2, 10)) * time.Second,    // 1024s
			maxExp:   time.Duration(math.Pow(2, 10)+10) * time.Second, // 1024s + 10s jitter = 1034s
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Run multiple times to account for randomness
			for range 100 {
				duration := exponentialBackoff(tc.retryNum)
				if duration < tc.minExp || duration >= tc.maxExp {
					t.Errorf("exponentialBackoff(%d) = %v; want between [%v, %v)",
						tc.retryNum, duration, tc.minExp, tc.maxExp)
				}
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
