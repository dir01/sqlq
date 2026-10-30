package sqlq

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestDefaultPollDelay(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		emptyPolls uint16
		want       time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 110 * time.Millisecond},
		{2, 121 * time.Millisecond},
		{60, 30 * time.Second}, // 100ms × 1.1^60 is approximately 30.4s
		{math.MaxUint16, 30 * time.Second},
	} {
		delays := make(map[time.Duration]struct{})
		for range 100 {
			delay := defaultPollDelay(tc.emptyPolls)
			requireJitteredDelay(t, tc.want, delay, "empty polls %d", tc.emptyPolls) // the delay plus a jitter of up to 20%
			delays[delay] = struct{}{}
		}
		// Idle consumers must not poll at the same time.
		require.Greater(t, len(delays), 1, "empty polls %d", tc.emptyPolls)
	}
}

// emptyPollsDriver returns a job only at the calls in withJobs, and an error at the calls in failures.
// At lastCall, it blocks until the test stops the loop.
type emptyPollsDriver struct {
	driver
	withJobs map[int]bool
	failures map[int]bool
	done     chan struct{}
	calls    int
	lastCall int
}

func (d *emptyPollsDriver) getJobsForConsumer(ctx context.Context, _ string, _ uint16, _ time.Duration) ([]job, error) {
	d.calls++
	if d.calls == d.lastCall { // Block until the test stops the loop.
		close(d.done)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if d.failures[d.calls] {
		return nil, errors.New("claim failure")
	}
	if d.withJobs[d.calls] {
		return []job{{ID: int64(d.calls), ClaimToken: "token"}}, nil //nolint:exhaustruct
	}
	return nil, nil
}

// The loop gives the poll delay function the number of polls since the last poll that found jobs.
// A failed fetch does not change that number.
func TestFetchLoopCountsEmptyPolls(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	metrics, err := newMetrics(metricnoop.NewMeterProvider().Meter("poll-test"))
	require.NoError(t, err)
	d := &emptyPollsDriver{ //nolint:exhaustruct
		withJobs: map[int]bool{3: true}, failures: map[int]bool{5: true},
		done: make(chan struct{}), lastCall: 7,
	}
	emptyPolls := make(chan uint16, 100)
	cons := &consumer{ //nolint:exhaustruct // The test uses only the polling loop.
		ctx: ctx, driver: d, tracer: noop.NewTracerProvider().Tracer("poll-test"), metrics: metrics,
		pollDelayFunc: func(n uint16) time.Duration {
			emptyPolls <- n
			return time.Millisecond
		},
		prefetchCount: 1, concurrency: 1, jobsChan: make(chan job, 10),
		claims: make(map[string]job),
	}
	done := make(chan struct{})
	go func() { defer close(done); cons.fetchJobsLoop() }()
	select {
	case <-d.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not poll")
	}
	cancel()
	<-done
	close(emptyPolls)

	var got []uint16
	for n := range emptyPolls {
		got = append(got, n)
	}
	// Calls: 1 empty, 2 empty, 3 jobs, 4 empty, 5 failure (the retry delay uses the count), 6 empty.
	// After the test stops the loop, the loop can call the poll delay function again, so examine only 6 values.
	require.GreaterOrEqual(t, len(got), 6)
	require.Equal(t, []uint16{1, 2, 0, 1, 1, 2}, got[:6])
}

// A consumer without free capacity holds jobs, so its next poll must not come later.
func TestFetchJobsWithoutCapacityCountsAsFoundJobs(t *testing.T) {
	t.Parallel()
	cons := &consumer{ //nolint:exhaustruct // The test uses only the capacity check.
		prefetchCount: 1, concurrency: 1,
		claims: map[string]job{"1/a": {}, "2/b": {}}, //nolint:exhaustruct
	}
	foundJobs, err := cons.fetchJobs(t.Context())
	require.NoError(t, err)
	require.True(t, foundJobs)
}
