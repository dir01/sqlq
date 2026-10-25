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

func TestFetchRetryBackoff(t *testing.T) {
	t.Parallel()
	require.Equal(t, 5*time.Millisecond, fetchRetryBackoff(0, 5*time.Millisecond))
	require.Equal(t, 10*time.Millisecond, fetchRetryBackoff(1, 5*time.Millisecond))
	require.Equal(t, 100*time.Millisecond, fetchRetryBackoff(0, time.Second))
	// After the first retry, the delay starts from the poll interval.
	require.Equal(t, 2*time.Second, fetchRetryBackoff(1, time.Second))
	for _, poll := range []time.Duration{time.Millisecond, 5 * time.Millisecond, time.Second} {
		for _, attempt := range []uint16{16, 32, 65535} {
			require.Equal(t, 30*time.Second, fetchRetryBackoff(attempt, poll), "poll interval %s, attempt %d", poll, attempt)
		}
	}
}

// A persistent fetch failure must not make the consumer query the database
// more frequently than its poll interval.
func TestFetchRetryBackoffDoesNotExceedPollRate(t *testing.T) {
	t.Parallel()
	for _, poll := range []time.Duration{time.Minute, 5 * time.Minute} {
		require.Equal(t, fetchRetryMaxFirstDelay, fetchRetryBackoff(0, poll))
		for _, attempt := range []uint16{1, 2, 16, 65535} {
			require.Equal(t, poll, fetchRetryBackoff(attempt, poll), "poll interval %s, attempt %d", poll, attempt)
		}
	}
}

// Doubling a very long poll interval must not overflow time.Duration.
func TestFetchRetryBackoffLongPollIntervalDoesNotOverflow(t *testing.T) {
	t.Parallel()
	poll := time.Duration(math.MaxInt64)
	for _, attempt := range []uint16{1, 16, 65535} {
		require.Equal(t, poll, fetchRetryBackoff(attempt, poll))
	}
}

type transientFetchDriver struct {
	driver
	recovered chan struct{}
	calls     int
}

func (d *transientFetchDriver) getJobsForConsumer(context.Context, string, uint16, time.Duration) ([]job, error) {
	d.calls++
	if d.calls == 1 {
		return nil, errors.New("transient claim failure")
	}
	if d.calls == 2 {
		close(d.recovered)
	}
	return nil, nil
}

func TestFetchLoopRecoversFromTransientFailure(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	metrics, err := newMetrics(metricnoop.NewMeterProvider().Meter("fetch-test"))
	require.NoError(t, err)
	d := &transientFetchDriver{recovered: make(chan struct{})} //nolint:exhaustruct
	cons := &consumer{                                         //nolint:exhaustruct // The test uses only the polling loop.
		ctx: ctx, driver: d, tracer: noop.NewTracerProvider().Tracer("fetch-test"), metrics: metrics,
		pollInterval: 5 * time.Millisecond, prefetchCount: 1, concurrency: 1,
		claims: make(map[string]job),
	}
	done := make(chan struct{})
	go func() { defer close(done); cons.fetchJobsLoop() }()
	defer func() { cancel(); <-done }()
	select {
	case <-d.recovered:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("transient fetch failure incurred an excessive delay")
	}
}
