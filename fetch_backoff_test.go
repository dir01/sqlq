package sqlq

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

// requireJitteredDelay makes sure that got is want plus a jitter of up to 20%.
func requireJitteredDelay(t *testing.T, want, got time.Duration, msgAndArgs ...any) {
	t.Helper()
	require.GreaterOrEqual(t, got, want, msgAndArgs...)
	require.LessOrEqual(t, got, want+want/5, msgAndArgs...)
}

func TestFetchRetryBackoff(t *testing.T) {
	t.Parallel()
	requireJitteredDelay(t, 5*time.Millisecond, fetchRetryBackoff(0, 5*time.Millisecond))
	requireJitteredDelay(t, 10*time.Millisecond, fetchRetryBackoff(1, 5*time.Millisecond))
	requireJitteredDelay(t, 100*time.Millisecond, fetchRetryBackoff(0, time.Second))
	// After the first retry, the delay starts from the poll interval.
	requireJitteredDelay(t, 2*time.Second, fetchRetryBackoff(1, time.Second))
	for _, poll := range []time.Duration{time.Millisecond, 5 * time.Millisecond, time.Second} {
		for _, attempt := range []uint16{16, 32, 65535} {
			requireJitteredDelay(t, 30*time.Second, fetchRetryBackoff(attempt, poll), "poll interval %s, attempt %d", poll, attempt)
		}
	}
}

// A persistent fetch failure must not make the consumer query the database
// more frequently than its poll interval.
func TestFetchRetryBackoffDoesNotExceedPollRate(t *testing.T) {
	t.Parallel()
	for _, poll := range []time.Duration{time.Minute, 5 * time.Minute} {
		requireJitteredDelay(t, 100*time.Millisecond, fetchRetryBackoff(0, poll))
		for _, attempt := range []uint16{1, 2, 16, 65535} {
			requireJitteredDelay(t, poll, fetchRetryBackoff(attempt, poll), "poll interval %s, attempt %d", poll, attempt)
		}
	}
}

// Doubling a very long poll interval and adding the jitter must not overflow time.Duration.
func TestFetchRetryBackoffLongPollIntervalDoesNotOverflow(t *testing.T) {
	t.Parallel()
	poll := time.Duration(math.MaxInt64)
	for _, attempt := range []uint16{1, 16, 65535} {
		require.Equal(t, poll, fetchRetryBackoff(attempt, poll))
	}
}

// Consumers that fail at the same time must not retry at the same time.
func TestFetchRetryBackoffHasJitter(t *testing.T) {
	t.Parallel()
	delays := make(map[time.Duration]struct{})
	for range 100 {
		delays[fetchRetryBackoff(5, time.Second)] = struct{}{}
	}
	require.Greater(t, len(delays), 1)
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
	d := &transientFetchDriver{recovered: make(chan struct{})} //nolint:exhaustruct
	cons := &consumer{                                         //nolint:exhaustruct // The test uses only the polling loop.
		ctx: ctx, driver: d, tracer: noop.NewTracerProvider().Tracer("fetch-test"),
		pollDelayFunc: func(uint16) time.Duration { return 5 * time.Millisecond }, prefetchCount: 1, concurrency: 1,
		claims: make(map[string]heldClaim), logger: slog.New(slog.DiscardHandler),
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

type pushDuringBackoffDriver struct {
	driver
	push    chan struct{}
	fetched chan struct{}
	calls   int
}

func (d *pushDuringBackoffDriver) subscribeForConsumer(context.Context, string, *tokenBucket) (<-chan struct{}, error) {
	return d.push, nil
}

func (d *pushDuringBackoffDriver) getJobsForConsumer(context.Context, string, uint16, time.Duration) ([]job, error) {
	d.calls++
	if d.calls <= 2 {
		return nil, errors.New("claim failure")
	}
	if d.calls == 3 { // A late push can cause more fetches. Close the channel only one time.
		close(d.fetched)
	}
	return nil, nil
}

// After two failed fetches, the consumer waits two poll intervals. An async push must stop the
// wait, because the driver discards a push that nobody reads.
func TestFetchLoopPushStopsBackoff(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	d := &pushDuringBackoffDriver{push: make(chan struct{}), fetched: make(chan struct{})} //nolint:exhaustruct
	cons := &consumer{                                                                     //nolint:exhaustruct // The test uses only the polling loop.
		ctx: ctx, driver: d, tracer: noop.NewTracerProvider().Tracer("fetch-test"), logger: slog.New(slog.DiscardHandler),
		pollDelayFunc: func(uint16) time.Duration { return time.Minute }, prefetchCount: 1, concurrency: 1, asyncPushEnabled: true,
		claims: make(map[string]heldClaim),
	}
	done := make(chan struct{})
	go func() { defer close(done); cons.fetchJobsLoop() }()
	defer func() { cancel(); <-done }()

	// The consumer reads a push in each wait, so send pushes until a fetch is successful.
	// Without the push, the third fetch would occur only after two poll intervals.
	timeout := time.After(5 * time.Second)
	for {
		select {
		case d.push <- struct{}{}:
		case <-d.fetched:
			return
		case <-timeout:
			t.Fatal("the push did not stop the backoff")
		}
	}
}
