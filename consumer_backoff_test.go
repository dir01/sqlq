package sqlq

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

type retryBackoffDriver struct {
	driver
	reschedule func(context.Context, int64, string, string, time.Duration) error
}

func (d *retryBackoffDriver) markJobFailedAndReschedule(ctx context.Context, id int64, token, message string, delay time.Duration) error {
	return d.reschedule(ctx, id, token, message, delay)
}

func TestRetryJobBackoffPanic(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		retryCount uint32
		retryNum   uint16
		minDelay   time.Duration
		maxDelay   time.Duration
	}{
		// The default backoff adds a jitter of up to 20% to each delay.
		{"first retry", 0, 1, 2 * time.Second, 2400 * time.Millisecond},
		{"jitter", 4, 5, 32 * time.Second, 38400 * time.Millisecond},
		{"saturated callback", 65535, 65535, 24 * time.Hour, 24 * time.Hour},
		{"maximum count", ^uint32(0), 65535, 24 * time.Hour, 24 * time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var logs bytes.Buffer
			calls := 0
			d := &retryBackoffDriver{ //nolint:exhaustruct // Only rescheduling is exercised.
				reschedule: func(_ context.Context, id int64, token, message string, delay time.Duration) error {
					calls++
					require.Equal(t, int64(42), id)
					require.Equal(t, "claim-token", token)
					require.Equal(t, "handler failed", message)
					require.GreaterOrEqual(t, delay, tc.minDelay)
					require.LessOrEqual(t, delay, tc.maxDelay)
					return nil
				},
			}
			cons := &consumer{ //nolint:exhaustruct // Only retry dependencies are needed.
				driver: d, tracer: noop.NewTracerProvider().Tracer("backoff-test"),
				logger: slog.New(slog.NewTextHandler(&logs, nil)),
			}
			callbackCalls := 0
			WithConsumerBackoffFunc(func(retry uint16) time.Duration {
				callbackCalls++
				require.Equal(t, tc.retryNum, retry)
				panic("broken backoff")
			})(cons)
			j := &job{ID: 42, ClaimToken: "claim-token", RetryCount: tc.retryCount} //nolint:exhaustruct
			require.NotPanics(t, func() {
				require.NoError(t, cons.retryJob(t.Context(), j, "handler failed"))
			})
			require.Equal(t, 1, callbackCalls)
			require.Equal(t, 1, calls, "panic must still schedule a retry")
			require.Contains(t, logs.String(), "broken backoff")
		})
	}
}

func TestRetryJobCustomBackoff(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("reschedule failed")
	called := false
	d := &retryBackoffDriver{ //nolint:exhaustruct // Only rescheduling is exercised.
		reschedule: func(_ context.Context, _ int64, _, _ string, delay time.Duration) error {
			called = true
			require.Equal(t, 123*time.Millisecond, delay)
			return wantErr
		},
	}
	cons := &consumer{ //nolint:exhaustruct // Only retry dependencies are needed.
		driver: d, tracer: noop.NewTracerProvider().Tracer("backoff-test"),
		backoffFunc: func(retry uint16) time.Duration {
			require.Equal(t, uint16(3), retry)
			return 123 * time.Millisecond
		},
	}
	require.ErrorIs(t, cons.retryJob(t.Context(), &job{RetryCount: 2}, "handler failed"), wantErr) //nolint:exhaustruct
	require.True(t, called)
}
