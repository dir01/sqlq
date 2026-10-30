package sqlq

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace/noop"
)

// delayedDeadlineContext simulates the time after a deadline, but before the runtime
// cancels the context (for example, after the process continues from a stop).
// Err and Done come from the parent, and do not change with the deadline.
type delayedDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c delayedDeadlineContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

// The callbacks that TestCallbackDeadlineTransaction runs.
const (
	handlerCallback    = "handler"
	deadLetterCallback = "dead-letter hook"
)

// deadlineCounts are the rows that TestCallbackDeadlineTransaction counts after a callback.
type deadlineCounts struct {
	effects, poison, pending, processed, dlq int
}

func TestCallbackDeadlineTransaction(t *testing.T) {
	t.Parallel()
	for _, callback := range []string{handlerCallback, deadLetterCallback} {
		for _, expired := range []bool{true, false} {
			name := callback + "/live"
			if expired {
				name = callback + "/expired-without-cancellation"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				base := t.Context()
				db, err := sql.Open("sqlite3", "file:"+filepath.Join(t.TempDir(), "queue.db"))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, db.Close()) })
				db.SetMaxOpenConns(1)
				d := newSQLiteDriver(db)
				require.NoError(t, d.initSchema(base))
				_, err = db.ExecContext(base, "CREATE TABLE effects (id INTEGER PRIMARY KEY)")
				require.NoError(t, err)
				require.NoError(t, d.insertJob(base, nil, "deadline", []byte("payload"), 0, nil))
				jobs, err := d.getJobsForConsumer(base, "deadline", 1, time.Minute)
				require.NoError(t, err)
				require.Len(t, jobs, 1)

				deadline := time.Now().Add(time.Hour)
				if expired {
					deadline = time.Now().Add(-time.Hour)
				}
				ctx := delayedDeadlineContext{Context: base, deadline: deadline}
				called := false
				writeEffects := func(ctx context.Context, tx *sql.Tx) error {
					called = true
					// The context is not canceled yet. Thus, SQL writes are successful.
					require.NoError(t, ctx.Err())
					_, execErr := tx.ExecContext(ctx, "INSERT INTO effects VALUES (1)")
					if execErr != nil {
						return execErr
					}
					return d.insertJob(ctx, tx, "poison", []byte("1"), 0, nil)
				}
				tracer := noop.NewTracerProvider().Tracer("deadline-test")
				metrics, metricsErr := newMetrics(metricnoop.NewMeterProvider().Meter("deadline-test"))
				require.NoError(t, metricsErr)
				cons := &consumer{ //nolint:exhaustruct // The test needs only the callback and transaction dependencies.
					ctx: base, driver: d, tracer: tracer, metrics: metrics,
					handler: func(ctx context.Context, tx *sql.Tx, _ []byte) error {
						return writeEffects(ctx, tx)
					},
					onDeadLetter: func(ctx context.Context, tx *sql.Tx, _ JobInfo, _ []byte, _ error) error {
						return writeEffects(ctx, tx)
					},
				}
				// A local timeout is not necessary. The callback gets the deadline of ctx.
				// Thus, the test shows a late timer without sleep or signals.
				if callback == handlerCallback {
					tx, beginErr := db.BeginTx(base, nil)
					require.NoError(t, beginErr)
					defer func() { _ = tx.Rollback() }()
					attemptCtx, span := tracer.Start(ctx, "attempt")
					_, err = cons.attemptJob(attemptCtx, span, &jobs[0], cons.jobInfo(&jobs[0]), tx)
					span.End()
				} else {
					err = cons.moveJobToDLQ(ctx, &jobs[0], cons.jobInfo(&jobs[0]), errors.New("handler failure"))
				}
				require.True(t, called)
				if expired {
					require.ErrorIs(t, err, context.DeadlineExceeded)
				} else {
					require.NoError(t, err)
				}

				count := func(query string) int {
					var n int
					require.NoError(t, db.QueryRowContext(base, query).Scan(&n))
					return n
				}
				counts := deadlineCounts{
					effects:   count("SELECT COUNT(*) FROM effects"),
					poison:    count("SELECT COUNT(*) FROM jobs WHERE job_type='poison'"),
					pending:   count("SELECT COUNT(*) FROM jobs WHERE job_type='deadline' AND processed_at IS NULL"),
					processed: count("SELECT COUNT(*) FROM jobs WHERE job_type='deadline' AND processed_at IS NOT NULL"),
					dlq:       count("SELECT COUNT(*) FROM dead_letter_queue"),
				}
				switch {
				case expired:
					require.Equal(t, deadlineCounts{effects: 0, poison: 0, pending: 1, processed: 0, dlq: 0}, counts)
				case callback == handlerCallback:
					require.Equal(t, deadlineCounts{effects: 1, poison: 1, pending: 0, processed: 1, dlq: 0}, counts)
				default:
					require.Equal(t, deadlineCounts{effects: 1, poison: 1, pending: 0, processed: 0, dlq: 1}, counts)
				}
			})
		}
	}
}
