package sqlq

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

// delayedDeadlineContext models the interval after a deadline passes but before
// the runtime executes its cancellation callback (e.g. after process resumption).
// Err and Done come from the live parent, independent of the exposed deadline.
type delayedDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c delayedDeadlineContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

func TestCallbackDeadlineTransaction(t *testing.T) {
	t.Parallel()
	for _, callback := range []string{"handler", "dead-letter hook"} {
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
					// Cancellation has not been delivered: SQL writes succeed.
					require.NoError(t, ctx.Err())
					_, execErr := tx.ExecContext(ctx, "INSERT INTO effects VALUES (1)")
					if execErr != nil {
						return execErr
					}
					return d.insertJob(ctx, tx, "poison", []byte("1"), 0, nil)
				}
				tracer := noop.NewTracerProvider().Tracer("deadline-test")
				cons := &consumer{ //nolint:exhaustruct // Only callback/transaction dependencies are needed.
					ctx: base, driver: d, tracer: tracer,
					handler: func(ctx context.Context, tx *sql.Tx, _ []byte) error {
						return writeEffects(ctx, tx)
					},
					onDeadLetter: func(ctx context.Context, tx *sql.Tx, _ JobInfo, _ []byte, _ error) error {
						return writeEffects(ctx, tx)
					},
				}
				// No local timeout is necessary: the callback inherits ctx's
				// deadline. This exposes timer-delivery lag without sleeps/signals.
				if callback == "handler" {
					tx, beginErr := db.BeginTx(base, nil)
					require.NoError(t, beginErr)
					defer func() { _ = tx.Rollback() }()
					attemptCtx, span := tracer.Start(ctx, "attempt")
					err = cons.attemptJob(attemptCtx, span, &jobs[0], cons.jobInfo(&jobs[0]), tx)
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

				counts := make(map[string]int)
				for name, query := range map[string]string{
					"effects":   "SELECT COUNT(*) FROM effects",
					"poison":    "SELECT COUNT(*) FROM jobs WHERE job_type='poison'",
					"pending":   "SELECT COUNT(*) FROM jobs WHERE job_type='deadline' AND processed_at IS NULL",
					"processed": "SELECT COUNT(*) FROM jobs WHERE job_type='deadline' AND processed_at IS NOT NULL",
					"dlq":       "SELECT COUNT(*) FROM dead_letter_queue",
				} {
					var count int
					require.NoError(t, db.QueryRowContext(base, query).Scan(&count))
					counts[name] = count
				}
				if expired {
					require.Equal(t, map[string]int{"effects": 0, "poison": 0, "pending": 1, "processed": 0, "dlq": 0}, counts)
				} else if callback == "handler" {
					require.Equal(t, map[string]int{"effects": 1, "poison": 1, "pending": 0, "processed": 1, "dlq": 0}, counts)
				} else {
					require.Equal(t, map[string]int{"effects": 1, "poison": 1, "pending": 0, "processed": 0, "dlq": 1}, counts)
				}
			})
		}
	}
}
