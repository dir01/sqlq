package sqlq

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestAttemptJobTransactionFailure(t *testing.T) {
	t.Parallel()

	for _, scenario := range []struct {
		name       string
		setup      string
		handlerSQL string
		wantError  string
	}{
		{
			name:       "completion update fails",
			setup:      "CREATE TRIGGER reject_completion BEFORE UPDATE OF processed_at ON jobs BEGIN SELECT RAISE(ABORT, 'completion rejected'); END",
			handlerSQL: "INSERT INTO effects VALUES (1)",
			wantError:  "failed to mark job",
		},
		{
			name:       "completion updates no row",
			setup:      "SELECT 1",
			handlerSQL: "INSERT INTO effects VALUES (1); DELETE FROM jobs",
			wantError:  "no longer claimed",
		},
		{
			name:       "commit fails",
			setup:      "CREATE TABLE invalid_effects (id INTEGER REFERENCES effects(id) DEFERRABLE INITIALLY DEFERRED)",
			handlerSQL: "INSERT INTO effects VALUES (1); INSERT INTO invalid_effects VALUES (2)",
			wantError:  "failed to commit transaction",
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			db, err := sql.Open("sqlite3", "file:"+filepath.Join(t.TempDir(), "queue.db")+"?_foreign_keys=on")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			db.SetMaxOpenConns(1)

			d := newSQLiteDriver(db)
			require.NoError(t, d.initSchema(ctx))
			_, err = db.ExecContext(ctx, "CREATE TABLE effects (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, scenario.setup)
			require.NoError(t, err)
			require.NoError(t, d.insertJob(ctx, nil, "transaction", []byte("payload"), 0, nil))
			jobs, err := d.getJobsForConsumer(ctx, "transaction", 1, defaultClaimTimeout)
			require.NoError(t, err)
			require.Len(t, jobs, 1)

			tracer := noop.NewTracerProvider().Tracer("test")
			cons := &consumer{ //nolint:exhaustruct // Only dependencies used by attemptJob are needed.
				driver: d,
				tracer: tracer,
				handler: func(ctx context.Context, tx *sql.Tx, _ []byte) error {
					_, execErr := tx.ExecContext(ctx, scenario.handlerSQL)
					return execErr
				},
			}
			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			ctx, span := tracer.Start(ctx, "attempt")
			defer span.End()

			err = cons.attemptJob(ctx, span, &jobs[0], cons.jobInfo(&jobs[0]), tx)
			require.ErrorContains(t, err, scenario.wantError, "transaction failures must reach the retry/DLQ path")
			require.ErrorIs(t, tx.Commit(), sql.ErrTxDone)

			var effects, pending int
			require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM effects").Scan(&effects))
			require.Zero(t, effects, "handler writes must not survive a failed attempt")
			require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs WHERE processed_at IS NULL").Scan(&pending))
			require.Equal(t, 1, pending, "completion must not survive a failed attempt")
		})
	}
}
