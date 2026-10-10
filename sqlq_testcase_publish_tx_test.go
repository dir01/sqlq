package sqlq_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/require"
)

// TestPublishTx verifies that application writes and jobs share the caller's
// transaction, including visibility before commit and removal on rollback.
func (tc *TestCase) TestPublishTx(ctx context.Context, t *testing.T) {
	t.Helper()

	_, err := tc.DB.ExecContext(ctx, `CREATE TABLE publish_tx_records (name TEXT PRIMARY KEY)`)
	require.NoError(t, err)

	for _, commit := range []bool{true, false} {
		name := "publish_tx_rollback"
		if commit {
			name = "publish_tx_commit"
		}

		t.Run(name, func(t *testing.T) { //nolint:paralleltest // Check one transaction at a time on SQLite.
			tx, err := tc.DB.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()

			_, err = tx.ExecContext(ctx, "INSERT INTO publish_tx_records (name) VALUES ($1)", name)
			require.NoError(t, err)
			require.NoError(t, tc.Q.PublishTx(ctx, tx, name, TestPayload{Message: name}, sqlq.WithDelay(time.Hour)))

			var payload []byte
			var delayed bool
			err = tx.QueryRowContext(ctx,
				"SELECT payload, scheduled_at > created_at FROM jobs WHERE job_type = $1", name,
			).Scan(&payload, &delayed)
			require.NoError(t, err, "the job must be visible in the caller's transaction")
			var decoded TestPayload
			require.NoError(t, json.Unmarshal(payload, &decoded))
			require.Equal(t, name, decoded.Message)
			require.True(t, delayed, "transactional publishing must preserve the delay")

			var count int
			err = tc.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs WHERE job_type = $1", name).Scan(&count)
			require.NoError(t, err)
			require.Zero(t, count, "uncommitted jobs must not be visible on another connection")

			if commit {
				require.NoError(t, tx.Commit())
			} else {
				require.NoError(t, tx.Rollback())
			}

			expected := 0
			if commit {
				expected = 1
			}
			err = tc.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs WHERE job_type = $1", name).Scan(&count)
			require.NoError(t, err)
			require.Equal(t, expected, count)
			err = tc.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM publish_tx_records WHERE name = $1", name).Scan(&count)
			require.NoError(t, err)
			require.Equal(t, expected, count)

			require.ErrorIs(t, tc.Q.PublishTx(ctx, tx, name, "already finished"), sql.ErrTxDone)
		})
	}
}
