package sqlq_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/require"
)

func newPublishingSQLiteTestCase(t *testing.T) *TestCase {
	t.Helper()

	// WAL permits checking visibility on a second connection while tx is open.
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(t.TempDir(), "queue.db")+"?_journal_mode=WAL&_busy_timeout=1000")
	require.NoError(t, err)
	q, err := sqlq.New(db, sqlq.DBTypeSQLite)
	require.NoError(t, err)
	q.Run()
	t.Cleanup(func() {
		q.Shutdown()
		require.NoError(t, db.Close())
	})
	return &TestCase{Q: q, DB: db}
}

func TestPublishTxSQLite(t *testing.T) { //nolint:tparallel // Transaction subtests share one SQLite writer and run sequentially.
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	newPublishingSQLiteTestCase(t).TestPublishTx(ctx, t)
}

func TestPublishSQLiteSingleConnection(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tc := newPublishingSQLiteTestCase(t)
	tc.DB.SetMaxOpenConns(1)

	require.NoError(t, tc.Q.Publish(ctx, "direct", "hello"))
	tx, err := tc.DB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, tc.Q.PublishTx(ctx, tx, "transactional", "hello"))
	require.NoError(t, tx.Commit())

	var count int
	require.NoError(t, tc.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs").Scan(&count))
	require.Equal(t, 2, count)
}

func TestPublishSQLiteInsertError(t *testing.T) {
	t.Parallel()

	tc := newPublishingSQLiteTestCase(t)
	_, err := tc.DB.ExecContext(t.Context(), "DROP TABLE jobs")
	require.NoError(t, err)
	require.Error(t, tc.Q.Publish(t.Context(), "missing_table", "hello"))

	tx, err := tc.DB.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.Error(t, tc.Q.PublishTx(t.Context(), tx, "missing_table", "hello"))
}
