package sqlq_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLiteHandlerAttachedWriteKeptWithMark checks that a handler's write to a database other than
// main, attached or temp, is committed with the processed mark. The handler reads the main database,
// which pins its snapshot, and then writes the other one. Once another connection commits to the main
// database, the mark fails with SQLITE_BUSY_SNAPSHOT although the transaction did write, so it must
// not be dropped in favor of a mark in a fresh transaction.
func TestSQLiteHandlerAttachedWriteKeptWithMark(t *testing.T) {
	t.Parallel()
	for name, setup := range map[string][]string{
		"attached": {`ATTACH DATABASE 'file:side.db' AS side`, `CREATE TABLE side.effects (n INTEGER)`},
		"temp":     {`CREATE TEMP TABLE effects (n INTEGER)`},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testHandlerOtherDatabaseWrite(t, setup)
		})
	}
}

func testHandlerOtherDatabaseWrite(t *testing.T, setup []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	dsn := "file:" + filepath.Join(dir, "main.db") + "?_journal_mode=WAL&_busy_timeout=1000"
	db, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	// One connection, so the setup below applies to every transaction.
	db.SetMaxOpenConns(1)
	for _, statement := range setup {
		statement = strings.ReplaceAll(statement, "file:side.db", "file:"+filepath.Join(dir, "side.db"))
		_, err = db.ExecContext(ctx, statement)
		require.NoError(t, err)
	}
	// Another process sharing the main database file.
	other, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)

	q, err := sqlq.New(
		db, sqlq.DBTypeSQLite,
		sqlq.WithDefaultBackoffFunc(func(_ uint16) time.Duration { return 0 }),
		sqlq.WithDefaultPollInterval(25*time.Millisecond),
	)
	require.NoError(t, err)
	require.NoError(t, q.Run(t.Context()))
	t.Cleanup(func() {
		assert.NoError(t, q.Shutdown(context.Background()))
		assert.NoError(t, db.Close())
		assert.NoError(t, other.Close())
	})
	_, err = other.ExecContext(ctx, "CREATE TABLE other_writes (n INTEGER)")
	require.NoError(t, err)

	const jobType = "attached_write"
	var calls atomic.Int32
	err = q.Consume(ctx, jobType, func(ctx context.Context, tx *sql.Tx, _ []byte) error {
		call := calls.Add(1)
		var n int
		if readErr := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs").Scan(&n); readErr != nil {
			return readErr
		}
		if _, writeErr := tx.ExecContext(ctx, "INSERT INTO effects VALUES (1)"); writeErr != nil {
			return writeErr
		}
		if call == 1 {
			// Another connection commits to the main database before the mark.
			if _, writeErr := other.ExecContext(ctx, "INSERT INTO other_writes VALUES (1)"); writeErr != nil {
				return writeErr
			}
		}
		return nil
	}, sqlq.WithConsumerMaxRetries(3))
	require.NoError(t, err)

	require.NoError(t, q.Publish(ctx, jobType, TestPayload{Message: "writes"}))

	require.Eventually(t, func() bool {
		var processedAt sql.NullInt64
		scanErr := db.QueryRowContext(ctx, "SELECT processed_at FROM jobs WHERE job_type = ?", jobType).Scan(&processedAt)
		return scanErr == nil && processedAt.Valid
	}, 5*time.Second, 20*time.Millisecond, "job was not marked processed")

	var effects int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM effects").Scan(&effects))
	require.Equal(t, 1, effects, "processed after %d handler calls; the handler's committed writes must be exactly one", calls.Load())
}
