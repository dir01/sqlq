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

// TestSQLiteHandlerAttachedWriteKeptWithMark makes sure that a handler write to an attached or temp
// database commits with the processed mark. The handler reads main, which sets its snapshot. Then
// it writes the other database. After a different connection commits to main, the mark fails with
// SQLITE_BUSY_SNAPSHOT, but the transaction has a write. Thus, the queue must not discard the
// transaction and mark the job in a new transaction.
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
	// One connection. Thus, the setup below applies to all transactions.
	db.SetMaxOpenConns(1)
	for _, statement := range setup {
		statement = strings.ReplaceAll(statement, "file:side.db", "file:"+filepath.Join(dir, "side.db"))
		_, err = db.ExecContext(ctx, statement)
		require.NoError(t, err)
	}
	// A different process that uses the same main database file.
	other, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)

	q, err := sqlq.New(
		db, sqlq.DBTypeSQLite,
		sqlq.WithDefaultBackoffFunc(func(_ uint16) time.Duration { return 0 }),
		sqlq.WithDefaultPollDelayFunc(pollEvery(25*time.Millisecond)),
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
		if readErr := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlq_jobs").Scan(&n); readErr != nil {
			return readErr
		}
		if _, writeErr := tx.ExecContext(ctx, "INSERT INTO effects VALUES (1)"); writeErr != nil {
			return writeErr
		}
		if call == 1 {
			// A different connection commits to main before the mark.
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
		scanErr := db.QueryRowContext(ctx, "SELECT processed_at FROM sqlq_jobs WHERE job_type = ?", jobType).Scan(&processedAt)
		return scanErr == nil && processedAt.Valid
	}, 5*time.Second, 20*time.Millisecond, "job was not marked processed")

	var effects int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM effects").Scan(&effects))
	require.Equal(t, 1, effects, "processed after %d handler calls; the handler's committed writes must be exactly one", calls.Load())
}
