package sqlq_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/require"
)

// TestSQLiteMigrationKeepsForeignKeyRows migrates an old jobs table, in which retry_count can be NULL.
// The migration makes the table again. Tables of the application refer to jobs, and foreign keys are on.
// The migration must not delete the rows of these tables through ON DELETE CASCADE, and must not fail
// because of a foreign key without an action. After the migration, foreign keys must be on again.
func TestSQLiteMigrationKeepsForeignKeyRows(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(t.TempDir(), "migration.db")+"?_foreign_keys=on")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1) // The pragma check at the end must use the connection of the migration.

	for _, statement := range []string{
		`CREATE TABLE jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_type TEXT NOT NULL,
			payload BLOB,
			created_at INTEGER NOT NULL,
			scheduled_at INTEGER NOT NULL,
			retry_count INTEGER,
			last_error TEXT,
			trace_context TEXT,
			consumed_at INTEGER NULL,
			processed_at INTEGER NULL
		)`,
		`INSERT INTO jobs (id, job_type, created_at, scheduled_at, retry_count) VALUES (1, 'old', 0, 0, NULL)`,
		`CREATE TABLE cascading (job_id INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE)`,
		`CREATE TABLE restricting (job_id INTEGER NOT NULL REFERENCES jobs(id))`,
		`INSERT INTO cascading (job_id) VALUES (1)`,
		`INSERT INTO restricting (job_id) VALUES (1)`,
	} {
		_, err = db.ExecContext(t.Context(), statement)
		require.NoError(t, err)
	}

	q, err := sqlq.New(db, sqlq.DBTypeSQLite)
	require.NoError(t, err)
	require.NoError(t, q.Run()) // Run makes the schema.
	q.Shutdown()

	var notNull bool
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT "notnull" FROM pragma_table_info('jobs') WHERE name = 'retry_count'`).Scan(&notNull))
	require.True(t, notNull, "the migration did not make the table again")
	for _, table := range []string{"cascading", "restricting"} {
		var count int
		require.NoError(t, db.QueryRowContext(t.Context(), `SELECT count(*) FROM `+table).Scan(&count))
		require.Equal(t, 1, count, "the migration deleted the rows of %s", table)
	}
	var foreignKeys bool
	require.NoError(t, db.QueryRowContext(t.Context(), `PRAGMA foreign_keys`).Scan(&foreignKeys))
	require.True(t, foreignKeys, "the migration did not set foreign keys on again")
}
