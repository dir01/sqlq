package chaos

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestObserverReportsQueueSnapshotUpgrades checks that the worker driver reports a stale read
// snapshot upgrade in a queue statement, but neither in a handler statement nor for a plain
// lock wait that timed out.
func TestObserverReportsQueueSnapshotUpgrades(t *testing.T) {
	dsn := "file:" + filepath.Join(t.TempDir(), "observer.db") + "?_journal_mode=WAL&_busy_timeout=10"
	db, err := sql.Open(observedSQLiteDriver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	other, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	for _, statement := range []string{`CREATE TABLE jobs (id INTEGER)`, `CREATE TABLE chaos_mutex (id INTEGER)`} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	var reported []string
	report := func(query string, _ error) { reported = append(reported, query) }
	snapshotConflict.Store(&report)
	defer snapshotConflict.Store(nil)

	// staleUpgrade reads in a transaction, lets another connection commit, then writes.
	staleUpgrade := func(write string) {
		t.Helper()
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		var n int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM jobs`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if _, err := other.Exec(`INSERT INTO jobs VALUES (1)`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(write); err == nil {
			t.Fatalf("%s: stale snapshot upgrade succeeded", write)
		}
	}

	staleUpgrade(`INSERT INTO chaos_mutex VALUES (1)`)
	if len(reported) != 0 {
		t.Fatalf("handler statement reported: %v", reported)
	}
	staleUpgrade(`INSERT INTO jobs VALUES (2)`)
	if len(reported) != 1 {
		t.Fatalf("queue statement upgrade reported %d times, want 1", len(reported))
	}

	// A lock held by another connection makes a write wait and time out: plain SQLITE_BUSY.
	holder, err := other.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback() }()
	if _, err := holder.Exec(`INSERT INTO jobs VALUES (3)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO jobs VALUES (4)`); err == nil {
		t.Fatal("write succeeded while another connection held the lock")
	}
	if len(reported) != 1 {
		t.Fatalf("lock timeout reported as a snapshot upgrade: %v", reported)
	}
}
