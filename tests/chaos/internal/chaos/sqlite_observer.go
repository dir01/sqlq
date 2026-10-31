package chaos

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/mattn/go-sqlite3"
)

// observedSQLiteDriver is registered for worker processes. It reports every statement that fails
// with SQLITE_BUSY_SNAPSHOT: a deferred transaction read, another connection committed, and the
// transaction then tried to write. busy_timeout cannot wait that out, so no lock-timeout setting
// explains it. The queue owns its transactions and must never upgrade a stale read. Only
// statements of a handler's transaction may fail this way:
//   - the workload's own statements (chaos_* tables), when a read-first handler then writes;
//   - the queue's processed mark after a read-only handler, which it retries in a fresh transaction.
//
// Anything else is a queue bug, and plain SQLITE_BUSY (a lock wait that timed out) is not reported.
const observedSQLiteDriver = "sqlite3-observed"

// snapshotConflict is called with each unexpected stale-snapshot upgrade. Set once per worker process.
var snapshotConflict atomic.Pointer[func(query string, err error)]

// sideCommitted is called with the task of each side write, after the transaction that wrote it
// committed successfully. Set once per worker process. The handler cannot report this itself,
// because the queue commits after the handler returns (see commitWatch).
var sideCommitted atomic.Pointer[func(task int)]

// sideWrite is the statement of a side audit. The observer finds the side writes by this text.
const sideWrite = `INSERT INTO side.chaos_side_effects VALUES (?)`

func init() {
	sql.Register(observedSQLiteDriver, observedDriver{base: &sqlite3.SQLiteDriver{}})
}

func expectedSnapshotConflict(query string) bool {
	return strings.Contains(query, "chaos_") || strings.Contains(query, "UPDATE sqlq_jobs SET processed_at = ?")
}

func observeSnapshot(query string, err error) {
	var sqliteErr sqlite3.Error
	if !errors.As(err, &sqliteErr) || sqliteErr.ExtendedCode != sqlite3.ErrBusySnapshot || expectedSnapshotConflict(query) {
		return
	}
	if report := snapshotConflict.Load(); report != nil {
		(*report)(strings.Join(strings.Fields(query), " "), err)
	}
}

type observedDriver struct{ base *sqlite3.SQLiteDriver }

func (d observedDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.base.Open(name)
	if err != nil {
		return nil, err
	}
	sqliteConn, ok := conn.(*sqlite3.SQLiteConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("unexpected sqlite3 connection type %T", conn)
	}
	return &observedConn{SQLiteConn: sqliteConn}, nil
}

// observedConn keeps every other method of the underlying connection.
type observedConn struct {
	*sqlite3.SQLiteConn
	sideTasks []int // The tasks of the side writes in the open transaction.
}

func (c *observedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := c.SQLiteConn.ExecContext(ctx, query, args)
	observeSnapshot(query, err)
	if err == nil && query == sideWrite && len(args) == 1 {
		if task, ok := args[0].Value.(int64); ok {
			c.sideTasks = append(c.sideTasks, int(task))
		}
	}
	return result, err
}

func (c *observedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.sideTasks = nil
	tx, err := c.SQLiteConn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &observedTx{Tx: tx, conn: c}, nil
}

// observedTx reports the side writes of its transaction when the commit is successful.
type observedTx struct {
	driver.Tx
	conn *observedConn
}

func (tx *observedTx) Commit() error {
	err := tx.Tx.Commit()
	tasks := tx.conn.sideTasks
	tx.conn.sideTasks = nil
	if report := sideCommitted.Load(); err == nil && report != nil {
		for _, task := range tasks {
			(*report)(task)
		}
	}
	return err
}

func (tx *observedTx) Rollback() error {
	tx.conn.sideTasks = nil
	return tx.Tx.Rollback()
}

func (c *observedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.SQLiteConn.QueryContext(ctx, query, args)
	observeSnapshot(query, err)
	if err != nil {
		return nil, err
	}
	// A statement such as UPDATE ... RETURNING runs, and can fail, on its first Next.
	return &observedRows{Rows: rows, query: query}, nil
}

type observedRows struct {
	driver.Rows
	query string
}

func (r *observedRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	observeSnapshot(r.query, err)
	return err
}
