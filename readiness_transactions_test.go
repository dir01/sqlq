package sqlq

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// boundaryConnector delegates all SQL to the real database driver. It only
// controls Begin/Commit boundaries, with no production failpoints or mock SQL.
type boundaryConnector struct {
	sqldriver.Connector
	beforeBegin func()
	commitFault atomic.Int32 // 1: disconnect before commit; 2: lose successful acknowledgment
	fired       atomic.Int32
}

func (c *boundaryConnector) Connect(ctx context.Context) (sqldriver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &boundaryConn{Conn: conn, control: c}, nil
}

type boundaryConn struct {
	sqldriver.Conn
	control *boundaryConnector
}

func (c *boundaryConn) BeginTx(ctx context.Context, opts sqldriver.TxOptions) (sqldriver.Tx, error) {
	if c.control.beforeBegin != nil {
		c.control.beforeBegin()
	}
	conn, ok := c.Conn.(sqldriver.ConnBeginTx)
	if !ok {
		return nil, errors.New("boundary test requires a context-aware database driver")
	}
	tx, err := conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &boundaryTx{Tx: tx, control: c.control}, nil
}

func (c *boundaryConn) ExecContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Result, error) {
	if conn, ok := c.Conn.(sqldriver.ExecerContext); ok {
		return conn.ExecContext(ctx, query, args)
	}
	return nil, sqldriver.ErrSkip
}

func (c *boundaryConn) QueryContext(ctx context.Context, query string, args []sqldriver.NamedValue) (sqldriver.Rows, error) {
	if conn, ok := c.Conn.(sqldriver.QueryerContext); ok {
		return conn.QueryContext(ctx, query, args)
	}
	return nil, sqldriver.ErrSkip
}

func (c *boundaryConn) ResetSession(ctx context.Context) error {
	if conn, ok := c.Conn.(sqldriver.SessionResetter); ok {
		return conn.ResetSession(ctx)
	}
	return nil
}

func (c *boundaryConn) IsValid() bool {
	if conn, ok := c.Conn.(sqldriver.Validator); ok {
		return conn.IsValid()
	}
	return true
}

type boundaryTx struct {
	sqldriver.Tx
	control *boundaryConnector
}

func (tx *boundaryTx) Commit() error {
	fault := tx.control.commitFault.Swap(0)
	if fault == 0 {
		return tx.Tx.Commit()
	}
	if fault == 1 {
		// A lost connection before COMMIT leaves the transaction rolled back.
		if err := tx.Rollback(); err != nil {
			return err
		}
	} else if err := tx.Tx.Commit(); err != nil {
		return err
	}
	tx.control.fired.Store(fault)
	// Do not return ErrBadConn, which permits database/sql transparent retries.
	return errors.New("injected ambiguous commit outcome")
}

func testCommitAmbiguity(t *testing.T, f readinessFixture, db *sql.DB, reset func(*testing.T)) {
	t.Helper()
	for _, fault := range []int32{1, 2} {
		for _, maxRetries := range []int32{0, 2} {
			t.Run(fmt.Sprintf("fault=%d/retries=%d", fault, maxRetries), func(t *testing.T) {
				reset(t)
				workerDB, d, arm, fired := readinessBoundaryDB(t, f)
				q, err := New(workerDB, f.kind)
				require.NoError(t, err)
				defer func() { require.NoError(t, q.Shutdown(t.Context())) }()
				require.NoError(t, q.Publish(t.Context(), "claims", "parent"))
				attempts := 0
				cons := testClaimConsumer(t.Context(), workerDB, d, func(ctx context.Context, tx *sql.Tx, _ []byte) error {
					attempts++
					if _, writeErr := tx.ExecContext(ctx, `INSERT INTO readiness_effects VALUES ('handler')`); writeErr != nil {
						return writeErr
					}
					if publishErr := q.PublishTx(ctx, tx, "child", "child"); publishErr != nil {
						return publishErr
					}
					if attempts == 1 {
						arm(fault)
					}
					return nil
				})
				defer cons.cancel()
				cons.maxRetries = maxRetries
				cons.onDeadLetter = func(ctx context.Context, tx *sql.Tx, _ JobInfo, _ []byte, _ error) error {
					_, hookErr := tx.ExecContext(ctx, `INSERT INTO readiness_effects VALUES ('hook')`)
					return hookErr
				}
				attempt := func() {
					jobs, fetchErr := d.getJobsForConsumer(t.Context(), "claims", 1, time.Minute)
					require.NoError(t, fetchErr)
					require.Len(t, jobs, 1)
					cons.processJob(&jobs[0])
				}
				attempt()
				require.Equal(t, fault, fired(), "the intended commit boundary must actually fire")
				if fault == 1 {
					require.Zero(t, countReadiness(t, db, `SELECT count(*) FROM readiness_effects WHERE value='handler'`))
					require.Zero(t, countReadiness(t, db, `SELECT count(*) FROM jobs WHERE job_type='child'`))
					if maxRetries == 0 {
						require.Equal(t, 1, countReadiness(t, db, `SELECT count(*) FROM dead_letter_queue`))
						require.Equal(t, 1, countReadiness(t, db, `SELECT count(*) FROM readiness_effects WHERE value='hook'`))
						require.Zero(t, countReadiness(t, db, `SELECT count(*) FROM jobs`))
						return
					}
					require.Equal(t, 1, countReadiness(t, db, `SELECT retry_count FROM jobs WHERE job_type='claims'`))
					attempt()
					require.Equal(t, 2, attempts)
				} else {
					require.Equal(t, 1, attempts)
				}
				require.Equal(t, 1, countReadiness(t, db, `SELECT count(*) FROM readiness_effects WHERE value='handler'`))
				require.Zero(t, countReadiness(t, db, `SELECT count(*) FROM readiness_effects WHERE value='hook'`))
				require.Equal(t, 1, countReadiness(t, db, `SELECT count(*) FROM jobs WHERE job_type='child'`))
				require.Equal(t, 1, countReadiness(t, db, `SELECT count(*) FROM jobs WHERE job_type='claims' AND processed_at IS NOT NULL`))
				require.Zero(t, countReadiness(t, db, `SELECT count(*) FROM dead_letter_queue`))
				jobs, err := d.getJobsForConsumer(t.Context(), "claims", 1, time.Minute)
				require.NoError(t, err)
				require.Empty(t, jobs, "lost acknowledgment must not resurrect completed work")
			})
		}
	}
}

func readinessBoundaryDB(t *testing.T, f readinessFixture) (*sql.DB, driver, func(int32), func() int32) {
	t.Helper()
	control := &boundaryConnector{Connector: f.connector(t)} //nolint:exhaustruct // Faults are initially disarmed.
	var connector sqldriver.Connector = control
	arm, fired := control.commitFault.Store, control.fired.Load
	if f.kind == DBTypePostgres {
		// The real PostgreSQL wire test confirms the outcome on the server;
		// SQLite uses the real transaction with a synthesized driver error.
		proxy := newReadinessProxy(t, f)
		f.dsn = proxy.dsn
		connector = f.connector(t)
		arm, fired = proxy.fault.Store, proxy.fired.Load
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	d, err := getDriver(db, f.kind)
	require.NoError(t, err)
	return db, d, arm, fired
}
