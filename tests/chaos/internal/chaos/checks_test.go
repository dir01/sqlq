package chaos

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
	"time"
)

// Gate the real database/sql cancellation goroutine's rollback so the test
// reliably observes DB.Close returning with a connection still checked out.
type rollbackGate struct{ started, release chan struct{} }
type gatedConnector struct{ gate *rollbackGate }

func (c gatedConnector) Connect(context.Context) (driver.Conn, error) {
	return gatedConnection{gate: c.gate}, nil
}
func (c gatedConnector) Driver() driver.Driver { return unusedDriver{} }

type unusedDriver struct{}

func (unusedDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type gatedConnection struct{ gate *rollbackGate }

func (c gatedConnection) Begin() (driver.Tx, error)         { return gatedTransaction{gate: c.gate}, nil }
func (gatedConnection) Close() error                        { return nil }
func (gatedConnection) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not needed") }

type gatedTransaction struct{ gate *rollbackGate }

func (gatedTransaction) Commit() error      { return nil }
func (tx gatedTransaction) Rollback() error { close(tx.gate.started); <-tx.gate.release; return nil }

func TestRuntimeWaitsForCancellationRollback(t *testing.T) {
	gate := &rollbackGate{started: make(chan struct{}), release: make(chan struct{})}
	db := sql.OpenDB(gatedConnector{gate: gate})
	var once sync.Once
	release := func() { once.Do(func() { close(gate.release) }) }
	t.Cleanup(func() { release(); _ = db.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if _, err := db.BeginTx(ctx, nil); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-gate.started:
	case <-time.After(time.Second):
		t.Fatal("rollback did not start")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if db.Stats().InUse != 1 {
		t.Fatal("fixture did not retain the rollback connection")
	}
	go func() { time.Sleep(5 * time.Millisecond); release() }()
	report, err := checkRuntime(t.ArtifactDir(), "async-rollback", db, false, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if report.InitialDatabase.InUse != 1 || report.Database.InUse != 0 || report.Database.OpenConnections != 0 || !report.Passed {
		t.Fatalf("rollback was not drained: %+v", report)
	}
}

func TestPoolDrainRejectsRetainedConnection(t *testing.T) {
	db := sql.OpenDB(gatedConnector{})
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()
	stats, err := waitClosedPool(ctx, db)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) || stats.InUse != 1 {
		t.Fatalf("retained connection accepted: stats=%+v err=%v", stats, err)
	}
}
