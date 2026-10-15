package sqlq_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// poolHoldingTracer takes the pool's connection when the first job starts, after the worker
// checked its claim and before it begins the handler's transaction, and keeps it for hold.
type poolHoldingTracer struct {
	trace.Tracer
	db      *sql.DB
	holding chan struct{}
	once    sync.Once
	hold    time.Duration
}

func (tr *poolHoldingTracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if name == "sqlq.consume" {
		tr.once.Do(func() {
			conn, err := tr.db.Conn(context.Background())
			if err != nil {
				panic(err)
			}
			close(tr.holding)
			time.AfterFunc(tr.hold, func() { _ = conn.Close() })
		})
	}
	return tr.Tracer.Start(ctx, name, opts...) //nolint:spancheck // A wrapper: the caller ends the span.
}

// TestHandlerNotRunAfterClaimExpiredInPoolWait checks that a worker which waited for a pool
// connection longer than its claim lasts does not run the handler. By then another consumer can have
// claimed and completed the job, and the handler's external effects would happen again.
func TestHandlerNotRunAfterClaimExpiredInPoolWait(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	const (
		jobType    = "pool_wait"
		jobTimeout = 200 * time.Millisecond // The claim lasts twice as long.
	)
	dsn := "file:" + filepath.Join(t.TempDir(), "pool.db") + "?_journal_mode=WAL&_busy_timeout=1000"
	newQueue := func(tracer trace.Tracer) (sqlq.JobsQueue, *sql.DB) {
		db, err := sql.Open("sqlite3", dsn)
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		opts := []sqlq.NewOption{sqlq.WithDefaultPollInterval(10 * time.Millisecond), sqlq.WithDefaultJobTimeout(jobTimeout)}
		if tracer != nil {
			opts = append(opts, sqlq.WithTracer(tracer))
		}
		q, err := sqlq.New(db, sqlq.DBTypeSQLite, opts...)
		require.NoError(t, err)
		t.Cleanup(func() {
			q.Shutdown()
			assert.NoError(t, db.Close())
		})
		return q, db
	}

	// The waiting worker's pool is held well past its claim's expiry.
	tracer := &poolHoldingTracer{Tracer: noop.NewTracerProvider().Tracer("pool-wait"), db: nil, holding: make(chan struct{}), once: sync.Once{}, hold: 3 * jobTimeout}
	waiting, waitingDB := newQueue(tracer)
	tracer.db = waitingDB
	waiting.Run()
	other, _ := newQueue(nil)

	var staleCalls, otherCalls atomic.Int32
	require.NoError(t, other.Publish(ctx, jobType, TestPayload{Message: "once"}))
	require.NoError(t, waiting.Consume(ctx, jobType, func(context.Context, *sql.Tx, []byte) error {
		staleCalls.Add(1)
		return nil
	}))
	select {
	case <-tracer.holding:
	case <-ctx.Done():
		t.Fatal("the job was never claimed")
	}
	// Another consumer takes the job once the waiting worker's claim expires.
	require.NoError(t, other.Consume(ctx, jobType, func(context.Context, *sql.Tx, []byte) error {
		otherCalls.Add(1)
		return nil
	}))

	require.Eventually(t, func() bool { return otherCalls.Load() == 1 }, 5*time.Second, 10*time.Millisecond,
		"the other consumer never processed the job")
	// Give the waiting worker time to get its connection back and act on the job.
	time.Sleep(3 * jobTimeout)
	require.Zero(t, staleCalls.Load(), "the handler ran after the claim expired while the worker waited for a connection")
}
