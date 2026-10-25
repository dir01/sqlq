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

// poolHoldingTracer takes the pool connection when the first job starts. This is after the worker
// examines its claim, and before it starts the handler transaction. It keeps the connection for hold.
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
	return tr.Tracer.Start(ctx, name, opts...) //nolint:spancheck // This is a wrapper. The caller ends the span.
}

// TestHandlerNotRunAfterClaimExpiredInPoolWait makes sure that a worker does not run the handler if
// its wait for a pool connection is longer than its claim. At that time, a different consumer can
// have the job, or can have completed it. Then the external effects of the handler occur again.
func TestHandlerNotRunAfterClaimExpiredInPoolWait(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	const (
		jobType    = "pool_wait"
		jobTimeout = 200 * time.Millisecond // The claim is two times longer.
	)
	dsn := "file:" + filepath.Join(t.TempDir(), "pool.db") + "?_journal_mode=WAL&_busy_timeout=1000"
	newQueue := func(tracer trace.Tracer) (sqlq.JobsQueue, *sql.DB) {
		db, err := sql.Open("sqlite3", dsn)
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		opts := []sqlq.NewOption{sqlq.WithDefaultPollDelayFunc(pollEvery(10 * time.Millisecond)), sqlq.WithDefaultJobTimeout(jobTimeout)}
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

	// The pool of the worker stays locked until long after its claim expires.
	tracer := &poolHoldingTracer{Tracer: noop.NewTracerProvider().Tracer("pool-wait"), db: nil, holding: make(chan struct{}), once: sync.Once{}, hold: 3 * jobTimeout}
	waiting, waitingDB := newQueue(tracer)
	tracer.db = waitingDB
	require.NoError(t, waiting.Run())
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
	// A different consumer takes the job after the claim of the worker expires.
	require.NoError(t, other.Consume(ctx, jobType, func(context.Context, *sql.Tx, []byte) error {
		otherCalls.Add(1)
		return nil
	}))

	require.Eventually(t, func() bool { return otherCalls.Load() == 1 }, 5*time.Second, 10*time.Millisecond,
		"the other consumer never processed the job")
	// Give the worker time to get its connection again and to process the job.
	time.Sleep(3 * jobTimeout)
	require.Zero(t, staleCalls.Load(), "the handler ran after the claim expired while the worker waited for a connection")
}
