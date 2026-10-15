package sqlq

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// readinessProxy understands PostgreSQL message boundaries, not TCP chunk
// boundaries. For fault 2 it reads the server's CommandComplete(COMMIT) before
// dropping the connection, proving the commit succeeded without delivering its
// acknowledgment. Fault 1 closes before forwarding COMMIT at all.
type readinessProxy struct {
	listener net.Listener
	conns    map[net.Conn]bool
	target   string
	dsn      string
	mu       sync.Mutex
	blocked  bool
	closed   bool
	wg       sync.WaitGroup
	fault    atomic.Int32
	fired    atomic.Int32
	rejected atomic.Int64
}

func newReadinessProxy(t *testing.T, f readinessFixture) *readinessProxy {
	t.Helper()
	c, err := pgx.ParseConfig(f.dsn)
	require.NoError(t, err)
	require.Nil(t, c.TLSConfig, "readiness proxy requires an isolated sslmode=disable test database")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	query := url.Values{"sslmode": {"disable"}, "default_query_exec_mode": {"simple_protocol"}}
	for k, v := range c.RuntimeParams {
		query.Set(k, v)
	}
	u := url.URL{Scheme: "postgres", User: url.UserPassword(c.User, c.Password), Host: ln.Addr().String(), Path: "/" + c.Database, RawQuery: query.Encode()} //nolint:exhaustruct // Only URL connection fields are needed.
	p := &readinessProxy{listener: ln, target: net.JoinHostPort(c.Host, fmt.Sprint(c.Port)), dsn: u.String(), conns: make(map[net.Conn]bool)}                //nolint:exhaustruct // Faults are initially disarmed.
	t.Cleanup(p.close)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.blocked || p.closed {
				p.rejected.Add(1)
				_ = client.Close()
				p.mu.Unlock()
				continue
			}
			p.conns[client] = true
			p.wg.Add(1)
			p.mu.Unlock()
			go func() { defer p.wg.Done(); p.serve(client) }()
		}
	}()
	return p
}

func (p *readinessProxy) serve(client net.Conn) {
	defer func() { _ = client.Close(); p.mu.Lock(); delete(p.conns, client); p.mu.Unlock() }()
	server, err := net.DialTimeout("tcp", p.target, time.Second)
	if err != nil {
		p.rejected.Add(1)
		return
	}
	defer func() { _ = server.Close() }()
	p.mu.Lock()
	if p.blocked || p.closed {
		p.mu.Unlock()
		return
	}
	p.conns[server] = true
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.conns, server); p.mu.Unlock() }()
	// Startup has a length but no message-type byte; all subsequent messages do.
	startup, err := readPGFrame(client)
	if err != nil {
		return
	}
	if _, err := server.Write(startup); err != nil {
		return
	}
	var afterCommit atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = client.Close(); _ = server.Close() }()
		for {
			var kind [1]byte
			if _, err := io.ReadFull(client, kind[:]); err != nil {
				return
			}
			frame, err := readPGFrame(client)
			if err != nil {
				return
			}
			if kind[0] == 'Q' && strings.EqualFold(strings.TrimSpace(strings.TrimSuffix(string(frame[4:]), "\x00")), "COMMIT") {
				switch p.fault.Swap(0) {
				case 1:
					p.fired.Store(1)
					return
				case 2:
					afterCommit.Store(true)
				}
			}
			if _, err := server.Write(append(kind[:], frame...)); err != nil {
				return
			}
		}
	}()
	defer func() { _ = client.Close(); _ = server.Close(); <-done }()
	for {
		var kind [1]byte
		if _, err := io.ReadFull(server, kind[:]); err != nil {
			return
		}
		frame, err := readPGFrame(server)
		if err != nil {
			return
		}
		if kind[0] == 'C' && string(frame[4:]) == "COMMIT\x00" && afterCommit.Load() {
			p.fired.Store(2)
			return
		}
		if _, err := client.Write(append(kind[:], frame...)); err != nil {
			return
		}
	}
}

func readPGFrame(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n < 4 || n > 16*1024*1024 {
		return nil, fmt.Errorf("invalid PostgreSQL test frame length %d", n)
	}
	frame := make([]byte, int(n))
	copy(frame, header[:])
	_, err := io.ReadFull(r, frame[4:])
	return frame, err
}

func (p *readinessProxy) partition(blocked bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocked = blocked
	if blocked {
		for conn := range p.conns {
			_ = conn.Close()
		}
	}
}

func (p *readinessProxy) close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	_ = p.listener.Close()
	p.partition(true)
	p.wg.Wait()
}

func testInPlaceRecovery(t *testing.T, f readinessFixture, db *sql.DB) {
	t.Helper()
	p := newReadinessProxy(t, f)
	f.dsn = p.dsn
	workerDB := sql.OpenDB(f.connector(t))
	defer func() { require.NoError(t, workerDB.Close()) }()
	q, err := New(workerDB, DBTypePostgres,
		WithDefaultConcurrency(2), WithDefaultPrefetchCount(2), WithDefaultPollInterval(20*time.Millisecond),
		WithDefaultJobTimeout(2*time.Second), WithDefaultClaimTimeout(5*time.Second),
		WithDefaultMaxRetries(-1), WithDefaultBackoffFunc(func(uint16) time.Duration { return 0 }),
		WithDefaultCleanupProcessedInterval(0), WithDefaultCleanupDLQInterval(0))
	require.NoError(t, err)
	defer func() { require.NoError(t, q.Shutdown(t.Context())) }()
	// The admin/publisher bypasses network impairment. The consumer queue and
	// pool below are created once and stay alive through every recovery cycle.
	publisher, err := New(db, DBTypePostgres)
	require.NoError(t, err)
	defer func() { require.NoError(t, publisher.Shutdown(t.Context())) }()
	restartWritten := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var gateOnce atomic.Bool
	require.NoError(t, q.Consume(t.Context(), "recovery", func(ctx context.Context, tx *sql.Tx, payload []byte) error {
		var value string
		if err := json.Unmarshal(payload, &value); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO readiness_effects VALUES ($1)`, value); err != nil {
			return err
		}
		if value == "restart-inflight" && !gateOnce.Swap(true) {
			close(restartWritten)
			// Keep this transaction open until the supervisor has crashed the
			// server. A timer must not roll it back before the intended boundary.
			<-release
		}
		return nil
	}))
	assertEffect := func(value string) {
		awaitReadiness(t, "committed "+value, func() bool {
			return countReadiness(t, db, `SELECT count(*) FROM readiness_effects WHERE value='`+value+`'`) == 1
		})
	}
	require.NoError(t, publisher.Publish(t.Context(), "recovery", "baseline"))
	assertEffect("baseline")
	for cycle := range 3 {
		value := fmt.Sprintf("outage-%d", cycle)
		before := p.rejected.Load()
		p.partition(true)
		require.NoError(t, publisher.Publish(t.Context(), "recovery", value))
		awaitReadiness(t, "existing consumers observed outage", func() bool { return p.rejected.Load() > before })
		require.Zero(t, countReadiness(t, db, `SELECT count(*) FROM readiness_effects WHERE value='`+value+`'`))
		p.partition(false)
		assertEffect(value)
	}
	t.Run("database-crash-restart", func(t *testing.T) {
		data := os.Getenv("SQLQ_TEST_POSTGRES_DATA_DIR")
		if data == "" {
			t.Skip("VM supplies SQLQ_TEST_POSTGRES_DATA_DIR for its disposable PostgreSQL server")
		}
		require.NoError(t, publisher.Publish(t.Context(), "recovery", "restart-inflight"))
		select {
		case <-restartWritten:
		case <-time.After(30 * time.Second):
			t.Fatal("handler never reached its uncommitted write")
		}
		// Immediate shutdown forces WAL crash recovery using the same data directory.
		pgctl := func(args ...string) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			command := append([]string{"-u", "postgres", "--", "pg_ctl", "-D", data}, args...)
			out, err := exec.CommandContext(ctx, "runuser", command...).CombinedOutput()
			if err != nil {
				return fmt.Errorf("pg_ctl %v: %w\n%s", args, err, out)
			}
			return nil
		}
		require.NoError(t, pgctl("-m", "immediate", "-w", "stop"))
		restarted := false
		defer func() {
			if !restarted {
				_ = pgctl("-l", filepath.Join(data, "server.log"), "-w", "start")
			}
		}()
		before := p.rejected.Load()
		releaseOnce.Do(func() { close(release) })
		awaitReadiness(t, "consumer reconnect while server stopped", func() bool { return p.rejected.Load() > before })
		require.NoError(t, pgctl("-l", filepath.Join(data, "server.log"), "-w", "start"))
		restarted = true
		assertEffect("restart-inflight")
		require.NoError(t, publisher.Publish(t.Context(), "recovery", "after-restart"))
		assertEffect("after-restart")
	})
	require.NoError(t, q.Shutdown(t.Context()))
	require.Zero(t, countReadiness(t, db, `SELECT count(*) FROM jobs WHERE processed_at IS NULL`))
	require.Zero(t, countReadiness(t, db, `SELECT count(*) FROM dead_letter_queue`))
	require.Zero(t, countReadiness(t, db, `SELECT count(*) FROM (SELECT value FROM readiness_effects GROUP BY value HAVING count(*) <> 1) duplicates`))
	require.NoError(t, workerDB.Close())
	awaitReadiness(t, "worker pool drained", func() bool { return workerDB.Stats().OpenConnections == 0 })
}
