package sqlq

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

// These tests are also compiled into the VM image. PostgreSQL uses the guest's
// existing server, never a nested Docker container. Each suite owns its schema.
func TestReadinessSQLite(t *testing.T) { //nolint:tparallel // Scenarios share a database and deliberately mutate its state.
	t.Parallel()
	f := readinessFixture{kind: DBTypeSQLite, dsn: "file:" + filepath.Join(t.TempDir(), "readiness.db") + "?_journal_mode=WAL&_busy_timeout=1000&_synchronous=FULL", schema: ""}
	testReadiness(t, f)
}

func TestReadinessPostgres(t *testing.T) { //nolint:paralleltest // May restart the dedicated VM database server.
	dsn := os.Getenv("SQLQ_TEST_POSTGRES_DSN")
	if testing.Short() || dsn == "" {
		t.Skip("set SQLQ_TEST_POSTGRES_DSN to an isolated test database (the VM supplies it)")
	}
	f := readinessFixture{kind: DBTypePostgres, dsn: dsn, schema: "readiness_" + strings.ToLower(newClaimToken())}
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer func() { require.NoError(t, admin.Close()) }()
	require.NoError(t, execSQL(t.Context(), admin, `CREATE SCHEMA `+f.schema))
	defer func() { require.NoError(t, execSQL(context.Background(), admin, `DROP SCHEMA `+f.schema+` CASCADE`)) }()
	testReadiness(t, f)
}

type readinessFixture struct {
	kind   DBType
	dsn    string
	schema string
}

func (f readinessFixture) connector(t *testing.T) sqldriver.Connector {
	t.Helper()
	if f.kind == DBTypeSQLite {
		return sqliteTestConnector{dsn: f.dsn}
	}
	c, err := pgx.ParseConfig(f.dsn)
	require.NoError(t, err)
	c.RuntimeParams["search_path"] = f.schema
	c.RuntimeParams["application_name"] = f.schema
	// No lock timeout: cancellation tests must prove queue cancellation works.
	c.RuntimeParams["lock_timeout"] = "0"
	return stdlib.GetConnector(*c)
}

func (f readinessFixture) open(t *testing.T) (*sql.DB, driver) {
	t.Helper()
	db := sql.OpenDB(f.connector(t))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	d, err := getDriver(db, f.kind)
	require.NoError(t, err)
	require.NoError(t, d.initSchema(t.Context()))
	return db, d
}

type sqliteTestConnector struct{ dsn string }

func (c sqliteTestConnector) Connect(context.Context) (sqldriver.Conn, error) {
	return c.Driver().Open(c.dsn)
}

func (sqliteTestConnector) Driver() sqldriver.Driver {
	return &sqlite3.SQLiteDriver{Extensions: nil, ConnectHook: nil}
}

func testReadiness(t *testing.T, f readinessFixture) {
	t.Helper()
	db, d := f.open(t)
	for _, statement := range []string{
		`CREATE TABLE readiness_effects (value TEXT NOT NULL)`,
	} {
		require.NoError(t, execSQL(t.Context(), db, statement))
	}
	reset := func(t *testing.T) {
		t.Helper()
		for _, table := range []string{"jobs", "dead_letter_queue", "readiness_effects"} {
			require.NoError(t, execSQL(t.Context(), db, `DELETE FROM `+table))
		}
	}
	t.Run("retry-boundary", func(t *testing.T) {
		for _, initial := range []uint32{65534, 65535, 2147483646, 2147483647} {
			t.Run(fmt.Sprint(initial), func(t *testing.T) {
				reset(t)
				require.NoError(t, d.insertJob(t.Context(), nil, "claims", []byte("retry"), 0, nil))
				require.NoError(t, execSQL(t.Context(), db, fmt.Sprintf(`UPDATE jobs SET retry_count=%d`, initial)))
				require.NoError(t, d.insertJob(t.Context(), nil, "claims", []byte("healthy"), 0, nil))
				var retries []uint32
				var backoffs []uint16
				healthy := 0
				cons := testClaimConsumer(t.Context(), db, d, func(ctx context.Context, _ *sql.Tx, payload []byte) error {
					if string(payload) == "healthy" {
						healthy++
						return nil
					}
					info, ok := JobInfoFromContext(ctx)
					if !ok {
						return errors.New("missing job info")
					}
					retries = append(retries, info.RetryCount)
					return errors.New("persistent failure")
				})
				defer cons.cancel()
				cons.maxRetries = -1
				cons.backoffFunc = func(n uint16) time.Duration { backoffs = append(backoffs, n); return 0 }
				for range 3 {
					jobs, err := d.getJobsForConsumer(t.Context(), "claims", 2, time.Minute)
					require.NoError(t, err, "one exhausted counter must not poison an entire fetch batch")
					require.NotEmpty(t, jobs)
					for i := range jobs {
						cons.processJob(&jobs[i])
					}
				}
				require.Equal(t, 1, healthy)
				require.Equal(t, []uint32{initial, min(initial+1, 2147483647), min(initial+2, 2147483647)}, retries)
				require.Equal(t, []uint16{65535, 65535, 65535}, backoffs)
				require.Equal(t, 24*time.Hour, exponentialBackoff(65535))
			})
		}
		t.Run("finite-limit-above-uint16", func(t *testing.T) {
			reset(t)
			require.NoError(t, d.insertJob(t.Context(), nil, "claims", []byte("retry"), 0, nil))
			require.NoError(t, execSQL(t.Context(), db, `UPDATE jobs SET retry_count=65535`))
			cons := testClaimConsumer(t.Context(), db, d, func(context.Context, *sql.Tx, []byte) error { return errors.New("fail") })
			defer cons.cancel()
			cons.maxRetries = 65536
			for range 2 {
				jobs, err := d.getJobsForConsumer(t.Context(), "claims", 1, time.Minute)
				require.NoError(t, err)
				require.Len(t, jobs, 1)
				cons.processJob(&jobs[0])
			}
			dlq, err := d.getDeadLetterJobs(t.Context(), "claims", 10)
			require.NoError(t, err)
			require.Len(t, dlq, 1)
			require.Equal(t, uint32(65536), dlq[0].RetryCount)
		})
	})
	t.Run("cleanup-shutdown-pool-wait", func(t *testing.T) {
		for _, dlq := range []bool{false, true} {
			t.Run(fmt.Sprintf("dlq=%t", dlq), func(t *testing.T) {
				cleanupDB, cleanupDriver := f.open(t)
				cleanupDB.SetMaxOpenConns(1)
				held, err := cleanupDB.Conn(t.Context())
				require.NoError(t, err)
				defer func() { require.NoError(t, held.Close()) }()
				// Holding SQLite's driver mutex must not obstruct cancellation either.
				if sqlite, ok := cleanupDriver.(*SQLiteDriver); ok {
					require.NoError(t, sqlite.lock(t.Context()))
					defer sqlite.unlock()
				}
				cons := testClaimConsumer(t.Context(), cleanupDB, cleanupDriver, nil)
				defer cons.cancel()
				cons.cleanupBatch = 1
				cons.cleanupProcessedInterval, cons.cleanupDLQInterval = time.Millisecond, time.Millisecond
				if dlq {
					cons.workerWg.Go(cons.runDLQCleanupLoop)
				} else {
					cons.workerWg.Go(cons.runProcessedCleanupLoop)
				}
				awaitReadiness(t, "cleanup waiting for a pool connection", func() bool { return cleanupDB.Stats().WaitCount > 0 })
				done := make(chan struct{})
				go func() { cons.shutdown(); close(done) }()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("shutdown did not cancel cleanup while the connection remained held")
				}
			})
		}
	})
	if f.kind == DBTypePostgres {
		t.Run("concurrent-requeue", func(t *testing.T) { reset(t); testConcurrentRequeue(t, f, db, d) })
		t.Run("cleanup-shutdown-row-lock", func(t *testing.T) { reset(t); testCleanupRowLock(t, f, db, d) })
	}
	t.Run("commit-ambiguity", func(t *testing.T) { testCommitAmbiguity(t, f, db, reset) })
	t.Run("queue-transition-ambiguity", func(t *testing.T) { testTransitionAmbiguity(t, f, db, reset) })
	t.Run("transactional-publishing", func(t *testing.T) { testReadinessPublishing(t, f, db, reset) })
	t.Run("cleanup-eligibility", func(t *testing.T) { reset(t); testCleanupEligibility(t, f, db, d) })
	t.Run("stale-worker-resumption", func(t *testing.T) { reset(t); testStaleResumption(t, f, db, d) })
	if f.kind == DBTypePostgres {
		t.Run("in-place-recovery", func(t *testing.T) { reset(t); testInPlaceRecovery(t, f, db) })
	}
}

func awaitReadiness(t *testing.T, boundary string, ready func() bool) {
	t.Helper()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if ready() {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatalf("did not reach boundary: %s", boundary)
		case <-t.Context().Done():
			t.Fatalf("canceled waiting for boundary: %s", boundary)
		}
	}
}

func countReadiness(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var count int
	require.NoError(t, db.QueryRowContext(ctx, query).Scan(&count))
	return count
}

func testConcurrentRequeue(t *testing.T, f readinessFixture, db *sql.DB, d driver) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, d.insertJob(ctx, nil, "requeue", []byte(`{}`), 0, nil))
	jobs, err := d.getJobsForConsumer(ctx, "requeue", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.NoError(t, d.moveToDeadLetterQueue(ctx, jobs[0].ID, jobs[0].ClaimToken, "test", nil))
	// Hold replacement INSERTs after any DLQ read/delete. In the old code both
	// callers read the row and block here; in the fixed code one blocks on DELETE.
	require.NoError(t, execSQL(ctx, db, `CREATE FUNCTION gate_requeue() RETURNS trigger LANGUAGE plpgsql AS $$
	BEGIN PERFORM pg_advisory_xact_lock(hashtext(TG_TABLE_SCHEMA), 17); RETURN NEW; END $$`))
	require.NoError(t, execSQL(ctx, db, `CREATE TRIGGER gate_requeue BEFORE INSERT ON jobs FOR EACH ROW EXECUTE FUNCTION gate_requeue()`))
	defer func() {
		require.NoError(t, execSQL(context.Background(), db, `DROP TRIGGER gate_requeue ON jobs; DROP FUNCTION gate_requeue()`))
	}()
	gate, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, gate.Close()) }()
	_, err = gate.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext($1), 17)`, f.schema)
	require.NoError(t, err)
	unlocked := false
	defer func() {
		if !unlocked {
			_, _ = gate.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtext($1), 17)`, f.schema)
		}
	}()
	_, other := f.open(t)
	results := make(chan error, 2)
	for _, caller := range []driver{d, other} {
		go func() {
			_, requeueErr := caller.requeueDeadLetterJob(ctx, jobs[0].ID)
			results <- requeueErr
		}()
	}
	awaitReadiness(t, "both requeue transactions blocked", func() bool {
		return countReadiness(t, db, `SELECT count(*) FROM pg_stat_activity WHERE application_name='`+f.schema+`' AND wait_event_type='Lock'`) == 2
	})
	_, err = gate.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext($1), 17)`, f.schema)
	require.NoError(t, err)
	unlocked = true
	var successes, missing int
	for range 2 {
		select {
		case outcome := <-results:
			if outcome == nil {
				successes++
			} else {
				require.ErrorIs(t, outcome, ErrJobNotFound)
				missing++
			}
		case <-time.After(30 * time.Second):
			t.Fatal("requeue did not finish after gate release")
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, missing)
	require.Equal(t, 1, countReadiness(t, db, `SELECT count(*) FROM jobs`))
	require.Zero(t, countReadiness(t, db, `SELECT count(*) FROM dead_letter_queue`))
	// Taking the row must remain transactional if replacement insertion fails.
	replacements, err := d.getJobsForConsumer(ctx, "requeue", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, replacements, 1)
	require.NoError(t, d.moveToDeadLetterQueue(ctx, replacements[0].ID, replacements[0].ClaimToken, "test", nil))
	require.NoError(t, execSQL(ctx, db, `CREATE OR REPLACE FUNCTION gate_requeue() RETURNS trigger LANGUAGE plpgsql AS $$
	BEGIN RAISE EXCEPTION 'injected replacement failure'; END $$`))
	_, err = d.requeueDeadLetterJob(ctx, replacements[0].ID)
	require.ErrorContains(t, err, "injected replacement failure")
	require.Equal(t, 1, countReadiness(t, db, `SELECT count(*) FROM dead_letter_queue`))
	require.Zero(t, countReadiness(t, db, `SELECT count(*) FROM jobs`))
}

func testCleanupRowLock(t *testing.T, f readinessFixture, db *sql.DB, d driver) {
	t.Helper()
	for _, dlq := range []bool{false, true} {
		t.Run(fmt.Sprintf("dlq=%t", dlq), func(t *testing.T) {
			require.NoError(t, d.insertJob(t.Context(), nil, "claims", []byte(`{}`), 0, nil))
			jobs, err := d.getJobsForConsumer(t.Context(), "claims", 1, time.Minute)
			require.NoError(t, err)
			require.Len(t, jobs, 1)
			table := "jobs"
			if dlq {
				require.NoError(t, d.moveToDeadLetterQueue(t.Context(), jobs[0].ID, jobs[0].ClaimToken, "test", nil))
				table = "dead_letter_queue"
			} else {
				require.NoError(t, execSQL(t.Context(), db, `UPDATE jobs SET processed_at=clock_timestamp()-interval '1 day'`))
			}
			lock, err := db.BeginTx(t.Context(), nil)
			require.NoError(t, err)
			defer func() { _ = lock.Rollback() }()
			_, err = lock.ExecContext(t.Context(), `SELECT * FROM `+table+` FOR UPDATE`)
			require.NoError(t, err)
			cons := testClaimConsumer(t.Context(), db, d, nil)
			defer cons.cancel()
			cons.cleanupBatch = 1
			cons.cleanupProcessedInterval, cons.cleanupDLQInterval = time.Millisecond, time.Millisecond
			if dlq {
				cons.workerWg.Go(cons.runDLQCleanupLoop)
			} else {
				cons.workerWg.Go(cons.runProcessedCleanupLoop)
			}
			awaitReadiness(t, "cleanup blocked on row lock", func() bool {
				return countReadiness(t, db, `SELECT count(*) FROM pg_stat_activity WHERE application_name='`+f.schema+`' AND wait_event_type='Lock'`) > 0
			})
			done := make(chan struct{})
			go func() { cons.shutdown(); close(done) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("cleanup ignored shutdown cancellation while row lock remained held")
			}
		})
	}
}
