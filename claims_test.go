package sqlq

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestClaimsSQLite(t *testing.T) { //nolint:tparallel // Shared-database scenarios must run sequentially.
	t.Parallel()
	dsn := "file:" + filepath.Join(t.TempDir(), "claims.db") + "?_busy_timeout=5000&_journal_mode=WAL"
	db, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	testClaims(t, db, DBTypeSQLite, dsn, "")
}

func TestClaimsPostgres(t *testing.T) { //nolint:tparallel // Shared-database scenarios must run sequentially.
	t.Parallel()
	if testing.Short() {
		t.Skip("PostgreSQL integration requires Docker or SQLQ_TEST_POSTGRES_DSN")
	}
	dsn := os.Getenv("SQLQ_TEST_POSTGRES_DSN")
	if dsn == "" {
		container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{ //nolint:exhaustruct
			ContainerRequest: testcontainers.ContainerRequest{ //nolint:exhaustruct
				Image: "postgres:14", ExposedPorts: []string{"5432/tcp"},
				Env:        map[string]string{"POSTGRES_PASSWORD": "testpass", "POSTGRES_DB": "claims"},
				WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			},
			Started: true,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, container.Terminate(context.Background())) })
		host, err := container.Host(t.Context())
		require.NoError(t, err)
		port, err := container.MappedPort(t.Context(), "5432/tcp")
		require.NoError(t, err)
		dsn = fmt.Sprintf("postgres://postgres:testpass@%s:%s/claims?sslmode=disable", host, port.Port())
	}
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	token, err := newClaimToken()
	require.NoError(t, err)
	schema := "claims_" + token
	require.NoError(t, execSQL(t.Context(), admin, `CREATE SCHEMA `+schema))
	t.Cleanup(func() { require.NoError(t, execSQL(context.Background(), admin, `DROP SCHEMA `+schema+` CASCADE`)) })
	db, err := openClaimTestDB(DBTypePostgres, dsn, schema)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	testClaims(t, db, DBTypePostgres, dsn, schema)
}

func testClaims(t *testing.T, db *sql.DB, dbType DBType, dsn, schema string) {
	t.Helper()
	ctx := t.Context()
	d, err := getDriver(db, dbType)
	require.NoError(t, err)
	require.NoError(t, d.initSchema(ctx))
	other, err := getDriver(db, dbType) // Independent mutexes, same database.
	require.NoError(t, err)
	expire := func() {
		t.Helper()
		query := `UPDATE jobs SET claim_expires_at = 0`
		if dbType == DBTypePostgres {
			query = `UPDATE jobs SET claim_expires_at = clock_timestamp() - interval '1 second'`
		}
		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err)
	}
	seed := func(count int) []job {
		t.Helper()
		_, err := db.ExecContext(ctx, `DELETE FROM jobs`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `DELETE FROM dead_letter_queue`)
		require.NoError(t, err)
		for range count {
			require.NoError(t, d.insertJob(ctx, nil, "claims", []byte(`{}`), 0, nil))
		}
		jobs, err := d.getJobsForConsumer(ctx, "claims", uint16(count), time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, count)
		return jobs
	}

	t.Run("abandoned prefetched batch is recovered with new ownership", func(t *testing.T) {
		old := seed(3)
		jobs, err := other.getJobsForConsumer(ctx, "claims", 3, time.Minute)
		require.NoError(t, err)
		require.Empty(t, jobs)
		expire()
		jobs, err = other.getJobsForConsumer(ctx, "claims", 3, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 3)
		for i, j := range jobs {
			require.NotEqual(t, old[i].ClaimToken, j.ClaimToken)
			require.Zero(t, j.RetryCount)
		}
	})

	t.Run("stale outcomes and hook cannot affect newer attempt", func(t *testing.T) {
		old := seed(1)[0]
		expire()
		jobs, err := other.getJobsForConsumer(ctx, "claims", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		_, err = d.extendClaim(ctx, old, time.Minute)
		require.ErrorIs(t, err, ErrClaimLost)
		require.ErrorIs(t, d.releaseClaim(ctx, old), ErrClaimLost)
		require.ErrorIs(t, d.markJobFailedAndReschedule(ctx, old.ID, old.ClaimToken, "stale", 0), ErrClaimLost)
		hookCalled := false
		require.ErrorIs(t, d.moveToDeadLetterQueue(ctx, old.ID, old.ClaimToken, "stale", func(*sql.Tx) error {
			hookCalled = true
			return nil
		}), ErrClaimLost)
		require.False(t, hookCalled)
		_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS claim_effects (id INTEGER PRIMARY KEY)`)
		require.NoError(t, err)
		err = runInTx(ctx, db, func(tx *sql.Tx) error {
			if _, insertErr := tx.ExecContext(ctx, `INSERT INTO claim_effects VALUES (1)`); insertErr != nil {
				return insertErr
			}
			return d.markJobProcessed(ctx, tx, old.ID, old.ClaimToken)
		})
		require.ErrorIs(t, err, ErrClaimLost)
		var count int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM claim_effects`).Scan(&count))
		require.Zero(t, count, "stale completion must roll back handler writes")
		require.NoError(t, runInTx(ctx, db, func(tx *sql.Tx) error {
			return other.markJobProcessed(ctx, tx, jobs[0].ID, jobs[0].ClaimToken)
		}))
	})

	t.Run("expiry alone does not reject completion or redeliver processed jobs", func(t *testing.T) {
		j := seed(1)[0]
		expire()
		require.NoError(t, runInTx(ctx, db, func(tx *sql.Tx) error {
			return d.markJobProcessed(ctx, tx, j.ID, j.ClaimToken)
		}))
		jobs, err := other.getJobsForConsumer(ctx, "claims", 1, time.Minute)
		require.NoError(t, err)
		require.Empty(t, jobs)
	})

	t.Run("renewal extends only a live owned claim", func(t *testing.T) {
		j := seed(1)[0]
		deadline, err := d.extendClaim(ctx, j, 2*time.Minute)
		require.NoError(t, err)
		require.True(t, deadline.After(j.ClaimExpiresAt))
		expire()
		_, err = d.extendClaim(ctx, j, time.Minute)
		require.ErrorIs(t, err, ErrClaimLost)
	})

	t.Run("independent consumers race to recover one job", func(t *testing.T) {
		seed(1)
		expire()
		start := make(chan struct{})
		var wg sync.WaitGroup
		results := make(chan int, 2)
		errors := make(chan error, 2)
		for _, driver := range []driver{d, other} {
			wg.Go(func() {
				<-start
				jobs, err := driver.getJobsForConsumer(ctx, "claims", 1, time.Minute)
				errors <- err
				results <- len(jobs)
			})
		}
		close(start)
		wg.Wait()
		// SQLite can reject a deferred read-to-write upgrade with SQLITE_BUSY.
		// That attempt must roll back rather than return an unowned job.
		for range 2 {
			err := <-errors
			if dbType == DBTypePostgres {
				require.NoError(t, err)
			}
		}
		require.Equal(t, 1, <-results+<-results)
	})

	t.Run("consumer fast path and conditional extension", func(t *testing.T) {
		for _, scenario := range []struct {
			name       string
			remaining  time.Duration
			threshold  float64
			extensions int
			called     bool
		}{
			{"fresh", time.Minute, 0.5, 0, true},
			{"near expiry", time.Second, 0.5, 1, true},
			{"expired buffer", -time.Second, 0.5, 0, false},
			{"extension disabled", time.Second, 0, 0, true},
			{"always extend", time.Minute, 1, 1, true},
		} {
			t.Run(scenario.name, func(t *testing.T) {
				j := seed(1)[0]
				j.ClaimExpiresAt = time.Now().Add(scenario.remaining)
				spy := &claimDriverSpy{driver: d, extensions: 0}
				called := false
				cons := testClaimConsumer(ctx, db, spy, func(context.Context, *sql.Tx, []byte) error {
					called = true
					return nil
				})
				WithConsumerClaimRenewalThreshold(scenario.threshold)(cons)
				cons.processJob(&j)
				require.Equal(t, scenario.called, called)
				require.Equal(t, scenario.extensions, spy.extensions)
			})
		}
	})

	t.Run("stale buffered job fails extension without invoking handler", func(t *testing.T) {
		j := seed(1)[0]
		expire()
		_, err := other.getJobsForConsumer(ctx, "claims", 1, time.Minute)
		require.NoError(t, err)
		j.ClaimExpiresAt = time.Now().Add(time.Second)
		called := false
		cons := testClaimConsumer(ctx, db, d, func(context.Context, *sql.Tx, []byte) error {
			called = true
			return nil
		})
		cons.processJob(&j)
		require.False(t, called)
	})

	t.Run("retry releases ownership and increments only actual failures", func(t *testing.T) {
		j := seed(1)[0]
		require.NoError(t, d.markJobFailedAndReschedule(ctx, j.ID, j.ClaimToken, "failed", 0))
		jobs, err := other.getJobsForConsumer(ctx, "claims", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		require.Equal(t, uint16(1), jobs[0].RetryCount)
		require.NotEqual(t, j.ClaimToken, jobs[0].ClaimToken)
	})

	t.Run("shutdown releases active and buffered jobs without counting failures", func(t *testing.T) {
		seed(3)
		// Make all three available for the actual consumer.
		_, err := db.ExecContext(ctx, `UPDATE jobs SET claim_token = NULL, claim_expires_at = NULL, consumed_at = NULL`)
		require.NoError(t, err)
		started := make(chan struct{})
		cons := testClaimConsumer(ctx, db, d, func(ctx context.Context, _ *sql.Tx, _ []byte) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
		cons.prefetchCount = 3
		cons.jobsChan = make(chan job, 3)
		cons.start()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("handler did not start")
		}
		cons.shutdown()
		jobs, err := other.getJobsForConsumer(ctx, "claims", 3, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 3)
		for _, j := range jobs {
			require.Zero(t, j.RetryCount)
		}
	})

	t.Run("committed and rolled back DLQ moves", func(t *testing.T) {
		j := seed(1)[0]
		rejected := fmt.Errorf("hook rejected")
		require.ErrorIs(t, d.moveToDeadLetterQueue(ctx, j.ID, j.ClaimToken, "failure", func(*sql.Tx) error {
			return rejected
		}), rejected)
		var count int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs`).Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letter_queue`).Scan(&count))
		require.Zero(t, count)
		require.NoError(t, d.moveToDeadLetterQueue(ctx, j.ID, j.ClaimToken, "failure", nil))
		require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs`).Scan(&count))
		require.Zero(t, count)
		require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letter_queue`).Scan(&count))
		require.Equal(t, 1, count)
	})

	t.Run("canceled delivery releases jobs that never reached the channel", func(t *testing.T) {
		seed(3)
		require.NoError(t, execSQL(ctx, db, `UPDATE jobs SET claim_token = NULL, claim_expires_at = NULL, consumed_at = NULL`))
		cons := testClaimConsumer(ctx, db, d, nil)
		cons.prefetchCount = 3
		cons.jobsChan = make(chan job) // No receiver: delivery blocks after acquisition.
		cons.workerWg.Add(1)
		go func() {
			defer cons.workerWg.Done()
			_ = cons.fetchJobs(cons.ctx)
		}()
		require.Eventually(t, func() bool {
			cons.claimsMutex.Lock()
			defer cons.claimsMutex.Unlock()
			return len(cons.claims) == 3
		}, 5*time.Second, time.Millisecond)
		cons.shutdown()
		jobs, err := other.getJobsForConsumer(ctx, "claims", 3, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 3)
	})

	t.Run("prefetch keeps workers supplied without claiming beyond local capacity", func(t *testing.T) {
		seed(6)
		require.NoError(t, execSQL(ctx, db, `UPDATE jobs SET claim_token = NULL, claim_expires_at = NULL, consumed_at = NULL`))
		cons := testClaimConsumer(ctx, db, d, nil)
		cons.prefetchCount = 3
		cons.jobsChan = make(chan job, 3)
		require.NoError(t, cons.fetchJobs(ctx))
		<-cons.jobsChan // Simulate one active worker, retaining its outstanding claim.
		require.NoError(t, cons.fetchJobs(ctx))
		require.NoError(t, cons.fetchJobs(ctx)) // At capacity: must not acquire more.
		require.Len(t, cons.claims, 4)
		jobs, err := other.getJobsForConsumer(ctx, "claims", 6, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 2, "unneeded jobs remain available to other consumers")
		cons.shutdown()
	})

	if dbType == DBTypeSQLite {
		t.Run("write protection blocks recovery as well as extension", func(t *testing.T) {
			j := seed(1)[0]
			expire()
			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			_, err = tx.ExecContext(ctx, `INSERT INTO claim_effects VALUES (2)`)
			require.NoError(t, err)
			blockedCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			defer cancel()
			jobs, fetchErr := other.getJobsForConsumer(blockedCtx, "claims", 1, time.Minute)
			require.Error(t, fetchErr)
			require.Empty(t, jobs)
			require.NoError(t, d.markJobProcessed(ctx, tx, j.ID, j.ClaimToken))
			require.NoError(t, tx.Commit(), "expiry alone must not invalidate the protected attempt")
		})
	}

	t.Run("worker process loss rolls back running effects and recovers prefetch", func(t *testing.T) {
		seed(3)
		require.NoError(t, execSQL(ctx, db, `UPDATE jobs SET claim_token = NULL, claim_expires_at = NULL, consumed_at = NULL`))
		require.NoError(t, execSQL(ctx, db, `CREATE TABLE claim_crash_effects (id INTEGER PRIMARY KEY)`))
		cmd := exec.Command(os.Args[0], "-test.run=^TestClaimCrashHelper$")
		cmd.Env = append(os.Environ(), "SQLQ_CLAIM_CRASH_DSN="+dsn, "SQLQ_CLAIM_CRASH_TYPE="+string(dbType), "SQLQ_CLAIM_CRASH_SCHEMA="+schema)
		stdout, err := cmd.StdoutPipe()
		require.NoError(t, err)
		cmd.Stderr = os.Stderr
		require.NoError(t, cmd.Start())
		t.Cleanup(func() { _ = cmd.Process.Kill() })
		ready := make(chan string, 1)
		go func() {
			line, _ := bufio.NewReader(stdout).ReadString('\n')
			ready <- line
		}()
		select {
		case line := <-ready:
			require.Equal(t, "ready\n", line)
		case <-time.After(10 * time.Second):
			t.Fatal("crash helper did not reach its transaction")
		}
		require.NoError(t, cmd.Process.Kill())
		require.Error(t, cmd.Wait())
		var count int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM claim_crash_effects`).Scan(&count))
		require.Zero(t, count)
		expire()
		jobs, err := other.getJobsForConsumer(ctx, "claims", 3, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 3)
	})

	t.Run("legacy schema migration recovers abandoned but not processed jobs", func(t *testing.T) {
		seed(2)
		require.NoError(t, execSQL(ctx, db, `UPDATE jobs SET processed_at = consumed_at WHERE id = (SELECT MIN(id) FROM jobs)`))
		require.NoError(t, execSQL(ctx, db, `DROP INDEX idx_jobs_available_claims`))
		require.NoError(t, execSQL(ctx, db, `ALTER TABLE jobs DROP COLUMN claim_token`))
		require.NoError(t, execSQL(ctx, db, `ALTER TABLE jobs DROP COLUMN claim_expires_at`))
		require.NoError(t, d.initSchema(ctx))
		require.NoError(t, d.initSchema(ctx))
		jobs, err := other.getJobsForConsumer(ctx, "claims", 2, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
	})
}

func execSQL(ctx context.Context, db *sql.DB, query string) error {
	_, err := db.ExecContext(ctx, query)
	return err
}

func openClaimTestDB(dbType DBType, dsn, schema string) (*sql.DB, error) {
	if dbType == DBTypeSQLite {
		return sql.Open("sqlite3", dsn)
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	config.RuntimeParams["search_path"] = schema
	return stdlib.OpenDB(*config), nil
}

type claimDriverSpy struct {
	driver
	extensions int
}

func (d *claimDriverSpy) extendClaim(ctx context.Context, j job, timeout time.Duration) (time.Time, error) {
	d.extensions++
	return d.driver.extendClaim(ctx, j, timeout)
}

func testClaimConsumer(ctx context.Context, db *sql.DB, d driver, handler func(context.Context, *sql.Tx, []byte) error) *consumer {
	ctx, cancel := context.WithCancel(ctx)
	return &consumer{ //nolint:exhaustruct
		db: db, driver: d, handler: handler, ctx: ctx, cancel: cancel,
		tracer: noop.NewTracerProvider().Tracer("claims"), jobType: "claims",
		claimTimeout: time.Minute, claimRenewalThreshold: 0.5,
		claims: make(map[string]job), concurrency: 1, prefetchCount: 1,
		pollInterval: time.Millisecond, jobsChan: make(chan job, 1),
		backoffFunc: func(uint16) time.Duration { return 0 },
	}
}

// Invoked in a separate process so killing it tests database transaction cleanup,
// rather than calling Rollback to simulate worker loss.
func TestClaimCrashHelper(t *testing.T) {
	t.Parallel()
	dsn := os.Getenv("SQLQ_CLAIM_CRASH_DSN")
	if dsn == "" {
		t.Skip("subprocess helper")
	}
	dbType := DBType(os.Getenv("SQLQ_CLAIM_CRASH_TYPE"))
	db, err := openClaimTestDB(dbType, dsn, os.Getenv("SQLQ_CLAIM_CRASH_SCHEMA"))
	require.NoError(t, err)
	d, err := getDriver(db, dbType)
	require.NoError(t, err)
	jobs, err := d.getJobsForConsumer(t.Context(), "claims", 3, time.Minute)
	require.NoError(t, err)
	require.Len(t, jobs, 3)
	tx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), `INSERT INTO claim_crash_effects VALUES (1)`)
	require.NoError(t, err)
	fmt.Println("ready")
	select {}
}
