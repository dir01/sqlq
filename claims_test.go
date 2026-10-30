package sqlq

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestClaimsSQLite(t *testing.T) { //nolint:tparallel // Subtests share one database, so they run one at a time.
	t.Parallel()
	dsn := "file:" + filepath.Join(t.TempDir(), "claims.db") + "?_busy_timeout=5000&_journal_mode=WAL"
	db, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	testClaims(t, db, DBTypeSQLite, dsn, "")
}

func TestClaimsPostgres(t *testing.T) { //nolint:tparallel // Subtests share one database, so they run one at a time.
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
	schema := "claims_" + strings.ToLower(newClaimToken())
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
	other, err := getDriver(db, dbType) // Acts as a second process: same database, its own mutex.
	require.NoError(t, err)
	expire := func() {
		t.Helper()
		query := `UPDATE sqlq_jobs SET claim_expires_at = 0`
		if dbType == DBTypePostgres {
			query = `UPDATE sqlq_jobs SET claim_expires_at = clock_timestamp() - interval '1 second'`
		}
		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err)
	}
	seed := func(count int) []job {
		t.Helper()
		_, err := db.ExecContext(ctx, `DELETE FROM sqlq_jobs`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `DELETE FROM sqlq_dead_letter_queue`)
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
		_, err = d.extendClaim(ctx, old, time.Minute, false)
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
		deadline, err := d.extendClaim(ctx, j, 2*time.Minute, false)
		require.NoError(t, err)
		require.True(t, deadline.After(j.ClaimExpiresAt))
		expire()
		_, err = d.extendClaim(ctx, j, time.Minute, false)
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
		for range 2 {
			require.NoError(t, <-errors)
		}
		require.Equal(t, 1, <-results+<-results)
	})

	t.Run("consumer fast path and conditional extension", func(t *testing.T) {
		// Each attempt that starts writes one time: the write counts the attempt and renews the claim.
		// The extended counter shows the jobs that waited so long that the claim was too short.
		for _, scenario := range []struct {
			name      string
			remaining time.Duration
			extended  int
			expired   int
			called    bool
		}{
			// testClaimConsumer has a 10s job timeout, so a handler needs 12.5s of claim time.
			{"fresh", time.Minute, 0, 0, true},
			{"enough for job timeout and margin", 20 * time.Second, 0, 0, true},
			{"enough for job timeout but not margin", 12 * time.Second, 1, 0, true},
			{"near expiry", time.Second, 1, 0, true},
			// The fetch query sees an expired claim as available, so the worker sends no release.
			{"expired buffer", -time.Second, 0, 1, false},
		} {
			t.Run(scenario.name, func(t *testing.T) {
				j := seed(1)[0]
				j.ClaimExpiresAt = time.Now().Add(scenario.remaining)
				spy := &claimDriverSpy{driver: d, starts: 0, extensions: 0, releases: 0}
				called := false
				cons := testClaimConsumer(ctx, db, spy, func(context.Context, *sql.Tx, []byte) error {
					called = true
					return nil
				})
				metrics := sdkmetric.NewManualReader()
				var err error
				cons.metrics, err = newMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(metrics)).Meter("claims"))
				require.NoError(t, err)
				cons.processJob(&j)
				require.Equal(t, scenario.called, called)
				wantStarts := 0
				if scenario.called {
					wantStarts = 1
				}
				require.Equal(t, wantStarts, spy.starts)
				require.Zero(t, spy.extensions)
				require.Zero(t, spy.releases)
				require.Equal(t, int64(scenario.extended), counterValue(t, metrics, "sqlq.jobs.claim.extended.count"))
				require.Equal(t, int64(scenario.expired), counterValue(t, metrics, "sqlq.jobs.claim.expired.count"))
			})
		}
	})

	t.Run("a result after a lost claim is counted and not written", func(t *testing.T) {
		j := seed(1)[0]
		spy := &claimDriverSpy{driver: d, starts: 0, extensions: 0, releases: 0}
		cons := testClaimConsumer(ctx, db, spy, func(context.Context, *sql.Tx, []byte) error {
			// While the handler runs, the claim expires, and a different consumer takes the job.
			expire()
			jobs, err := other.getJobsForConsumer(ctx, "claims", 1, time.Minute)
			if err == nil && len(jobs) != 1 {
				err = fmt.Errorf("the other consumer got %d jobs", len(jobs))
			}
			return err
		})
		metrics := sdkmetric.NewManualReader()
		var err error
		cons.metrics, err = newMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(metrics)).Meter("claims"))
		require.NoError(t, err)
		cons.processJob(&j)
		require.Equal(t, int64(1), counterValue(t, metrics, "sqlq.jobs.claim.lost.count"))
		require.Zero(t, spy.releases)
		var processed int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlq_jobs WHERE processed_at IS NOT NULL`).Scan(&processed))
		require.Zero(t, processed)
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
		_, err := d.extendClaim(ctx, j, time.Minute, true)
		require.NoError(t, err)
		require.NoError(t, d.markJobFailedAndReschedule(ctx, j.ID, j.ClaimToken, "failed", 0))
		jobs, err := other.getJobsForConsumer(ctx, "claims", 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		require.Equal(t, uint32(1), jobs[0].RetryCount)
		require.Equal(t, "failed", jobs[0].LastError)
		require.NotEqual(t, j.ClaimToken, jobs[0].ClaimToken)
	})

	// The start of an attempt counts it. Only a result or a release changes the count again.
	t.Run("an attempt counts when it starts", func(t *testing.T) {
		for _, scenario := range []struct {
			finish  func(j job)
			name    string
			retries uint32
		}{
			{func(job) { expire() }, "the worker stops", 1},
			{func(j job) { require.NoError(t, d.releaseClaim(ctx, j)) }, "the worker releases the claim", 0},
		} {
			t.Run(scenario.name, func(t *testing.T) {
				j := seed(1)[0]
				_, err := d.extendClaim(ctx, j, time.Minute, true)
				require.NoError(t, err)
				scenario.finish(j)
				jobs, err := other.getJobsForConsumer(ctx, "claims", 1, time.Minute)
				require.NoError(t, err)
				require.Len(t, jobs, 1)
				require.Equal(t, scenario.retries, jobs[0].RetryCount)
				require.Empty(t, jobs[0].LastError)
			})
		}
		t.Run("a success is not a retry", func(t *testing.T) {
			j := seed(1)[0]
			_, err := d.extendClaim(ctx, j, time.Minute, true)
			require.NoError(t, err)
			require.NoError(t, runInTx(ctx, db, func(tx *sql.Tx) error { return d.markJobProcessed(ctx, tx, j.ID, j.ClaimToken) }))
			var retries int
			require.NoError(t, db.QueryRowContext(ctx, `SELECT retry_count FROM sqlq_jobs`).Scan(&retries))
			require.Zero(t, retries)
		})
		t.Run("the dead letter queue does not count the final attempt as a retry", func(t *testing.T) {
			j := seed(1)[0]
			_, err := d.extendClaim(ctx, j, time.Minute, true)
			require.NoError(t, err)
			require.NoError(t, d.moveToDeadLetterQueue(ctx, j.ID, j.ClaimToken, "failed", nil))
			dlq, err := d.getDeadLetterJobs(ctx, "claims", 1)
			require.NoError(t, err)
			require.Len(t, dlq, 1)
			require.Zero(t, dlq[0].RetryCount)
		})
	})

	t.Run("shutdown releases active and buffered jobs without counting failures", func(t *testing.T) {
		seed(3)
		// Clear seed's claims, so the consumer below can claim all three jobs.
		_, err := db.ExecContext(ctx, `UPDATE sqlq_jobs SET claim_token = NULL, claim_expires_at = NULL, consumed_at = NULL`)
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
		count := func(query string) int {
			t.Helper()
			var n int
			require.NoError(t, db.QueryRowContext(ctx, query).Scan(&n))
			return n
		}
		inJobs := fmt.Sprintf(`SELECT COUNT(*) FROM sqlq_jobs WHERE id = %d`, j.ID)
		stillClaimed := fmt.Sprintf(`SELECT COUNT(*) FROM sqlq_jobs WHERE id = %d AND claim_token = '%s'`, j.ID, j.ClaimToken)
		inDLQ := fmt.Sprintf(`SELECT COUNT(*) FROM sqlq_dead_letter_queue WHERE original_job_id = %d`, j.ID)

		rejected := fmt.Errorf("hook rejected")
		require.ErrorIs(t, d.moveToDeadLetterQueue(ctx, j.ID, j.ClaimToken, "failure", func(*sql.Tx) error {
			return rejected
		}), rejected)
		require.Equal(t, 1, count(stillClaimed), "a rolled-back move must preserve the original job and its claim")
		require.Zero(t, count(`SELECT COUNT(*) FROM sqlq_dead_letter_queue`))

		require.NoError(t, d.moveToDeadLetterQueue(ctx, j.ID, j.ClaimToken, "failure", nil))
		require.Zero(t, count(inJobs), "a committed move must delete the job")
		require.Equal(t, 1, count(inDLQ))
		require.Equal(t, 1, count(`SELECT COUNT(*) FROM sqlq_dead_letter_queue`))
	})

	t.Run("canceled delivery releases jobs that never reached the channel", func(t *testing.T) {
		seed(3)
		require.NoError(t, execSQL(ctx, db, `UPDATE sqlq_jobs SET claim_token = NULL, claim_expires_at = NULL, consumed_at = NULL`))
		cons := testClaimConsumer(ctx, db, d, nil)
		cons.prefetchCount = 3
		cons.jobsChan = make(chan job) // Nobody reads this channel, so the consumer claims jobs and then blocks.
		cons.workerWg.Go(func() { _, _ = cons.fetchJobs(cons.ctx) })
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
		require.NoError(t, execSQL(ctx, db, `UPDATE sqlq_jobs SET claim_token = NULL, claim_expires_at = NULL, consumed_at = NULL`))
		cons := testClaimConsumer(ctx, db, d, nil)
		cons.prefetchCount = 3
		cons.jobsChan = make(chan job, 3)
		fetch := func() {
			_, err := cons.fetchJobs(ctx)
			require.NoError(t, err)
		}
		fetch()
		<-cons.jobsChan // Take one job, like a busy worker. Its claim stays open.
		fetch()
		fetch() // The consumer is full, so this fetch must claim nothing.
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
		require.NoError(t, execSQL(ctx, db, `UPDATE sqlq_jobs SET claim_token = NULL, claim_expires_at = NULL, consumed_at = NULL`))
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
}

func TestClaimLength(t *testing.T) {
	t.Parallel()
	for _, scenario := range []struct {
		jobTimeout, length, minLeftAtStart time.Duration
	}{
		{15 * time.Minute, 15*time.Minute + 10*time.Second, 15*time.Minute + 5*time.Second}, // the overhead stops at 10 seconds
		{10 * time.Second, 15 * time.Second, 12500 * time.Millisecond},                      // the overhead is half the job timeout
		{time.Second, 1500 * time.Millisecond, 1250 * time.Millisecond},                     // a short job timeout
		{minJobTimeout, 75 * time.Millisecond, 62500 * time.Microsecond},                    // the shortest job timeout
	} {
		require.Equal(t, scenario.length, claimLength(scenario.jobTimeout), scenario.jobTimeout)
		require.Equal(t, scenario.minLeftAtStart, minClaimLeftAtStart(scenario.jobTimeout), scenario.jobTimeout)
	}
}

// The job timeout options increase a timeout shorter than 50ms, and ignore a timeout of zero or less.
func TestJobTimeoutOptions(t *testing.T) {
	t.Parallel()
	for _, scenario := range []struct {
		name      string
		queueOpts []NewOption
		consOpts  []ConsumerOption
		want      time.Duration
	}{
		{"default", nil, nil, 2 * time.Second},
		{"short consumer timeout", nil, []ConsumerOption{WithConsumerJobTimeout(100 * time.Millisecond)}, 100 * time.Millisecond},
		{"consumer timeout below the minimum", nil, []ConsumerOption{WithConsumerJobTimeout(time.Millisecond)}, minJobTimeout},
		{"queue timeout below the minimum", []NewOption{WithDefaultJobTimeout(time.Millisecond)}, nil, minJobTimeout},
		{"zero timeout is ignored", nil, []ConsumerOption{WithConsumerJobTimeout(0)}, defaultJobTimeout},
		{"negative timeout is ignored", []NewOption{WithDefaultJobTimeout(-time.Second)}, nil, defaultJobTimeout},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			cons, err := consumeOnTestQueue(t, scenario.queueOpts, scenario.consOpts)
			require.NoError(t, err)
			require.Equal(t, scenario.want, cons.jobTimeout)
		})
	}
}

func TestConsumePrefetchCount(t *testing.T) {
	t.Parallel()
	for _, scenario := range []struct {
		name      string
		queueOpts []NewOption
		consOpts  []ConsumerOption
		want      uint16
	}{
		{"follows consumer concurrency", nil, []ConsumerOption{WithConsumerConcurrency(32)}, 32},
		{"follows queue concurrency", []NewOption{WithDefaultConcurrency(16)}, nil, 16},
		{"explicit higher value is kept", nil, []ConsumerOption{WithConsumerConcurrency(4), WithConsumerPrefetchCount(64)}, 64},
		{"explicit lower value is raised", nil, []ConsumerOption{WithConsumerConcurrency(8), WithConsumerPrefetchCount(2)}, 8},
		{"queue default lower than concurrency is raised", []NewOption{WithDefaultPrefetchCount(2)}, []ConsumerOption{WithConsumerConcurrency(8)}, 8},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			cons, err := consumeOnTestQueue(t, scenario.queueOpts, scenario.consOpts)
			require.NoError(t, err)
			require.Equal(t, scenario.want, cons.prefetchCount)
			require.Equal(t, int(scenario.want), cap(cons.jobsChan))
		})
	}
}

// consumeOnTestQueue starts a consumer on a new SQLite queue and returns it.
func consumeOnTestQueue(t *testing.T, queueOpts []NewOption, consOpts []ConsumerOption) (*consumer, error) {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(t.TempDir(), "consume.db")+"?_busy_timeout=5000&_journal_mode=WAL")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	queue, err := New(db, DBTypeSQLite, queueOpts...)
	require.NoError(t, err)
	q, ok := queue.(*sqlq)
	require.True(t, ok)
	require.NoError(t, q.Run())
	t.Cleanup(q.Shutdown)
	handler := func(context.Context, *sql.Tx, []byte) error { return nil }
	if err = q.Consume(t.Context(), "claims", handler, consOpts...); err != nil {
		return nil, err
	}
	return q.consumersMap["claims"], nil
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
	starts     int
	extensions int
	releases   int
}

func (d *claimDriverSpy) extendClaim(ctx context.Context, j job, timeout time.Duration, startAttempt bool) (time.Time, error) {
	if startAttempt {
		d.starts++
	} else {
		d.extensions++
	}
	return d.driver.extendClaim(ctx, j, timeout, startAttempt)
}

func (d *claimDriverSpy) releaseClaim(ctx context.Context, j job) error {
	d.releases++
	return d.driver.releaseClaim(ctx, j)
}

// counterValue returns the sum of all data points of the named counter.
func counterValue(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok && m.Name == name {
				for _, dp := range sum.DataPoints {
					total += dp.Value
				}
			}
		}
	}
	return total
}

// noopMetrics returns instruments that record nothing, for consumers built without New.
func noopMetrics() *sqlqMetrics {
	m, err := newMetrics(metricnoop.NewMeterProvider().Meter("claims"))
	if err != nil {
		panic(err)
	}
	return m
}

func testClaimConsumer(ctx context.Context, db *sql.DB, d driver, handler func(context.Context, *sql.Tx, []byte) error) *consumer {
	ctx, cancel := context.WithCancel(ctx)
	return &consumer{ //nolint:exhaustruct
		db: db, driver: d, handler: handler, ctx: ctx, cancel: cancel,
		tracer: noop.NewTracerProvider().Tracer("claims"), jobType: "claims",
		metrics:    noopMetrics(),
		jobTimeout: 10 * time.Second,
		claims:     make(map[string]job), concurrency: 1, prefetchCount: 1,
		pollDelayFunc: func(uint16) time.Duration { return time.Millisecond }, jobsChan: make(chan job, 1),
		backoffFunc: func(uint16) time.Duration { return 0 },
	}
}

// TestClaimCrashHelper runs in a child process that the parent test kills.
// This tests what the database does when a worker really dies, not a call to Rollback.
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
