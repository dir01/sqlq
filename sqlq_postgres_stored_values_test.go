package sqlq_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestPostgresStoredValues checks values that the PostgreSQL driver stores as they come:
// timestamps, whatever the session's time zone, and handler error text, whatever its bytes.
func TestPostgresStoredValues(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("PostgreSQL integration requires Docker or SQLQ_TEST_POSTGRES_DSN")
	}
	dsn := os.Getenv("SQLQ_TEST_POSTGRES_DSN")
	if dsn == "" {
		container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{ //nolint:exhaustruct
			ContainerRequest: testcontainers.ContainerRequest{ //nolint:exhaustruct
				Image: "postgres:14", ExposedPorts: []string{"5432/tcp"},
				Env:        map[string]string{"POSTGRES_PASSWORD": "testpass", "POSTGRES_DB": "stored"},
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
		dsn = fmt.Sprintf("postgres://postgres:testpass@%s:%s/stored?sslmode=disable", host, port.Port())
	}
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })

	// openSchema returns a database in a schema of its own, whose sessions use timeZone.
	openSchema := func(t *testing.T, timeZone string) *sql.DB {
		t.Helper()
		schema := fmt.Sprintf("stored_%d", time.Now().UnixNano())
		_, err := admin.ExecContext(t.Context(), `CREATE SCHEMA `+schema)
		require.NoError(t, err)
		config, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		config.RuntimeParams["search_path"] = schema
		config.RuntimeParams["timezone"] = timeZone
		db := stdlib.OpenDB(*config)
		t.Cleanup(func() {
			assert.NoError(t, db.Close())
			_, err := admin.ExecContext(context.Background(), `DROP SCHEMA `+schema+` CASCADE`)
			assert.NoError(t, err)
		})
		return db
	}
	// newQueue returns a running queue on db.
	newQueue := func(t *testing.T, db *sql.DB, opts ...sqlq.NewOption) sqlq.JobsQueue {
		t.Helper()
		q, err := sqlq.New(db, sqlq.DBTypePostgres, append([]sqlq.NewOption{
			sqlq.WithDefaultPollInterval(10 * time.Millisecond),
			sqlq.WithDefaultBackoffFunc(func(uint16) time.Duration { return 0 }),
		}, opts...)...)
		require.NoError(t, err)
		require.NoError(t, q.Run(t.Context()))
		t.Cleanup(func() { assert.NoError(t, q.Shutdown(context.Background())) }) // Runs before the database closes.
		return q
	}

	// A dead-lettered job is as old as the time since it failed, in whatever time zone the
	// database session and the application are: cleanup with a long retention keeps it.
	for _, timeZone := range []string{"America/Los_Angeles", "UTC", "Asia/Tokyo"} {
		t.Run("DLQ cleanup keeps a fresh job in "+timeZone, func(t *testing.T) {
			t.Parallel()
			q := newQueue(t, openSchema(t, timeZone),
				sqlq.WithDefaultCleanupDLQInterval(20*time.Millisecond),
				sqlq.WithDefaultCleanupDLQAge(time.Hour),
			)
			deadLettered := make(chan struct{})
			require.NoError(t, q.Consume(t.Context(), "fails", func(context.Context, *sql.Tx, []byte) error {
				return errors.New("always fails")
			}, sqlq.WithConsumerMaxRetries(0), sqlq.WithConsumerOnDeadLetter(func(context.Context, *sql.Tx, sqlq.JobInfo, []byte, error) error {
				close(deadLettered)
				return nil
			})))
			require.NoError(t, q.Publish(t.Context(), "fails", TestPayload{Message: "fresh"}))
			select {
			case <-deadLettered:
			case <-time.After(5 * time.Second):
				t.Fatal("job was never dead-lettered")
			}
			// Many cleanup runs, all well within the retention.
			time.Sleep(500 * time.Millisecond)
			jobs, err := q.GetDeadLetterJobs(t.Context(), "fails", 10)
			require.NoError(t, err)
			require.Len(t, jobs, 1, "cleanup deleted a job dead-lettered half a second ago")
			assert.WithinDuration(t, time.Now(), jobs[0].FailedAt, time.Minute, "failure time read back")
			assert.WithinDuration(t, time.Now(), jobs[0].CreatedAt, time.Minute, "creation time read back")
		})
	}

	// The same, with the application in other time zones: the queue reads its clock there.
	for _, timeZone := range []string{"Asia/Tokyo", "America/Los_Angeles"} {
		if os.Getenv("SQLQ_STORED_VALUES_CHILD") != "" {
			break
		}
		t.Run("DLQ cleanup with the application in "+timeZone, func(t *testing.T) {
			t.Parallel()
			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestPostgresStoredValues$/^DLQ_cleanup_keeps", "-test.count=1")
			cmd.Env = append(os.Environ(), "TZ="+timeZone, "SQLQ_TEST_POSTGRES_DSN="+dsn, "SQLQ_STORED_VALUES_CHILD=1")
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "with TZ=%s:\n%s", timeZone, out)
		})
	}

	// PostgreSQL text rejects NUL bytes and invalid UTF-8. A handler's error must still be
	// recorded, or its retries are never counted and the job runs again and again.
	for name, message := range map[string]string{"NUL": "bad input: \x00", "invalid UTF-8": "bad input: \xff"} {
		t.Run("error text with "+name+" counts as a retry", func(t *testing.T) {
			t.Parallel()
			q := newQueue(t, openSchema(t, "UTC"))
			var calls atomic.Int32
			require.NoError(t, q.Consume(t.Context(), "bad_text", func(context.Context, *sql.Tx, []byte) error {
				calls.Add(1)
				return errors.New(message)
			}, sqlq.WithConsumerMaxRetries(1)))
			require.NoError(t, q.Publish(t.Context(), "bad_text", TestPayload{Message: "once"}))
			var jobs []sqlq.DeadLetterJob
			if !assert.Eventually(t, func() bool {
				var err error
				jobs, err = q.GetDeadLetterJobs(t.Context(), "bad_text", 10)
				return err == nil && len(jobs) == 1
			}, 5*time.Second, 20*time.Millisecond) {
				t.Fatalf("job never reached the DLQ, after %d calls", calls.Load())
			}
			// Polls that could run it again, had it stayed in the queue.
			time.Sleep(200 * time.Millisecond)
			assert.Equal(t, int32(2), calls.Load(), "one attempt and one retry")
			assert.Equal(t, uint32(1), jobs[0].RetryCount)
			assert.True(t, strings.HasPrefix(jobs[0].FailureReason, "bad input: "), "failure reason %q", jobs[0].FailureReason)
		})
	}
}
