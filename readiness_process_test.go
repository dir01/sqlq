package sqlq

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type readinessProcessConfig struct {
	Kind   DBType `json:"kind"`
	DSN    string `json:"dsn"`
	Schema string `json:"schema"`
}

// Stop the process in BeginTx, after the consumer examines its claim, as a wait for a pool
// connection does. This is a real gap in the schedule. Different from a stop of a SQLite writer,
// it lets a different consumer claim the job on the two databases. The claim expires during the stop.
// Thus, the worker must not run the handler when it continues. If it does, the handler has external
// effects, but a different consumer has the job. The claim token still blocks its database
// writes (see claims_test.go).
func TestReadinessWorkerProcess(t *testing.T) { //nolint:paralleltest // The parent process controls when this process stops and continues.
	config := os.Getenv("SQLQ_READINESS_WORKER")
	if config == "" {
		t.Skip("subprocess helper")
	}
	var c readinessProcessConfig
	require.NoError(t, json.Unmarshal([]byte(config), &c))
	f := readinessFixture{kind: c.Kind, dsn: c.DSN, schema: c.Schema}
	control := &boundaryConnector{Connector: f.connector(t)} //nolint:exhaustruct // At the start, all faults are off.
	db := sql.OpenDB(control)
	defer func() { require.NoError(t, db.Close()) }()
	d, err := getDriver(db, f.kind)
	require.NoError(t, err)
	jobs, err := d.getJobsForConsumer(t.Context(), "claims", 1, 15*time.Second)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	var once atomic.Bool
	control.beforeBegin = func() {
		if once.Swap(true) {
			return
		}
		fmt.Println("claimed-before-begin")
		var release [1]byte
		_, err := io.ReadFull(os.Stdin, release[:])
		require.NoError(t, err)
	}
	var called atomic.Bool
	cons := testClaimConsumer(t.Context(), db, d, func(ctx context.Context, tx *sql.Tx, _ []byte) error {
		called.Store(true)
		_, err := tx.ExecContext(ctx, `INSERT INTO readiness_effects VALUES ('stale')`)
		return err
	})
	defer cons.cancel()
	cons.jobTimeout = 5 * time.Second
	cons.claimTimeout = 15 * time.Second
	cons.processJob(&jobs[0])
	require.True(t, once.Load())
	require.False(t, called.Load(), "the handler ran after the claim expired while its transaction began")
	fmt.Println("stale-attempt-finished")
}

func testStaleResumption(t *testing.T, f readinessFixture, db *sql.DB, d driver) {
	t.Helper()
	require.NoError(t, d.insertJob(t.Context(), nil, "claims", []byte(`{}`), 0, nil))
	config, err := json.Marshal(readinessProcessConfig{Kind: f.kind, DSN: f.dsn, Schema: f.schema})
	require.NoError(t, err)
	cmd := exec.Command(os.Args[0], "-test.run=^TestReadinessWorkerProcess$", "-test.timeout=2m")
	cmd.Env = append(os.Environ(), "SQLQ_READINESS_WORKER="+string(config), "GORACE=halt_on_error=1 atexit_sleep_ms=0")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.StdoutPipe()
	require.NoError(t, err)
	in, err := cmd.StdinPipe()
	require.NoError(t, err)
	defer func() { _ = in.Close() }()
	require.NoError(t, cmd.Start())
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	scanner := bufio.NewScanner(out)
	ready := make(chan bool, 1)
	go func() { ready <- scanner.Scan() && scanner.Text() == "claimed-before-begin" }()
	select {
	case ok := <-ready:
		require.True(t, ok, "worker failed to reach pre-transaction barrier")
	case <-time.After(30 * time.Second):
		t.Fatal("worker did not reach pre-transaction barrier")
	}
	var oldToken string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT claim_token FROM jobs WHERE job_type='claims'`).Scan(&oldToken))
	require.NotEmpty(t, oldToken)
	require.NoError(t, cmd.Process.Signal(syscall.SIGSTOP))
	var status syscall.WaitStatus
	_, err = syscall.Wait4(cmd.Process.Pid, &status, syscall.WUNTRACED, nil)
	require.NoError(t, err)
	require.True(t, status.Stopped(), "must observe an actual process pause")
	expired := `SELECT count(*) FROM jobs WHERE claim_expires_at <= ` + sqliteNow
	if f.kind == DBTypePostgres {
		expired = `SELECT count(*) FROM jobs WHERE claim_expires_at <= clock_timestamp()`
	}
	awaitReadiness(t, "paused worker's claim naturally expired", func() bool { return countReadiness(t, db, expired) == 1 })
	jobs, err := d.getJobsForConsumer(t.Context(), "claims", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.NotEqual(t, oldToken, jobs[0].ClaimToken)
	// B has the job, but has not completed it. If B completes it, processed_at alone
	// stops A, and the test cannot find a missing claim-token check.
	require.NoError(t, cmd.Process.Signal(syscall.SIGCONT))
	_, err = in.Write([]byte{1})
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		// Read all of stdout before Wait closes the pipe. Thus, the test keeps the last marker.
		finished := false
		for scanner.Scan() {
			finished = finished || scanner.Text() == "stale-attempt-finished"
		}
		err := cmd.Wait()
		if err == nil && !finished {
			err = fmt.Errorf("worker never finished stale attempt")
		}
		done <- err
	}()
	select {
	case err := <-done:
		waited = true
		require.NoError(t, err, stderr.String())
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		if stopErr := <-done; stopErr != nil {
			t.Logf("timed-out worker exit: %v", stopErr)
		}
		waited = true
		t.Fatal("resumed worker did not finish")
	}
	require.Zero(t, countReadiness(t, db, `SELECT count(*) FROM readiness_effects`))
	var currentToken string
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT claim_token FROM jobs WHERE processed_at IS NULL`).Scan(&currentToken))
	require.Equal(t, jobs[0].ClaimToken, currentToken, "stale settlement/release must preserve B's ownership")
	cons := testClaimConsumer(t.Context(), db, d, func(ctx context.Context, tx *sql.Tx, _ []byte) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO readiness_effects VALUES ('replacement')`)
		return err
	})
	defer cons.cancel()
	cons.processJob(&jobs[0])
	require.Equal(t, 1, countReadiness(t, db, `SELECT count(*) FROM readiness_effects WHERE value='replacement'`))
	require.Zero(t, countReadiness(t, db, `SELECT count(*) FROM readiness_effects WHERE value='stale'`))
	require.Zero(t, countReadiness(t, db, `SELECT count(*) FROM dead_letter_queue`))
	require.Equal(t, 1, countReadiness(t, db, `SELECT count(*) FROM jobs WHERE processed_at IS NOT NULL AND retry_count=0`))
}
