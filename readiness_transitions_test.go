package sqlq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testTransitionAmbiguity(t *testing.T, f readinessFixture, observer *sql.DB, reset func(*testing.T)) {
	t.Helper()
	const deadLetterMode = "dead-letter"
	const retryMode = "retry"
	for _, operation := range []string{"claim", retryMode, deadLetterMode, "requeue"} {
		for _, fault := range []int32{1, 2} {
			t.Run(fmt.Sprintf("%s/fault=%d", operation, fault), func(t *testing.T) {
				reset(t)
				db, d, arm, fired := readinessBoundaryDB(t, f)
				q, err := New(db, f.kind)
				require.NoError(t, err)
				defer q.Shutdown()
				require.NoError(t, q.Publish(t.Context(), "claims", "task"))
				switch operation {
				case "claim":
					testAmbiguousClaim(t, f, observer, db, d, arm, fault)
				case retryMode, deadLetterMode:
					testAmbiguousFailure(t, observer, db, d, q, arm, fault, operation == deadLetterMode)
				case "requeue":
					testAmbiguousRequeue(t, observer, db, d, q, arm, fault)
				}
				require.Equal(t, fault, fired(), "missing confirmed transaction-boundary fault")
			})
		}
	}
}

func claimReadinessJob(t *testing.T, d driver) job {
	t.Helper()
	jobs, err := d.getJobsForConsumer(t.Context(), "claims", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	return jobs[0]
}

func completeReadinessJob(t *testing.T, db *sql.DB, d driver, j job) {
	t.Helper()
	cons := testClaimConsumer(t.Context(), db, d, func(ctx context.Context, tx *sql.Tx, _ []byte) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO readiness_effects VALUES ('completed')`)
		return err
	})
	defer cons.cancel()
	cons.processJob(&j)
	require.Equal(t, 1, countReadiness(t, db, `SELECT count(*) FROM jobs WHERE job_type='claims' AND processed_at IS NOT NULL`))
	require.Equal(t, 1, countReadiness(t, db, `SELECT count(*) FROM readiness_effects WHERE value='completed'`))
}

func testAmbiguousClaim(t *testing.T, f readinessFixture, observer, db *sql.DB, d driver, arm func(int32), fault int32) {
	t.Helper()
	arm(fault)
	jobs, err := d.getJobsForConsumer(t.Context(), "claims", 1, time.Minute)
	require.Error(t, err)
	require.Empty(t, jobs, "a failed claim response must not deliver speculative jobs")
	var oldToken string
	if fault == 2 {
		require.NoError(t, observer.QueryRowContext(t.Context(), `SELECT claim_token FROM jobs`).Scan(&oldToken))
		require.NotEmpty(t, oldToken)
		jobs, err = d.getJobsForConsumer(t.Context(), "claims", 1, time.Minute)
		require.NoError(t, err)
		require.Empty(t, jobs, "an acknowledged-lost claim must remain exclusively held until expiry")
		// Natural expiry is covered by the SIGSTOP test. Fast-forward only the
		// database fixture here to cheaply test the lost-claim-response branch.
		expire := `UPDATE jobs SET claim_expires_at=0`
		if f.kind == DBTypePostgres {
			expire = `UPDATE jobs SET claim_expires_at=clock_timestamp()-interval '1 second'`
		}
		require.NoError(t, execSQL(t.Context(), observer, expire))
	} else {
		require.Equal(t, 1, countReadiness(t, observer, `SELECT count(*) FROM jobs WHERE claim_token IS NULL AND consumed_at IS NULL`))
	}
	j := claimReadinessJob(t, d)
	require.NotEqual(t, oldToken, j.ClaimToken)
	require.Zero(t, j.RetryCount, "a lost claim response is not a failed handler attempt")
	completeReadinessJob(t, db, d, j)
}

func testAmbiguousFailure(t *testing.T, observer, db *sql.DB, d driver, q JobsQueue, arm func(int32), fault int32, terminal bool) {
	t.Helper()
	attempts := 0
	cons := testClaimConsumer(t.Context(), db, d, func(ctx context.Context, tx *sql.Tx, _ []byte) error {
		attempts++
		if attempts > 1 && !terminal {
			_, err := tx.ExecContext(ctx, `INSERT INTO readiness_effects VALUES ('completed')`)
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO readiness_effects VALUES ('failed-attempt')`); err != nil {
			return err
		}
		if attempts == 1 {
			// The failed attempt rolls back. Its retry/DLQ settlement is the
			// next COMMIT, so this fault targets settlement, not the handler.
			arm(fault)
		}
		return errors.New("deliberate handler failure")
	})
	defer cons.cancel()
	cons.maxRetries = 2
	if terminal {
		cons.maxRetries = 0
	}
	cons.onDeadLetter = func(ctx context.Context, tx *sql.Tx, _ JobInfo, _ []byte, _ error) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO readiness_effects VALUES ('terminal-hook')`); err != nil {
			return err
		}
		return q.PublishTx(ctx, tx, "terminal-child", "terminal-child")
	}
	j := claimReadinessJob(t, d)
	cons.processJob(&j)
	require.Zero(t, countReadiness(t, observer, `SELECT count(*) FROM readiness_effects WHERE value='failed-attempt'`))
	if terminal {
		if fault == 1 {
			// The DLQ transaction rolled back; the consumer reschedules the job.
			require.Zero(t, countReadiness(t, observer, `SELECT count(*) FROM dead_letter_queue`))
			require.Zero(t, countReadiness(t, observer, `SELECT count(*) FROM readiness_effects`))
			require.Zero(t, countReadiness(t, observer, `SELECT count(*) FROM jobs WHERE job_type='terminal-child'`))
			next := claimReadinessJob(t, d)
			require.Equal(t, uint32(1), next.RetryCount)
			cons.processJob(&next)
		}
		require.Equal(t, 1, countReadiness(t, observer, `SELECT count(*) FROM dead_letter_queue`))
		require.Equal(t, 1, countReadiness(t, observer, `SELECT count(*) FROM readiness_effects WHERE value='terminal-hook'`))
		require.Equal(t, 1, countReadiness(t, observer, `SELECT count(*) FROM jobs WHERE job_type='terminal-child'`))
		require.Zero(t, countReadiness(t, observer, `SELECT count(*) FROM jobs WHERE job_type='claims'`))
	} else {
		require.Zero(t, countReadiness(t, observer, `SELECT count(*) FROM readiness_effects`))
		next := claimReadinessJob(t, d)
		wantRetries := uint32(0)
		if fault == 2 {
			wantRetries = 1
		}
		require.Equal(t, wantRetries, next.RetryCount, "only committed retry settlements advance the count")
		// Repeating settlement with the old token must not count a second failure
		// or interfere with a freshly acquired claim.
		require.ErrorIs(t, d.markJobFailedAndReschedule(t.Context(), j.ID, j.ClaimToken, "stale", 0), ErrClaimLost)
		cons.processJob(&next)
		require.Equal(t, 1, countReadiness(t, observer, `SELECT count(*) FROM readiness_effects WHERE value='completed'`))
		require.Equal(t, 1, countReadiness(t, observer, `SELECT count(*) FROM jobs WHERE processed_at IS NOT NULL`))
		require.Zero(t, countReadiness(t, observer, `SELECT count(*) FROM dead_letter_queue`))
	}
}

func testAmbiguousRequeue(t *testing.T, observer, db *sql.DB, d driver, q JobsQueue, arm func(int32), fault int32) {
	t.Helper()
	j := claimReadinessJob(t, d)
	require.NoError(t, d.moveToDeadLetterQueue(t.Context(), j.ID, j.ClaimToken, "terminal", nil))
	arm(fault)
	require.Error(t, q.RequeueDeadLetterJob(t.Context(), j.ID))
	if fault == 1 {
		require.Equal(t, 1, countReadiness(t, observer, `SELECT count(*) FROM dead_letter_queue`))
		require.Zero(t, countReadiness(t, observer, `SELECT count(*) FROM jobs`))
		require.NoError(t, q.RequeueDeadLetterJob(t.Context(), j.ID))
	} else {
		require.ErrorIs(t, q.RequeueDeadLetterJob(t.Context(), j.ID), ErrJobNotFound)
	}
	require.Zero(t, countReadiness(t, observer, `SELECT count(*) FROM dead_letter_queue`))
	require.Equal(t, 1, countReadiness(t, observer, `SELECT count(*) FROM jobs`))
	replacement := claimReadinessJob(t, d)
	require.NotEqual(t, j.ID, replacement.ID)
	require.Equal(t, j.Payload, replacement.Payload)
	require.Zero(t, replacement.RetryCount)
	completeReadinessJob(t, db, d, replacement)
}

func testReadinessPublishing(t *testing.T, f readinessFixture, observer *sql.DB, reset func(*testing.T)) {
	t.Helper()
	for _, outcome := range []string{"rollback", "commit", "lost-before-commit", "lost-after-commit"} {
		t.Run(outcome, func(t *testing.T) {
			reset(t)
			db, _, arm, fired := readinessBoundaryDB(t, f)
			q, err := New(db, f.kind)
			require.NoError(t, err)
			defer q.Shutdown()
			tx, err := db.BeginTx(t.Context(), nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			_, err = tx.ExecContext(t.Context(), `INSERT INTO readiness_effects VALUES ('business-write')`)
			require.NoError(t, err)
			require.NoError(t, q.PublishTx(t.Context(), tx, "claims", "first"))
			require.NoError(t, q.PublishTx(t.Context(), tx, "claims", "second"))
			// PublishTx success is not a commit. An independent connection must
			// see neither the application's writes nor either queue publication.
			require.Zero(t, countReadiness(t, observer, `SELECT count(*) FROM readiness_effects`))
			require.Zero(t, countReadiness(t, observer, `SELECT count(*) FROM jobs`))
			committed := false
			switch outcome {
			case "rollback":
				require.NoError(t, tx.Rollback())
			case "commit":
				require.NoError(t, tx.Commit())
				committed = true
			case "lost-before-commit":
				arm(1)
				require.Error(t, tx.Commit())
				require.Equal(t, int32(1), fired())
			case "lost-after-commit":
				arm(2)
				require.Error(t, tx.Commit())
				require.Equal(t, int32(2), fired())
				committed = true
			}
			wantWrites, wantJobs := 0, 0
			if committed {
				wantWrites, wantJobs = 1, 2
			}
			require.Equal(t, wantWrites, countReadiness(t, observer, `SELECT count(*) FROM readiness_effects`))
			require.Equal(t, wantJobs, countReadiness(t, observer, `SELECT count(*) FROM jobs`))
			require.Zero(t, countReadiness(t, observer, `SELECT count(*) FROM dead_letter_queue`))
		})
	}
}
