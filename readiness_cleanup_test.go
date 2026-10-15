package sqlq

import (
	"database/sql"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testCleanupEligibility(t *testing.T, f readinessFixture, db *sql.DB, d driver) {
	t.Helper()
	const targetType = "claims"
	const liveClaim = "claimed"
	const (
		recentLabel    = "recent"
		pendingLabel   = "pending"
		expiredClaim   = "expired-claim"
		delayedLabel   = "delayed"
		retryLabel     = "retry"
		otherTypeLabel = "other-type"
	)
	// Controlled historical timestamps avoid sleeping for retention windows.
	old := fmt.Sprint(time.Now().Add(-2 * time.Hour).UnixMilli())
	recent := fmt.Sprint(time.Now().UnixMilli())
	future := fmt.Sprint(time.Now().Add(2 * time.Hour).UnixMilli())
	if f.kind == DBTypePostgres {
		old, recent, future = "clock_timestamp()-interval '2 hours'", "clock_timestamp()", "clock_timestamp()+interval '2 hours'"
	}
	for _, label := range []string{"expired-0", "expired-1", "expired-2", "expired-3", "expired-4", recentLabel, pendingLabel, liveClaim, expiredClaim, delayedLabel, retryLabel, otherTypeLabel} {
		jobType := targetType
		if label == otherTypeLabel {
			jobType = "other"
		}
		require.NoError(t, d.insertJob(t.Context(), nil, jobType, []byte(label), 0, nil))
		state := ", processed_at=" + old
		switch label {
		case recentLabel:
			state = ", processed_at=" + recent
		case pendingLabel:
			state = ""
		case liveClaim:
			state = ", consumed_at=" + recent + ", claim_token='live', claim_expires_at=" + future
		case expiredClaim:
			state = ", consumed_at=" + old + ", claim_token='abandoned', claim_expires_at=" + old
		case delayedLabel:
			state = ", scheduled_at=" + future
		case retryLabel:
			state = ", retry_count=3, scheduled_at=" + future + ", last_error='retryable'"
		}
		require.NoError(t, execSQL(t.Context(), db, `UPDATE jobs SET created_at=`+old+state+` WHERE id=(SELECT max(id) FROM jobs)`))
	}
	deleted, err := d.cleanupJobs(t.Context(), targetType, time.Hour, 2)
	require.NoError(t, err)
	require.Equal(t, int64(5), deleted, "cleanup must continue over multiple small batches")
	keptJobs := []string{liveClaim, delayedLabel, expiredClaim, otherTypeLabel, pendingLabel, recentLabel, retryLabel}
	require.Equal(t, keptJobs, readinessPayloads(t, db, "jobs"))
	deleted, err = d.cleanupJobs(t.Context(), targetType, time.Hour, 2)
	require.NoError(t, err)
	require.Zero(t, deleted, "repeated cleanup must be harmless")
	require.Equal(t, keptJobs, readinessPayloads(t, db, "jobs"))

	for _, label := range []string{"expired-0", "expired-1", "expired-2", "expired-3", "expired-4", recentLabel, otherTypeLabel} {
		jobType := "cleanup-dlq"
		if label == otherTypeLabel {
			jobType = "cleanup-other-dlq"
		}
		require.NoError(t, d.insertJob(t.Context(), nil, jobType, []byte(label), 0, nil))
		jobs, fetchErr := d.getJobsForConsumer(t.Context(), jobType, 1, time.Minute)
		require.NoError(t, fetchErr)
		require.Len(t, jobs, 1)
		j := jobs[0]
		require.NoError(t, d.moveToDeadLetterQueue(t.Context(), j.ID, j.ClaimToken, "terminal", nil))
		failedAt := old
		if label == recentLabel {
			failedAt = recent
		}
		require.NoError(t, execSQL(t.Context(), db, fmt.Sprintf(`UPDATE dead_letter_queue SET created_at=%s, failed_at=%s WHERE original_job_id=%d`, old, failedAt, j.ID)))
	}
	deleted, err = d.cleanupDeadLetterQueueJobs(t.Context(), "cleanup-dlq", time.Hour, 2)
	require.NoError(t, err)
	require.Equal(t, int64(5), deleted)
	require.Equal(t, []string{otherTypeLabel, recentLabel}, readinessPayloads(t, db, "dead_letter_queue"))
	deleted, err = d.cleanupDeadLetterQueueJobs(t.Context(), "cleanup-dlq", time.Hour, 2)
	require.NoError(t, err)
	require.Zero(t, deleted)
	require.Equal(t, []string{otherTypeLabel, recentLabel}, readinessPayloads(t, db, "dead_letter_queue"))
	require.Equal(t, keptJobs, readinessPayloads(t, db, "jobs"), "DLQ cleanup must not affect the main queue")
}

func readinessPayloads(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT payload FROM `+table)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var payloads []string
	for rows.Next() {
		var payload string
		require.NoError(t, rows.Scan(&payload))
		payloads = append(payloads, payload)
	}
	require.NoError(t, rows.Err())
	sort.Strings(payloads)
	return payloads
}
