package sqlq_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDriverSQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:memdb1?mode=memory&cache=shared")
	require.NoError(t, err, "Failed to open SQLite database")

	driver := sqlq.NewSQLiteDriver(db)

	err = driver.InitSchema(t.Context())
	require.NoError(t, err)

	payload := []byte{'H', 'i'}
	traceContext := map[string]string{"foo": "bar"}

	t.Run("Insert with delay", func(t *testing.T) {
		delay := 30 * time.Minute
		expectedScheduledAt := time.Now().Add(delay)
		jobType := "insert_with_delay"

		err := driver.InsertJob(t.Context(), jobType, payload, delay, traceContext)
		require.NoError(t, err)

		var job struct {
			ID          int
			JobType     string
			Payload     []byte
			CreatedAt   int64
			ScheduledAt int64
		}

		err = db.QueryRowContext(t.Context(), `
		SELECT id, job_type, payload, created_at, scheduled_at
		FROM jobs WHERE job_type = ? LIMIT 1
	`, jobType).Scan(&job.ID, &job.JobType, &job.Payload, &job.CreatedAt, &job.ScheduledAt)
		require.NoError(t, err)

		require.Equal(t, payload, job.Payload)
		require.Equal(t, jobType, job.JobType)

		// Convert milliseconds to time.Time for comparison
		scheduledAtTime := time.UnixMilli(job.ScheduledAt)
		createdAtTime := time.UnixMilli(job.CreatedAt)

		// Verify scheduled time is approximately correct (within 1 second)
		require.Less(t, scheduledAtTime.Sub(expectedScheduledAt).Abs(), 1*time.Second)

		// Verify created time is recent
		require.Less(t, time.Since(createdAtTime), 5*time.Second)
	})

	t.Run("GetJobsForConsumer", func(t *testing.T) {
		ctx := t.Context()
		jobType := "GetJobsForConsumer"

		err := driver.InsertJob(ctx, jobType, payload, 0, traceContext)
		require.NoError(t, err)

		jobs, err := driver.GetJobsForConsumer(ctx, "some-consumer-name", jobType, 10)
		require.NoError(t, err)

		require.Equal(t, 1, len(jobs))
		j := jobs[0]
		assert.Equal(t, payload, j.Payload)
		assert.Equal(t, jobType, j.JobType)
		assert.Equal(t, traceContext, j.TraceContext)
	})

	t.Run("GetJobsForConsumer - delay", func(t *testing.T) {
		jobType := "GetJobsForConsumer - delay"
		err := driver.InsertJob(t.Context(), jobType, payload, 30*time.Minute, traceContext)
		require.NoError(t, err)

		jobs, err := driver.GetJobsForConsumer(t.Context(), "some-consumer-name", jobType, 10)
		require.NoError(t, err)

		require.Equal(t, 0, len(jobs))
	})

	t.Run("Millisecond time precision", func(t *testing.T) {
		jobType := "epoch_test"
		now := time.Now()
		nowMs := now.UnixMilli()

		err := driver.InsertJob(t.Context(), jobType, payload, 0, traceContext)
		require.NoError(t, err)

		// Directly check the database to verify timestamps are stored as milliseconds
		var createdAt, scheduledAt int64
		err = db.QueryRowContext(t.Context(),
			"SELECT created_at, scheduled_at FROM jobs WHERE job_type = ? LIMIT 1",
			jobType).Scan(&createdAt, &scheduledAt)
		require.NoError(t, err)

		// Verify timestamps are stored as milliseconds since epoch
		// They should be close to current time
		require.Greater(t, createdAt, nowMs-5000) // Within 5 seconds before test
		require.Less(t, createdAt, nowMs+5000)    // Within 5 seconds after test

		// For a job with no delay, scheduled_at should be same as created_at
		require.Equal(t, createdAt, scheduledAt, "For immediate jobs, created_at and scheduled_at should be identical")

		// Verify that SQLite's timestamp function works as expected with millisecond precision
		var dbTimeMs float64 // Changed from int64 to float64 to match SQLite's return type
		err = db.QueryRowContext(t.Context(),
			"SELECT (strftime('%s','now') * 1000 + strftime('%f','now') * 1000 % 1000) as current_time").Scan(&dbTimeMs)
		require.NoError(t, err)

		// Convert to int64 after scanning
		dbTimeMsInt := int64(dbTimeMs)

		// DB time should be close to our time
		require.Less(t, time.UnixMilli(dbTimeMsInt).Sub(now).Abs(), 2*time.Second,
			"Database time and client time should be close")

		// Test millisecond precision by inserting multiple jobs quickly
		// Add a small sleep between insertions to ensure we get different timestamps
		for i := 0; i < 3; i++ {
			err := driver.InsertJob(t.Context(), "precision_test", payload, 0, traceContext)
			require.NoError(t, err)
			// Sleep a tiny amount to ensure different timestamps
			time.Sleep(time.Millisecond)
		}

		// Query the jobs and verify they have different timestamps
		rows, err := db.QueryContext(t.Context(),
			"SELECT created_at FROM jobs WHERE job_type = 'precision_test' ORDER BY created_at")
		require.NoError(t, err)
		// Defer close and check error
		defer func() {
			require.NoError(t, rows.Close(), "Failed to close rows in precision test")
		}()

		var timestamps []int64
		for rows.Next() {
			var ts int64
			require.NoError(t, rows.Scan(&ts))
			timestamps = append(timestamps, ts)
		}

		require.Len(t, timestamps, 3, "Should have 3 precision test jobs")

		// At least some of the timestamps should be different if we have millisecond precision
		// (This is a probabilistic test, but with millisecond precision it's extremely likely to pass)
		uniqueTimestamps := make(map[int64]bool)
		for _, ts := range timestamps {
			uniqueTimestamps[ts] = true
		}
		require.Greater(t, len(uniqueTimestamps), 1,
			"Should have at least 2 different timestamps, indicating millisecond precision")
	})

	t.Run("Concurrent GetJobsForConsumer Race", func(t *testing.T) {
		jobType := "concurrent_race_test"
		consumerName := "race-consumer"
		err := driver.InsertJob(t.Context(), jobType, payload, 0, traceContext)
		require.NoError(t, err, "Failed to insert job for race test")

		// Simulate first consumer fetching the job
		jobs1, err := driver.GetJobsForConsumer(t.Context(), consumerName, jobType, 1)
		require.NoError(t, err, "First GetJobsForConsumer call failed")
		require.Len(t, jobs1, 1, "First GetJobsForConsumer should fetch 1 job")
		jobID1 := jobs1[0].ID

		// Simulate second consumer (or same consumer polling again quickly)
		// *before* the first one marks the job as processed
		jobs2, err := driver.GetJobsForConsumer(t.Context(), consumerName, jobType, 1)
		require.NoError(t, err, "Second GetJobsForConsumer call failed")

		// *** This is the assertion that should FAIL with the current driver logic ***
		// It demonstrates that the job was fetched again before being marked processed.
		require.Len(t, jobs2, 0, "Second GetJobsForConsumer should fetch 0 jobs as it's already fetched by the first")

		// If the above assertion passes (meaning the bug is fixed),
		// we can proceed to mark the first job processed.
		// If the assertion failed (current state), the following lines might
		// not be reached, or jobs2 might contain the same job.

		err = driver.MarkJobProcessed(t.Context(), jobID1, consumerName)
		require.NoError(t, err, "Marking job processed for the first time failed")

		// If jobs2 incorrectly contained the job, attempting to mark it
		// processed again would cause the UNIQUE constraint error.
		// This check is secondary to the len(jobs2) == 0 check above.
		if len(jobs2) > 0 {
			jobID2 := jobs2[0].ID
			require.NotEqual(t, jobID1, jobID2, "If second fetch got a job, it shouldn't be the same ID (this indicates a deeper issue or test setup problem)")
			// Or, if we expect the *same* job ID due to the race:
			// require.Equal(t, jobID1, jobID2, "Second fetch got the same job ID due to race condition")
			// err = driver.MarkJobProcessed(t.Context(), jobID2, consumerName)
			// require.Error(t, err, "Marking the *same* job processed a second time should fail due to UNIQUE constraint")
		}
	})
}
