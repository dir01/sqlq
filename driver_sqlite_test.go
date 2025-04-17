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
		jobType := "GetJobsForConsumer"
		err := driver.InsertJob(t.Context(), jobType, payload, 0, traceContext)
		require.NoError(t, err)

		jobs, err := driver.GetJobsForConsumer(t.Context(), "some-consumer-name", jobType, 10)
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
	
	t.Run("Epoch milliseconds storage", func(t *testing.T) {
		// Insert a job
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
		require.InDelta(t, createdAt, scheduledAt, 100) // Allow small delta for processing time
	})
}
