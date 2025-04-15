package sqlq

import (
	"database/sql"
	"time"
)

// PostgresDriver implements the Driver interface for PostgreSQL
type PostgresDriver struct {
	db *sql.DB
}

// NewPostgresDriver creates a new PostgreSQL driver with the given database connection
func NewPostgresDriver(db *sql.DB) *PostgresDriver {
	return &PostgresDriver{db: db}
}

func (d *PostgresDriver) InitSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS jobs (
			id SERIAL PRIMARY KEY,
			job_type TEXT NOT NULL,
			payload BYTEA,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			scheduled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			retry_count INTEGER DEFAULT 0,
			last_error TEXT
		)`,

		`CREATE TABLE IF NOT EXISTS job_consumers (
			job_id INTEGER,
			consumer_name TEXT,
			processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (job_id, consumer_name),
			FOREIGN KEY (job_id) REFERENCES jobs(id)
		)`,

		`CREATE INDEX IF NOT EXISTS idx_jobs_job_type ON jobs(job_type)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_scheduled_at ON jobs(scheduled_at)`,

		`CREATE TABLE IF NOT EXISTS dead_letter_queue (
			id SERIAL PRIMARY KEY,
			original_job_id INTEGER,
			job_type TEXT NOT NULL,
			payload BYTEA,
			created_at TIMESTAMP,
			failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			retry_count INTEGER,
			failure_reason TEXT
		)`,

		`CREATE INDEX IF NOT EXISTS idx_dlq_job_type ON dead_letter_queue(job_type)`,
	}

	for _, query := range queries {
		_, err := d.db.Exec(query)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *PostgresDriver) InsertJob(jobType string, payload []byte, scheduledAt time.Time) error {
	var query string
	var args []interface{}
	
	if scheduledAt.IsZero() {
		// Use database's current time
		query = "INSERT INTO jobs (job_type, payload) VALUES ($1, $2)"
		args = []interface{}{jobType, payload}
	} else {
		// Use the provided scheduled time
		query = "INSERT INTO jobs (job_type, payload, scheduled_at) VALUES ($1, $2, $3)"
		args = []interface{}{jobType, payload, scheduledAt}
	}
	
	_, err := d.db.Exec(query, args...)
	return err
}

func (d *PostgresDriver) GetJobsForConsumer(consumerName, jobType string, prefetchCount int) ([]job, error) {
	rows, err := d.db.Query(`
		SELECT j.id, j.payload, j.retry_count
		FROM jobs j
		LEFT JOIN job_consumers jc ON j.id = jc.job_id AND jc.consumer_name = $1
		WHERE j.job_type = $2 
		AND j.scheduled_at <= NOW()
		AND jc.job_id IS NULL
		ORDER BY j.id
		LIMIT $3
	`, consumerName, jobType, prefetchCount)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []job
	for rows.Next() {
		var j job
		if err := rows.Scan(&j.ID, &j.Payload, &j.RetryCount); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return jobs, nil
}

func (d *PostgresDriver) MarkJobProcessed(jobID int64, consumerName string) error {
	_, err := d.db.Exec("INSERT INTO job_consumers (job_id, consumer_name) VALUES ($1, $2)", jobID, consumerName)
	return err
}

func (d *PostgresDriver) MarkJobFailedAndReschedule(jobID int64, errorMsg string, backoffDuration time.Duration) error {
	_, err := d.db.Exec(
		"UPDATE jobs SET retry_count = retry_count + 1, last_error = $1, scheduled_at = NOW() + $2 WHERE id = $3",
		errorMsg, backoffDuration.String(), jobID,
	)
	return err
}

func (d *PostgresDriver) MoveToDeadLetterQueue(jobID int64, reason string) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Get the job details
	var job job
	err = tx.QueryRow(`
		SELECT id, job_type, payload, created_at, retry_count
		FROM jobs WHERE id = $1
	`, jobID).Scan(&job.ID, &job.JobType, &job.Payload, &job.CreatedAt, &job.RetryCount)
	if err != nil {
		if err == sql.ErrNoRows {
			return ErrJobNotFound
		}
		return err
	}

	// Insert into dead letter queue
	_, err = tx.Exec(`
		INSERT INTO dead_letter_queue 
		(original_job_id, job_type, payload, created_at, retry_count, failure_reason)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, job.ID, job.JobType, job.Payload, job.CreatedAt, job.RetryCount, reason)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (d *PostgresDriver) GetDeadLetterJobs(jobType string, limit int) ([]DeadLetterJob, error) {
	query := `
		SELECT id, original_job_id, job_type, payload, created_at, failed_at, retry_count, failure_reason
		FROM dead_letter_queue
		WHERE job_type = $1
		ORDER BY failed_at DESC
		LIMIT $2
	`
	if jobType == "" {
		query = `
			SELECT id, original_job_id, job_type, payload, created_at, failed_at, retry_count, failure_reason
			FROM dead_letter_queue
			ORDER BY failed_at DESC
			LIMIT $1
		`
		return d.queryDeadLetterJobs(query, limit)
	}

	return d.queryDeadLetterJobs(query, jobType, limit)
}

func (d *PostgresDriver) queryDeadLetterJobs(query string, args ...interface{}) ([]DeadLetterJob, error) {
	rows, err := d.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []DeadLetterJob
	for rows.Next() {
		var j DeadLetterJob
		if err := rows.Scan(
			&j.ID,
			&j.OriginalID,
			&j.JobType,
			&j.Payload,
			&j.CreatedAt,
			&j.FailedAt,
			&j.RetryCount,
			&j.FailureReason,
		); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return jobs, nil
}

func (d *PostgresDriver) RequeueDeadLetterJob(dlqID int64) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Get the job from the dead letter queue
	var dlqJob DeadLetterJob
	err = tx.QueryRow(`
		SELECT id, job_type, payload
		FROM dead_letter_queue WHERE id = $1
	`, dlqID).Scan(&dlqJob.ID, &dlqJob.JobType, &dlqJob.Payload)
	if err != nil {
		if err == sql.ErrNoRows {
			return ErrJobNotFound
		}
		return err
	}

	// Insert back into the main queue with reset retry count
	_, err = tx.Exec(`
		INSERT INTO jobs (job_type, payload, retry_count)
		VALUES ($1, $2, 0)
	`, dlqJob.JobType, dlqJob.Payload)
	if err != nil {
		return err
	}

	// Delete from the dead letter queue
	_, err = tx.Exec("DELETE FROM dead_letter_queue WHERE id = $1", dlqID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

