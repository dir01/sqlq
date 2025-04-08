package sqlq

import (
	"database/sql"
	"time"
)

// MySQLDriver implements the Driver interface for MySQL
type MySQLDriver struct{
	db *sql.DB
}

// NewMySQLDriver creates a new MySQL driver with the given database connection
func NewMySQLDriver(db *sql.DB) *MySQLDriver {
	return &MySQLDriver{db: db}
}

func (d *MySQLDriver) InitSchema() error {
	// MySQL doesn't support multiple statements in a single query
	// so we need to execute them one by one
	queries := []string{
		`CREATE TABLE IF NOT EXISTS jobs (
			id INT AUTO_INCREMENT PRIMARY KEY,
			job_type TEXT NOT NULL,
			payload BLOB,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			scheduled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			retry_count INT DEFAULT 0,
			max_retries INT DEFAULT 0,
			last_error TEXT
		)`,

		`CREATE TABLE IF NOT EXISTS job_consumers (
			job_id INT,
			consumer_name VARCHAR(255),
			processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (job_id, consumer_name),
			FOREIGN KEY (job_id) REFERENCES jobs(id)
		)`,

		`CREATE INDEX idx_jobs_job_type ON jobs(job_type(255))`,

		`CREATE INDEX idx_jobs_scheduled_at ON jobs(scheduled_at)`,
		
		`CREATE TABLE IF NOT EXISTS dead_letter_queue (
			id INT AUTO_INCREMENT PRIMARY KEY,
			original_job_id INT,
			job_type TEXT NOT NULL,
			payload BLOB,
			created_at TIMESTAMP,
			failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			retry_count INT,
			max_retries INT,
			failure_reason TEXT
		)`,
		
		`CREATE INDEX idx_dlq_job_type ON dead_letter_queue(job_type(255))`
	}

	for _, query := range queries {
		_, err := d.db.Exec(query)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *MySQLDriver) InsertJob(jobType string, payload []byte, maxRetries int) error {
	_, err := d.db.Exec("INSERT INTO jobs (job_type, payload, max_retries) VALUES (?, ?, ?)", jobType, payload, maxRetries)
	return err
}

func (d *MySQLDriver) InsertDelayedJob(jobType string, payload []byte, scheduledAt time.Time, maxRetries int) error {
	_, err := d.db.Exec("INSERT INTO jobs (job_type, payload, scheduled_at, max_retries) VALUES (?, ?, ?, ?)", jobType, payload, scheduledAt, maxRetries)
	return err
}

func (d *MySQLDriver) GetJobsForConsumer(consumerName, jobType string, prefetchCount int) ([]job, error) {
	rows, err := d.db.Query(`
		SELECT j.id, j.payload, j.retry_count, j.max_retries 
		FROM jobs j
		LEFT JOIN job_consumers jc ON j.id = jc.job_id AND jc.consumer_name = ?
		WHERE j.job_type = ? 
		AND j.scheduled_at <= NOW()
		AND jc.job_id IS NULL
		ORDER BY j.id
		LIMIT ?
	`, consumerName, jobType, prefetchCount)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []job
	for rows.Next() {
		var j job
		if err := rows.Scan(&j.ID, &j.Payload, &j.RetryCount, &j.MaxRetries); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return jobs, nil
}

func (d *MySQLDriver) MarkJobProcessed(jobID int64, consumerName string) error {
	_, err := d.db.Exec("INSERT INTO job_consumers (job_id, consumer_name) VALUES (?, ?)", jobID, consumerName)
	return err
}

func (d *MySQLDriver) MarkJobFailed(jobID int64, errorMsg string) error {
	_, err := d.db.Exec("UPDATE jobs SET retry_count = retry_count + 1, last_error = ? WHERE id = ?", errorMsg, jobID)
	return err
}

func (d *MySQLDriver) RescheduleJob(jobID int64, scheduledAt time.Time) error {
	_, err := d.db.Exec("UPDATE jobs SET scheduled_at = ? WHERE id = ?", scheduledAt, jobID)
	return err
}

func (d *MySQLDriver) MoveToDeadLetterQueue(jobID int64, reason string) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Get the job details
	var job job
	err = tx.QueryRow(`
		SELECT id, job_type, payload, created_at, retry_count, max_retries
		FROM jobs WHERE id = ?
	`, jobID).Scan(&job.ID, &job.JobType, &job.Payload, &job.CreatedAt, &job.RetryCount, &job.MaxRetries)
	if err != nil {
		if err == sql.ErrNoRows {
			return ErrJobNotFound
		}
		return err
	}

	// Insert into dead letter queue
	_, err = tx.Exec(`
		INSERT INTO dead_letter_queue 
		(original_job_id, job_type, payload, created_at, retry_count, max_retries, failure_reason)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, job.ID, job.JobType, job.Payload, job.CreatedAt, job.RetryCount, job.MaxRetries, reason)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (d *MySQLDriver) GetDeadLetterJobs(jobType string, limit int) ([]DeadLetterJob, error) {
	query := `
		SELECT id, original_job_id, job_type, payload, created_at, failed_at, retry_count, max_retries, failure_reason
		FROM dead_letter_queue
		WHERE job_type = ?
		ORDER BY failed_at DESC
		LIMIT ?
	`
	if jobType == "" {
		query = `
			SELECT id, original_job_id, job_type, payload, created_at, failed_at, retry_count, max_retries, failure_reason
			FROM dead_letter_queue
			ORDER BY failed_at DESC
			LIMIT ?
		`
		return d.queryDeadLetterJobs(query, limit)
	}
	
	return d.queryDeadLetterJobs(query, jobType, limit)
}

func (d *MySQLDriver) queryDeadLetterJobs(query string, args ...interface{}) ([]DeadLetterJob, error) {
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
			&j.MaxRetries, 
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

func (d *MySQLDriver) RequeueDeadLetterJob(dlqID int64) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Get the job from the dead letter queue
	var dlqJob DeadLetterJob
	err = tx.QueryRow(`
		SELECT id, job_type, payload, max_retries
		FROM dead_letter_queue WHERE id = ?
	`, dlqID).Scan(&dlqJob.ID, &dlqJob.JobType, &dlqJob.Payload, &dlqJob.MaxRetries)
	if err != nil {
		if err == sql.ErrNoRows {
			return ErrJobNotFound
		}
		return err
	}

	// Insert back into the main queue with reset retry count
	_, err = tx.Exec(`
		INSERT INTO jobs (job_type, payload, max_retries, retry_count)
		VALUES (?, ?, ?, 0)
	`, dlqJob.JobType, dlqJob.Payload, dlqJob.MaxRetries)
	if err != nil {
		return err
	}

	// Delete from the dead letter queue
	_, err = tx.Exec("DELETE FROM dead_letter_queue WHERE id = ?", dlqID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (d *MySQLDriver) GetCurrentTime() (time.Time, error) {
	var currentTime time.Time
	err := d.db.QueryRow("SELECT NOW()").Scan(&currentTime)
	return currentTime, err
}
