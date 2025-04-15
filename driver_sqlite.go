package sqlq

import (
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// SQLiteDriver implements the Driver interface for SQLite
type SQLiteDriver struct {
	db    *sql.DB
	mutex sync.Mutex
}

// NewSQLiteDriver creates a new SQLite driver with the given database connection
func NewSQLiteDriver(db *sql.DB) *SQLiteDriver {
	return &SQLiteDriver{db: db}
}

func (d *SQLiteDriver) InitSchema() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	queries := []string{
		`CREATE TABLE IF NOT EXISTS jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_type TEXT NOT NULL,
			payload BLOB,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			scheduled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			retry_count INTEGER DEFAULT 0,
			last_error TEXT
		)`,

		`CREATE INDEX IF NOT EXISTS idx_jobs_job_type ON jobs(job_type)`,

		`CREATE INDEX IF NOT EXISTS idx_jobs_scheduled_at ON jobs(scheduled_at)`,

		`CREATE TABLE IF NOT EXISTS job_consumers (
			job_id INTEGER,
			consumer_name TEXT,
			processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (job_id, consumer_name),
			FOREIGN KEY (job_id) REFERENCES jobs(id)
		)`,

		`CREATE TABLE IF NOT EXISTS dead_letter_queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			original_job_id INTEGER,
			job_type TEXT NOT NULL,
			payload BLOB,
			created_at TIMESTAMP,
			failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			retry_count INTEGER,
			failure_reason TEXT
		)`,

		`CREATE INDEX IF NOT EXISTS idx_dlq_job_type ON dead_letter_queue(job_type)`,
	}

	for _, q := range queries {
		_, err := d.db.Exec(q)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *SQLiteDriver) InsertJob(jobType string, payload []byte, scheduledAt time.Time) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	_, err := d.db.Exec(
		"INSERT INTO jobs (job_type, payload, scheduled_at) VALUES (?, ?, ?)",
		jobType, payload, scheduledAt,
	)
	return err
}

func (d *SQLiteDriver) GetJobsForConsumer(consumerName, jobType string, prefetchCount int) ([]job, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	rows, err := d.db.Query(`
		SELECT j.id, j.payload, j.retry_count
		FROM jobs j
		LEFT JOIN job_consumers jc ON j.id = jc.job_id AND jc.consumer_name = ?
		WHERE j.job_type = ? 
		AND j.scheduled_at <= datetime('now')
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

func (d *SQLiteDriver) MarkJobProcessed(jobID int64, consumerName string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	_, err := d.db.Exec(
		"INSERT INTO job_consumers (job_id, consumer_name) VALUES (?, ?)",
		jobID, consumerName,
	)
	return err
}

func (d *SQLiteDriver) MarkJobFailedAndReschedule(jobID int64, errorMsg string, scheduledAt time.Time) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	_, err := d.db.Exec(
		"UPDATE jobs SET retry_count = retry_count + 1, last_error = ?, scheduled_at = ? WHERE id = ?",
		errorMsg, scheduledAt, jobID,
	)
	return err
}

func (d *SQLiteDriver) MoveToDeadLetterQueue(jobID int64, reason string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Get the job details
	var job job
	err = tx.QueryRow(`
		SELECT id, job_type, payload, created_at, retry_count
		FROM jobs WHERE id = ?
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
		VALUES (?, ?, ?, ?, ?, ?)
	`, job.ID, job.JobType, job.Payload, job.CreatedAt, job.RetryCount, reason)
	if err != nil {
		return err
	}

	// Delete job from regular jobs
	_, err = tx.Exec(`DELETE FROM jobs WHERE id = ?`, job.ID)

	return tx.Commit()
}

func (d *SQLiteDriver) GetDeadLetterJobs(jobType string, limit int) ([]DeadLetterJob, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	query := `
		SELECT id, original_job_id, job_type, payload, created_at, failed_at, retry_count, failure_reason
		FROM dead_letter_queue
		WHERE job_type = ?
		ORDER BY failed_at DESC
		LIMIT ?
	`
	if jobType == "" {
		query = `
			SELECT id, original_job_id, job_type, payload, created_at, failed_at, retry_count, failure_reason
			FROM dead_letter_queue
			ORDER BY failed_at DESC
			LIMIT ?
		`
		return d.queryDeadLetterJobs(query, limit)
	}

	return d.queryDeadLetterJobs(query, jobType, limit)
}

func (d *SQLiteDriver) queryDeadLetterJobs(query string, args ...interface{}) ([]DeadLetterJob, error) {
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

func (d *SQLiteDriver) RequeueDeadLetterJob(dlqID int64) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Get the job from the dead letter queue
	var dlqJob DeadLetterJob
	err = tx.QueryRow(`
		SELECT id, job_type, payload
		FROM dead_letter_queue WHERE id = ?
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
		VALUES (?, ?, 0)
	`, dlqJob.JobType, dlqJob.Payload)
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

func (d *SQLiteDriver) GetCurrentTime() (time.Time, error) {
	var timeStr string
	err := d.db.QueryRow("SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')").Scan(&timeStr)
	if err != nil {
		return time.Time{}, err
	}

	currentTime, err := time.Parse("2006-01-02 15:04:05.999", timeStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse database time: %w", err)
	}

	return currentTime, nil
}
