package sqlqueue

import (
	"database/sql"
	"time"
)

// PostgresDriver implements the Driver interface for PostgreSQL
type PostgresDriver struct{}

func (d *PostgresDriver) InitSchema(db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS jobs (
			id SERIAL PRIMARY KEY,
			job_type TEXT NOT NULL,
			payload BYTEA,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			scheduled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			retry_count INTEGER DEFAULT 0,
			max_retries INTEGER DEFAULT 0,
			last_error TEXT
		);
		
		CREATE TABLE IF NOT EXISTS job_consumers (
			job_id INTEGER,
			consumer_name TEXT,
			processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (job_id, consumer_name),
			FOREIGN KEY (job_id) REFERENCES jobs(id)
		);
		
		CREATE INDEX IF NOT EXISTS idx_jobs_job_type ON jobs(job_type);
		CREATE INDEX IF NOT EXISTS idx_jobs_scheduled_at ON jobs(scheduled_at);
	`
	_, err := db.Exec(query)
	return err
}

func (d *PostgresDriver) InsertJob(db *sql.DB, jobType string, payload []byte, maxRetries int) error {
	_, err := db.Exec("INSERT INTO jobs (job_type, payload, max_retries) VALUES ($1, $2, $3)", jobType, payload, maxRetries)
	return err
}

func (d *PostgresDriver) InsertDelayedJob(db *sql.DB, jobType string, payload []byte, scheduledAt time.Time, maxRetries int) error {
	_, err := db.Exec("INSERT INTO jobs (job_type, payload, scheduled_at, max_retries) VALUES ($1, $2, $3, $4)", jobType, payload, scheduledAt, maxRetries)
	return err
}

func (d *PostgresDriver) GetJobsForConsumer(db *sql.DB, consumerName, jobType string, prefetchCount int) ([]job, error) {
	rows, err := db.Query(`
		SELECT j.id, j.payload, j.retry_count, j.max_retries 
		FROM jobs j
		LEFT JOIN job_consumers jc ON j.id = jc.job_id AND jc.consumer_name = $1
		WHERE j.job_type = $2 
		AND j.scheduled_at <= NOW()
		AND jc.job_id IS NULL
		ORDER BY j.id
		LIMIT 10
		LIMIT $3
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

func (d *PostgresDriver) MarkJobProcessed(db *sql.DB, jobID int64, consumerName string) error {
	_, err := db.Exec("INSERT INTO job_consumers (job_id, consumer_name) VALUES ($1, $2)", jobID, consumerName)
	return err
}

func (d *PostgresDriver) MarkJobFailed(db *sql.DB, jobID int64, errorMsg string) error {
	_, err := db.Exec("UPDATE jobs SET retry_count = retry_count + 1, last_error = $1 WHERE id = $2", errorMsg, jobID)
	return err
}

func (d *PostgresDriver) RescheduleJob(db *sql.DB, jobID int64, scheduledAt time.Time) error {
	_, err := db.Exec("UPDATE jobs SET scheduled_at = $1 WHERE id = $2", scheduledAt, jobID)
	return err
}

func (d *PostgresDriver) GetCurrentTime(db *sql.DB) (time.Time, error) {
	var currentTime time.Time
	err := db.QueryRow("SELECT NOW()").Scan(&currentTime)
	return currentTime, err
}
