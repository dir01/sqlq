package sqlqueue

import (
	"database/sql"
	"time"
)

// MySQLDriver implements the Driver interface for MySQL
type MySQLDriver struct{}

func (d *MySQLDriver) InitSchema(db *sql.DB) error {
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

		`CREATE INDEX idx_jobs_scheduled_at ON jobs(scheduled_at)`}

	for _, query := range queries {
		_, err := db.Exec(query)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *MySQLDriver) InsertJob(db *sql.DB, jobType string, payload []byte, maxRetries int) error {
	_, err := db.Exec("INSERT INTO jobs (job_type, payload, max_retries) VALUES (?, ?, ?)", jobType, payload, maxRetries)
	return err
}

func (d *MySQLDriver) InsertDelayedJob(db *sql.DB, jobType string, payload []byte, scheduledAt time.Time, maxRetries int) error {
	_, err := db.Exec("INSERT INTO jobs (job_type, payload, scheduled_at, max_retries) VALUES (?, ?, ?, ?)", jobType, payload, scheduledAt, maxRetries)
	return err
}

func (d *MySQLDriver) GetJobsForConsumer(db *sql.DB, consumerName, jobType string) (*sql.Rows, error) {
	return db.Query(`
		SELECT j.id, j.payload, j.retry_count, j.max_retries 
		FROM jobs j
		LEFT JOIN job_consumers jc ON j.id = jc.job_id AND jc.consumer_name = ?
		WHERE j.job_type = ? 
		AND j.scheduled_at <= NOW()
		AND jc.job_id IS NULL
		ORDER BY j.id
		LIMIT 10
	`, consumerName, jobType)
}

func (d *MySQLDriver) MarkJobProcessed(db *sql.DB, jobID int64, consumerName string) error {
	_, err := db.Exec("INSERT INTO job_consumers (job_id, consumer_name) VALUES (?, ?)", jobID, consumerName)
	return err
}

func (d *MySQLDriver) MarkJobFailed(db *sql.DB, jobID int64, errorMsg string) error {
	_, err := db.Exec("UPDATE jobs SET retry_count = retry_count + 1, last_error = ? WHERE id = ?", errorMsg, jobID)
	return err
}

func (d *MySQLDriver) RescheduleJob(db *sql.DB, jobID int64, scheduledAt time.Time) error {
	_, err := db.Exec("UPDATE jobs SET scheduled_at = ? WHERE id = ?", scheduledAt, jobID)
	return err
}

func (d *MySQLDriver) GetCurrentTime(db *sql.DB) (time.Time, error) {
	var currentTime time.Time
	err := db.QueryRow("SELECT NOW()").Scan(&currentTime)
	return currentTime, err
}
