package sqlqueue

import (
	"database/sql"
	"fmt"
	"time"
)

// SQLiteDriver implements the Driver interface for SQLite
type SQLiteDriver struct{}

func (d *SQLiteDriver) InitSchema(db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_type TEXT NOT NULL,
			payload BLOB,
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

func (d *SQLiteDriver) InsertJob(db *sql.DB, jobType string, payload []byte, maxRetries int) error {
	_, err := db.Exec("INSERT INTO jobs (job_type, payload, max_retries) VALUES (?, ?, ?)", jobType, payload, maxRetries)
	return err
}

func (d *SQLiteDriver) InsertDelayedJob(db *sql.DB, jobType string, payload []byte, scheduledAt time.Time, maxRetries int) error {
	_, err := db.Exec("INSERT INTO jobs (job_type, payload, scheduled_at, max_retries) VALUES (?, ?, ?, ?)", jobType, payload, scheduledAt, maxRetries)
	return err
}

func (d *SQLiteDriver) GetJobsForConsumer(db *sql.DB, consumerName, jobType string) (*sql.Rows, error) {
	return db.Query(`
		SELECT j.id, j.payload, j.retry_count, j.max_retries 
		FROM jobs j
		LEFT JOIN job_consumers jc ON j.id = jc.job_id AND jc.consumer_name = ?
		WHERE j.job_type = ? 
		AND j.scheduled_at <= datetime('now')
		AND jc.job_id IS NULL
		ORDER BY j.id
		LIMIT 10
	`, consumerName, jobType)
}

func (d *SQLiteDriver) MarkJobProcessed(db *sql.DB, jobID int64, consumerName string) error {
	_, err := db.Exec("INSERT INTO job_consumers (job_id, consumer_name) VALUES (?, ?)", jobID, consumerName)
	return err
}

func (d *SQLiteDriver) MarkJobFailed(db *sql.DB, jobID int64, errorMsg string) error {
	_, err := db.Exec("UPDATE jobs SET retry_count = retry_count + 1, last_error = ? WHERE id = ?", errorMsg, jobID)
	return err
}

func (d *SQLiteDriver) RescheduleJob(db *sql.DB, jobID int64, scheduledAt time.Time) error {
	_, err := db.Exec("UPDATE jobs SET scheduled_at = ? WHERE id = ?", scheduledAt, jobID)
	return err
}

func (d *SQLiteDriver) GetCurrentTime(db *sql.DB) (time.Time, error) {
	var timeStr string
	err := db.QueryRow("SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')").Scan(&timeStr)
	if err != nil {
		return time.Time{}, err
	}

	currentTime, err := time.Parse("2006-01-02 15:04:05.999", timeStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse database time: %w", err)
	}

	return currentTime, nil
}
