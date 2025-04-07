package sqlqueue

import (
	"database/sql"
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

func (d *SQLiteDriver) GetInsertJobQuery() string {
	return "INSERT INTO jobs (job_type, payload, max_retries) VALUES (?, ?, ?)"
}

func (d *SQLiteDriver) GetInsertDelayedJobQuery() string {
	return "INSERT INTO jobs (job_type, payload, scheduled_at, max_retries) VALUES (?, ?, ?, ?)"
}

func (d *SQLiteDriver) GetJobsForConsumerQuery() string {
	return `
		SELECT j.id, j.payload, j.retry_count, j.max_retries 
		FROM jobs j
		LEFT JOIN job_consumers jc ON j.id = jc.job_id AND jc.consumer_name = ?
		WHERE j.job_type = ? 
		AND j.scheduled_at <= datetime('now')
		AND jc.job_id IS NULL
		ORDER BY j.id
		LIMIT 10
	`
}

func (d *SQLiteDriver) GetMarkJobProcessedQuery() string {
	return "INSERT INTO job_consumers (job_id, consumer_name) VALUES (?, ?)"
}

func (d *SQLiteDriver) GetMarkJobFailedQuery() string {
	return "UPDATE jobs SET retry_count = retry_count + 1, last_error = ? WHERE id = ?"
}

func (d *SQLiteDriver) GetRescheduleJobQuery() string {
	return "UPDATE jobs SET scheduled_at = ? WHERE id = ?"
}

func (d *SQLiteDriver) GetCurrentTimeQuery() string {
	return "SELECT datetime('now')"
}

func (d *SQLiteDriver) FormatQueryParams(args ...interface{}) []interface{} {
	return args
}
