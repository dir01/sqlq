package sqlqueue

import (
	"database/sql"
)

// SQLiteDriver implements the Driver interface for SQLite
type SQLiteDriver struct{}

func (d *SQLiteDriver) InitSchema(db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_type TEXT NOT NULL,
			payload BLOB,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		CREATE TABLE IF NOT EXISTS job_consumers (
			job_id INTEGER,
			consumer_name TEXT,
			processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (job_id, consumer_name),
			FOREIGN KEY (job_id) REFERENCES jobs(id)
		);
		
		CREATE INDEX IF NOT EXISTS idx_jobs_job_type ON jobs(job_type);
	`
	_, err := db.Exec(query)
	return err
}

func (d *SQLiteDriver) GetInsertJobQuery() string {
	return "INSERT INTO jobs (job_type, payload) VALUES (?, ?)"
}

func (d *SQLiteDriver) GetJobsForConsumerQuery() string {
	return `
		SELECT j.id, j.payload 
		FROM jobs j
		LEFT JOIN job_consumers jc ON j.id = jc.job_id AND jc.consumer_name = ?
		WHERE j.job_type = ? AND jc.job_id IS NULL
		ORDER BY j.id
		LIMIT 10
	`
}

func (d *SQLiteDriver) GetMarkJobProcessedQuery() string {
	return "INSERT INTO job_consumers (job_id, consumer_name) VALUES (?, ?)"
}

func (d *SQLiteDriver) FormatQueryParams(args ...interface{}) []interface{} {
	return args
}
