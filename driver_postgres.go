package sqlqueue

import (
	"database/sql"
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

func (d *PostgresDriver) GetInsertJobQuery() string {
	return "INSERT INTO jobs (job_type, payload, max_retries) VALUES ($1, $2, $3)"
}

func (d *PostgresDriver) GetInsertDelayedJobQuery() string {
	return "INSERT INTO jobs (job_type, payload, scheduled_at, max_retries) VALUES ($1, $2, $3, $4)"
}

func (d *PostgresDriver) GetJobsForConsumerQuery() string {
	return `
		SELECT j.id, j.payload, j.retry_count, j.max_retries 
		FROM jobs j
		LEFT JOIN job_consumers jc ON j.id = jc.job_id AND jc.consumer_name = $1
		WHERE j.job_type = $2 
		AND j.scheduled_at <= NOW()
		AND jc.job_id IS NULL
		ORDER BY j.id
		LIMIT 10
	`
}

func (d *PostgresDriver) GetMarkJobProcessedQuery() string {
	return "INSERT INTO job_consumers (job_id, consumer_name) VALUES ($1, $2)"
}

func (d *PostgresDriver) GetMarkJobFailedQuery() string {
	return "UPDATE jobs SET retry_count = retry_count + 1, last_error = $1 WHERE id = $2"
}

func (d *PostgresDriver) GetRescheduleJobQuery() string {
	return "UPDATE jobs SET scheduled_at = $1 WHERE id = $2"
}

func (d *PostgresDriver) GetCurrentTimeQuery() string {
	return "SELECT NOW()"
}

func (d *PostgresDriver) FormatQueryParams(args ...interface{}) []interface{} {
	return args
}
