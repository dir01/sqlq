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

func (d *PostgresDriver) GetInsertJobQuery() string {
	return "INSERT INTO jobs (job_type, payload) VALUES ($1, $2)"
}

func (d *PostgresDriver) GetJobsForConsumerQuery() string {
	return `
		SELECT j.id, j.payload 
		FROM jobs j
		LEFT JOIN job_consumers jc ON j.id = jc.job_id AND jc.consumer_name = $1
		WHERE j.job_type = $2 AND jc.job_id IS NULL
		ORDER BY j.id
		LIMIT 10
	`
}

func (d *PostgresDriver) GetMarkJobProcessedQuery() string {
	return "INSERT INTO job_consumers (job_id, consumer_name) VALUES ($1, $2)"
}

func (d *PostgresDriver) FormatQueryParams(args ...interface{}) []interface{} {
	return args
}
