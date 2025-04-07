package sqlqueue

import (
	"database/sql"
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
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		
		`CREATE TABLE IF NOT EXISTS job_consumers (
			job_id INT,
			consumer_name VARCHAR(255),
			processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (job_id, consumer_name),
			FOREIGN KEY (job_id) REFERENCES jobs(id)
		)`,
		
		`CREATE INDEX idx_jobs_job_type ON jobs(job_type(255))`}
	
	for _, query := range queries {
		_, err := db.Exec(query)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *MySQLDriver) GetInsertJobQuery() string {
	return "INSERT INTO jobs (job_type, payload) VALUES (?, ?)"
}

func (d *MySQLDriver) GetJobsForConsumerQuery() string {
	return `
		SELECT j.id, j.payload 
		FROM jobs j
		LEFT JOIN job_consumers jc ON j.id = jc.job_id AND jc.consumer_name = ?
		WHERE j.job_type = ? AND jc.job_id IS NULL
		ORDER BY j.id
		LIMIT 10
	`
}

func (d *MySQLDriver) GetMarkJobProcessedQuery() string {
	return "INSERT INTO job_consumers (job_id, consumer_name) VALUES (?, ?)"
}

func (d *MySQLDriver) FormatQueryParams(args ...interface{}) []interface{} {
	return args
}
