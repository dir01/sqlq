package sqlq

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const sqliteTracerName = "github.com/your_org/sqlq/driver/sqlite" // Replace with your actual module path

// SQLiteDriver implements the Driver interface for SQLite
type SQLiteDriver struct {
	db     *sql.DB
	mutex  sync.Mutex
	tracer trace.Tracer // Add tracer field
}

// NewSQLiteDriver creates a new SQLite driver with the given database connection
func NewSQLiteDriver(db *sql.DB) *SQLiteDriver {
	return &SQLiteDriver{db: db, tracer: otel.Tracer(sqliteTracerName)} // Initialize tracer
}

func (d *SQLiteDriver) InitSchema(ctx context.Context) error {
	ctx, span := d.tracer.Start(ctx, "SQLiteDriver.InitSchema", trace.WithAttributes(
		semconv.DBSystemSqlite,
	))
	defer span.End()

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

	tx, err := d.db.BeginTx(ctx, nil) // Use BeginTx with context
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Rollback if commit fails or panics

	for _, query := range queries {
		_, err := tx.ExecContext(ctx, query) // Use ExecContext
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("failed to execute query (%s): %w", query, err)
		}
	}

	err = tx.Commit()
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (d *SQLiteDriver) InsertJob(ctx context.Context, jobType string, payload []byte, delay time.Duration) error {
	ctx, span := d.tracer.Start(ctx, "SQLiteDriver.InsertJob", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.String("sqlq.job_type", jobType),
		attribute.Float64("sqlq.delay_seconds", delay.Seconds()),
	))
	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	var query string
	var args []interface{}

	if delay <= 0 {
		query = "INSERT INTO jobs (job_type, payload) VALUES (?, ?)"
		args = []interface{}{jobType, payload}
	} else {
		delaySeconds := int(delay.Seconds())
		query = "INSERT INTO jobs (job_type, payload, scheduled_at) VALUES (?, ?, datetime('now', '+' || ? || ' seconds'))"
		args = []interface{}{jobType, payload, delaySeconds}
	}

	_, err := d.db.ExecContext(ctx, query, args...) // Use ExecContext
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (d *SQLiteDriver) GetJobsForConsumer(ctx context.Context, consumerName, jobType string, prefetchCount int) ([]job, error) {
	ctx, span := d.tracer.Start(ctx, "SQLiteDriver.GetJobsForConsumer", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.String("sqlq.consumer_name", consumerName),
		attribute.String("sqlq.job_type", jobType),
		attribute.Int("sqlq.prefetch_count", prefetchCount),
	))
	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Note: SQLite doesn't support FOR UPDATE SKIP LOCKED.
	// Locking is handled by the mutex at the driver level.
	rows, err := d.db.QueryContext(ctx, `
		SELECT j.id, j.payload, j.retry_count
		FROM jobs j
		LEFT JOIN job_consumers jc ON j.id = jc.job_id AND jc.consumer_name = ?
		WHERE j.job_type = ? 
		AND j.scheduled_at <= datetime('now', 'localtime')
		AND jc.job_id IS NULL
		ORDER BY j.id
		LIMIT ?
	`, consumerName, jobType, prefetchCount) // Use QueryContext
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	defer rows.Close()

	var jobs []job
	for rows.Next() {
		var j job
		// Populate job type for potential tracing/logging later if needed
		j.JobType = jobType
		if err := rows.Scan(&j.ID, &j.Payload, &j.RetryCount); err != nil {
			span.RecordError(err)
			return nil, err
		}
		jobs = append(jobs, j)
	}
	span.SetAttributes(attribute.Int("sqlq.jobs_fetched", len(jobs)))

	if err := rows.Err(); err != nil {
		span.RecordError(err)
		return nil, err
	}

	return jobs, nil
}

func (d *SQLiteDriver) MarkJobProcessed(ctx context.Context, jobID int64, consumerName string) error {
	ctx, span := d.tracer.Start(ctx, "SQLiteDriver.MarkJobProcessed", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.Int64("sqlq.job_id", jobID),
		attribute.String("sqlq.consumer_name", consumerName),
	))
	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	_, err := d.db.ExecContext(ctx, // Use ExecContext
		"INSERT INTO job_consumers (job_id, consumer_name) VALUES (?, ?)",
		jobID, consumerName,
	)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (d *SQLiteDriver) MarkJobFailedAndReschedule(ctx context.Context, jobID int64, errorMsg string, backoffDuration time.Duration) error {
	ctx, span := d.tracer.Start(ctx, "SQLiteDriver.MarkJobFailedAndReschedule", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.Int64("sqlq.job_id", jobID),
		attribute.String("sqlq.error_message", errorMsg),
		attribute.Float64("sqlq.backoff_seconds", backoffDuration.Seconds()),
	))
	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Convert duration to seconds for SQLite datetime function
	backoffSeconds := int(backoffDuration.Seconds())

	_, err := d.db.ExecContext(ctx, // Use ExecContext
		"UPDATE jobs SET retry_count = retry_count + 1, last_error = ?, scheduled_at = datetime('now', 'localtime', '+' || ? || ' seconds') WHERE id = ?",
		errorMsg, backoffSeconds, jobID,
	)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (d *SQLiteDriver) MoveToDeadLetterQueue(ctx context.Context, jobID int64, reason string) error {
	ctx, span := d.tracer.Start(ctx, "SQLiteDriver.MoveToDeadLetterQueue", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.Int64("sqlq.job_id", jobID),
		attribute.String("sqlq.dlq_reason", reason),
	))
	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	tx, err := d.db.BeginTx(ctx, nil) // Use BeginTx with context
	if err != nil {
		span.RecordError(err)
		return err
	}
	defer tx.Rollback()

	// Get the job details
	var job job
	err = tx.QueryRowContext(ctx, `
		SELECT id, job_type, payload, created_at, retry_count
		FROM jobs WHERE id = ?
	`, jobID).Scan(&job.ID, &job.JobType, &job.Payload, &job.CreatedAt, &job.RetryCount) // Use QueryRowContext
	if err != nil {
		span.RecordError(err)
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
	`, job.ID, job.JobType, job.Payload, job.CreatedAt, job.RetryCount, reason) // Use ExecContext
	if err != nil {
		span.RecordError(err)
		return err
	}

	// Delete job from regular jobs
	_, err = tx.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, job.ID) // Use ExecContext
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to delete job from main table: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (d *SQLiteDriver) GetDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]DeadLetterJob, error) {
	ctx, span := d.tracer.Start(ctx, "SQLiteDriver.GetDeadLetterJobs", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.String("sqlq.job_type", jobType), // jobType might be empty
		attribute.Int("sqlq.limit", limit),
	))
	defer span.End()

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
		return d.queryDeadLetterJobs(ctx, query, limit)
	}

	jobs, err := d.queryDeadLetterJobs(ctx, query, jobType, limit) // Pass context
	if err != nil {
		span.RecordError(err)
	} else {
		span.SetAttributes(attribute.Int("sqlq.dlq_jobs_fetched", len(jobs)))
	}
	return jobs, err
}

// queryDeadLetterJobs is a helper, context is passed from the caller
func (d *SQLiteDriver) queryDeadLetterJobs(ctx context.Context, query string, args ...interface{}) ([]DeadLetterJob, error) {
	rows, err := d.db.QueryContext(ctx, query, args...) // Use QueryContext
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
			// Don't record error in span here, let the caller handle it
			return nil, err
		}
		jobs = append(jobs, j)
	}

	if err := rows.Err(); err != nil {
		// Don't record error in span here, let the caller handle it
		return nil, err
	}

	return jobs, nil
}

func (d *SQLiteDriver) RequeueDeadLetterJob(ctx context.Context, dlqID int64) error {
	ctx, span := d.tracer.Start(ctx, "SQLiteDriver.RequeueDeadLetterJob", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.Int64("sqlq.dlq_id", dlqID),
	))
	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	tx, err := d.db.BeginTx(ctx, nil) // Use BeginTx with context
	if err != nil {
		span.RecordError(err)
		return err
	}
	defer tx.Rollback()

	// Get the job from the dead letter queue
	var dlqJob DeadLetterJob
	err = tx.QueryRowContext(ctx, `
		SELECT id, job_type, payload
		FROM dead_letter_queue WHERE id = ?
	`, dlqID).Scan(&dlqJob.ID, &dlqJob.JobType, &dlqJob.Payload) // Use QueryRowContext
	if err != nil {
		span.RecordError(err)
		if err == sql.ErrNoRows {
			return ErrJobNotFound
		}
		return err
	}

	// Insert back into the main queue with reset retry count and scheduled_at = now
	_, err = tx.ExecContext(ctx, `
		INSERT INTO jobs (job_type, payload, retry_count, scheduled_at)
		VALUES (?, ?, 0, datetime('now', 'localtime'))
	`, dlqJob.JobType, dlqJob.Payload) // Use ExecContext
	if err != nil {
		span.RecordError(err)
		return err
	}

	// Delete from the dead letter queue
	_, err = tx.ExecContext(ctx, "DELETE FROM dead_letter_queue WHERE id = ?", dlqID) // Use ExecContext
	if err != nil {
		span.RecordError(err)
		return err
	}

	err = tx.Commit()
	if err != nil {
		span.RecordError(err)
	}
	return err
}
