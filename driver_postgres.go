package sqlq

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

const pgTracerName = "github.com/your_org/sqlq/driver/postgres" // Replace with your actual module path

// PostgresDriver implements the Driver interface for PostgreSQL
type PostgresDriver struct {
	db     *sql.DB
	tracer trace.Tracer // Add tracer field
}

// NewPostgresDriver creates a new PostgreSQL driver with the given database connection
func NewPostgresDriver(db *sql.DB) *PostgresDriver {
	return &PostgresDriver{db: db, tracer: otel.Tracer(pgTracerName)} // Initialize tracer
}

func (d *PostgresDriver) InitSchema(ctx context.Context) error {
	ctx, span := d.tracer.Start(ctx, "PostgresDriver.InitSchema", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
	))
	defer span.End()

	queries := []string{
		`CREATE TABLE IF NOT EXISTS jobs (
			id SERIAL PRIMARY KEY,
			job_type TEXT NOT NULL,
			payload BYTEA,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			scheduled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			retry_count INTEGER DEFAULT 0,
			last_error TEXT,
			trace_context JSONB -- Added for trace propagation (using JSONB for efficiency)
		)`,

		`CREATE TABLE IF NOT EXISTS job_consumers (
			job_id INTEGER,
			consumer_name TEXT,
			processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (job_id, consumer_name),
			FOREIGN KEY (job_id) REFERENCES jobs(id)
		)`,

		`CREATE INDEX IF NOT EXISTS idx_jobs_job_type ON jobs(job_type)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_scheduled_at ON jobs(scheduled_at)`,

		`CREATE TABLE IF NOT EXISTS dead_letter_queue (
			id SERIAL PRIMARY KEY,
			original_job_id INTEGER,
			job_type TEXT NOT NULL,
			payload BYTEA,
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

func (d *PostgresDriver) InsertJob(ctx context.Context, jobType string, payload []byte, delay time.Duration, traceContext map[string]string) error {
	ctx, span := d.tracer.Start(ctx, "PostgresDriver.InsertJob", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.String("sqlq.job_type", jobType),
		attribute.String("sqlq.trace_context", fmt.Sprintf("%v", traceContext)), // Log trace context
		attribute.Float64("sqlq.delay_seconds", delay.Seconds()),
	))
	defer span.End()

	// Serialize trace context to JSON string
	traceContextJSON, err := json.Marshal(traceContext)
	if err != nil {
		span.RecordError(fmt.Errorf("failed to marshal trace context: %w", err))
		// Decide if you want to proceed without trace context or return error
		// For now, we log and proceed without it.
		traceContextJSON = []byte("{}") // Store empty JSON object
	}

	var query string
	var args []interface{}

	if delay <= 0 {
		// Use database's current time
		query = "INSERT INTO jobs (job_type, payload, trace_context) VALUES ($1, $2, $3)"
		args = []interface{}{jobType, payload, string(traceContextJSON)}
	} else {
		// Use the database's time function with delay
		query = "INSERT INTO jobs (job_type, payload, scheduled_at, trace_context) VALUES ($1, $2, NOW() + $3, $4)"
		args = []interface{}{jobType, payload, delay.String(), string(traceContextJSON)}
	}

	_, err = d.db.ExecContext(ctx, query, args...) // Use ExecContext
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (d *PostgresDriver) GetJobsForConsumer(ctx context.Context, consumerName, jobType string, prefetchCount int) ([]job, error) {
	ctx, span := d.tracer.Start(ctx, "PostgresDriver.GetJobsForConsumer", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.String("sqlq.consumer_name", consumerName),
		attribute.String("sqlq.job_type", jobType),
		attribute.Int("sqlq.prefetch_count", prefetchCount),
	))
	defer span.End()

	// Use FOR UPDATE SKIP LOCKED for PostgreSQL to allow multiple consumers
	// without selecting the same job concurrently.
	rows, err := d.db.QueryContext(ctx, `
		SELECT j.id, j.payload, j.retry_count, j.trace_context
		FROM jobs j
		LEFT JOIN job_consumers jc ON j.id = jc.job_id AND jc.consumer_name = $1
		WHERE j.job_type = $2 
		AND j.scheduled_at <= NOW()
		AND jc.job_id IS NULL
		ORDER BY j.id
		LIMIT $3
		FOR UPDATE SKIP LOCKED
	`, consumerName, jobType, prefetchCount)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	defer rows.Close()

	var jobs []job
	for rows.Next() {
		var j job
		var traceContextJSON []byte // JSONB is returned as []byte
		// Populate job type for potential tracing/logging later if needed
		j.JobType = jobType
		if err := rows.Scan(&j.ID, &j.Payload, &j.RetryCount, &traceContextJSON); err != nil {
			span.RecordError(err)
			return nil, err
		}

		// Deserialize trace context
		j.TraceContext = make(map[string]string)
		if len(traceContextJSON) > 0 && string(traceContextJSON) != "null" { // Check for empty or null JSONB
			if err := json.Unmarshal(traceContextJSON, &j.TraceContext); err != nil {
				span.RecordError(fmt.Errorf("failed to unmarshal trace context for job %d: %w", j.ID, err))
				// Continue without trace context if unmarshal fails
				j.TraceContext = make(map[string]string)
			}
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

func (d *PostgresDriver) MarkJobProcessed(ctx context.Context, jobID int64, consumerName string) error {
	ctx, span := d.tracer.Start(ctx, "PostgresDriver.MarkJobProcessed", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.Int64("sqlq.job_id", jobID),
		attribute.String("sqlq.consumer_name", consumerName),
	))
	defer span.End()

	_, err := d.db.ExecContext(ctx, "INSERT INTO job_consumers (job_id, consumer_name) VALUES ($1, $2)", jobID, consumerName) // Use ExecContext
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (d *PostgresDriver) MarkJobFailedAndReschedule(ctx context.Context, jobID int64, errorMsg string, backoffDuration time.Duration) error {
	ctx, span := d.tracer.Start(ctx, "PostgresDriver.MarkJobFailedAndReschedule", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.Int64("sqlq.job_id", jobID),
		attribute.String("sqlq.error_message", errorMsg),
		attribute.Float64("sqlq.backoff_seconds", backoffDuration.Seconds()),
	))
	defer span.End()

	_, err := d.db.ExecContext(ctx, // Use ExecContext
		"UPDATE jobs SET retry_count = retry_count + 1, last_error = $1, scheduled_at = NOW() + $2 WHERE id = $3",
		errorMsg, backoffDuration.String(), jobID,
	)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (d *PostgresDriver) MoveToDeadLetterQueue(ctx context.Context, jobID int64, reason string) error {
	ctx, span := d.tracer.Start(ctx, "PostgresDriver.MoveToDeadLetterQueue", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.Int64("sqlq.job_id", jobID),
		attribute.String("sqlq.dlq_reason", reason),
	))
	defer span.End()

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
		FROM jobs WHERE id = $1
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
		VALUES ($1, $2, $3, $4, $5, $6)
	`, job.ID, job.JobType, job.Payload, job.CreatedAt, job.RetryCount, reason) // Use ExecContext
	if err != nil {
		span.RecordError(err)
		return err
	}

	// Delete from jobs table
	_, err = tx.ExecContext(ctx, "DELETE FROM jobs WHERE id = $1", jobID) // Use ExecContext
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

func (d *PostgresDriver) GetDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]DeadLetterJob, error) {
	ctx, span := d.tracer.Start(ctx, "PostgresDriver.GetDeadLetterJobs", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.String("sqlq.job_type", jobType), // jobType might be empty
		attribute.Int("sqlq.limit", limit),
	))
	defer span.End()

	query := `
		SELECT id, original_job_id, job_type, payload, created_at, failed_at, retry_count, failure_reason
		FROM dead_letter_queue
		WHERE job_type = $1
		ORDER BY failed_at DESC
		LIMIT $2
	`
	if jobType == "" {
		query = `
			SELECT id, original_job_id, job_type, payload, created_at, failed_at, retry_count, failure_reason
			FROM dead_letter_queue
			ORDER BY failed_at DESC
			LIMIT $1
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
func (d *PostgresDriver) queryDeadLetterJobs(ctx context.Context, query string, args ...interface{}) ([]DeadLetterJob, error) {
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

func (d *PostgresDriver) RequeueDeadLetterJob(ctx context.Context, dlqID int64) error {
	ctx, span := d.tracer.Start(ctx, "PostgresDriver.RequeueDeadLetterJob", trace.WithAttributes(
		semconv.DBSystemPostgreSQL,
		attribute.Int64("sqlq.dlq_id", dlqID),
	))
	defer span.End()

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
		FROM dead_letter_queue WHERE id = $1
	`, dlqID).Scan(&dlqJob.ID, &dlqJob.JobType, &dlqJob.Payload) // Use QueryRowContext
	if err != nil {
		span.RecordError(err)
		if err == sql.ErrNoRows {
			return ErrJobNotFound
		}
		return err
	}

	// Insert back into the main queue with reset retry count and scheduled_at = NOW()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO jobs (job_type, payload, retry_count, scheduled_at)
		VALUES ($1, $2, 0, NOW())
	`, dlqJob.JobType, dlqJob.Payload) // Use ExecContext
	if err != nil {
		span.RecordError(err)
		return err
	}

	// Delete from the dead letter queue
	_, err = tx.ExecContext(ctx, "DELETE FROM dead_letter_queue WHERE id = $1", dlqID) // Use ExecContext
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
