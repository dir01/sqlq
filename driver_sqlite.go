package sqlq

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"encoding/json"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const sqliteTracerName = "github.com/dir01/sqlq/driver_sqlite"

// SQLiteDriver implements the Driver interface for SQLite
type SQLiteDriver struct {
	db     *sql.DB
	mutex  sync.Mutex
	tracer trace.Tracer
}

// NewSQLiteDriver creates a new SQLite driver with the given database connection
func NewSQLiteDriver(db *sql.DB) *SQLiteDriver {
	return &SQLiteDriver{db: db, tracer: otel.Tracer(sqliteTracerName)}
}

func (d *SQLiteDriver) InitSchema(ctx context.Context) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.init_schema", trace.WithAttributes(
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
			created_at INTEGER NOT NULL, -- Milliseconds since epoch
			scheduled_at INTEGER NOT NULL, -- Milliseconds since epoch
			retry_count INTEGER DEFAULT 0,
			last_error TEXT,
			trace_context TEXT -- Added for trace propagation
		)`,

		`CREATE INDEX IF NOT EXISTS idx_jobs_job_type ON jobs(job_type)`,

		`CREATE INDEX IF NOT EXISTS idx_jobs_scheduled_at ON jobs(scheduled_at)`,

		`CREATE TABLE IF NOT EXISTS job_consumers (
			job_id INTEGER,
			consumer_name TEXT,
			processed_at INTEGER NOT NULL, -- Milliseconds since epoch
			PRIMARY KEY (job_id, consumer_name),
			FOREIGN KEY (job_id) REFERENCES jobs(id)
		)`,

		`CREATE TABLE IF NOT EXISTS dead_letter_queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			original_job_id INTEGER,
			job_type TEXT NOT NULL,
			payload BLOB,
			created_at INTEGER NOT NULL, -- Milliseconds since epoch
			failed_at INTEGER NOT NULL, -- Milliseconds since epoch
			retry_count INTEGER,
			failure_reason TEXT
		)`,

		`CREATE INDEX IF NOT EXISTS idx_dlq_job_type ON dead_letter_queue(job_type)`,
	}

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	for _, query := range queries {
		_, err := tx.ExecContext(ctx, query)
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

func (d *SQLiteDriver) InsertJob(
	ctx context.Context,
	jobType string,
	payload []byte,
	delay time.Duration,
	traceContext map[string]string,
) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.insert_job", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.String("sqlq.job_type", jobType),
		attribute.String("sqlq.trace_context", fmt.Sprintf("%v", traceContext)),
		attribute.Float64("sqlq.delay_seconds", delay.Seconds()),
	))
	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	traceContextJSON, err := json.Marshal(traceContext)
	if err != nil {
		span.RecordError(fmt.Errorf("failed to marshal trace context: %w", err))
		traceContextJSON = []byte("{}")
	}

	// Get current time in milliseconds since epoch
	nowMs := time.Now().UnixMilli()
	
	// Calculate scheduled time in milliseconds
	scheduledMs := nowMs
	if delay > 0 {
		scheduledMs = nowMs + delay.Milliseconds()
	}

	query := "INSERT INTO jobs (job_type, payload, created_at, scheduled_at, trace_context) VALUES (?, ?, ?, ?, ?)"
	args := []interface{}{jobType, payload, nowMs, scheduledMs, string(traceContextJSON)}

	_, err = d.db.ExecContext(ctx, query, args...)
	if err != nil {
		span.RecordError(err)
	}

	return err
}

func (d *SQLiteDriver) GetJobsForConsumer(ctx context.Context, consumerName, jobType string, prefetchCount int) ([]job, error) {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.get_jobs_for_consumer", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.String("sqlq.consumer_name", consumerName),
		attribute.String("sqlq.job_type", jobType),
		attribute.Int("sqlq.prefetch_count", prefetchCount),
	))
	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Get current time in milliseconds
	nowMs := time.Now().UnixMilli()

	rows, err := d.db.QueryContext(ctx, `
		SELECT j.id, j.payload, j.retry_count, j.trace_context, j.scheduled_at
		FROM jobs j
		LEFT JOIN job_consumers jc ON j.id = jc.job_id AND jc.consumer_name = ?
		WHERE j.job_type = ? 
		AND j.scheduled_at <= ?
		AND jc.job_id IS NULL
		ORDER BY j.id
		LIMIT ?
	`, consumerName, jobType, nowMs, prefetchCount)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	defer rows.Close()

	var jobs []job
	for rows.Next() {
		var j job
		var traceContextJSON sql.NullString
		var scheduledAtMs int64
		// Populate job type for potential tracing/logging later if needed
		j.JobType = jobType
		if err := rows.Scan(&j.ID, &j.Payload, &j.RetryCount, &traceContextJSON, &scheduledAtMs); err != nil {
			span.RecordError(err)
			return nil, err
		}

		scheduledAt := time.UnixMilli(scheduledAtMs)
		fmt.Printf("JOB SCHEDULED_AT: %s\n", scheduledAt)

		// Deserialize trace context
		j.TraceContext = make(map[string]string)
		if traceContextJSON.Valid && traceContextJSON.String != "" {
			if err := json.Unmarshal([]byte(traceContextJSON.String), &j.TraceContext); err != nil {
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

func (d *SQLiteDriver) MarkJobProcessed(ctx context.Context, jobID int64, consumerName string) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.mark_job_processed", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.Int64("sqlq.job_id", jobID),
		attribute.String("sqlq.consumer_name", consumerName),
	))

	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Get current time in milliseconds
	nowMs := time.Now().UnixMilli()

	_, err := d.db.ExecContext(ctx,
		"INSERT INTO job_consumers (job_id, consumer_name, processed_at) VALUES (?, ?, ?)",
		jobID, consumerName, nowMs,
	)
	if err != nil {
		span.RecordError(err)
	}

	return err
}

func (d *SQLiteDriver) MarkJobFailedAndReschedule(
	ctx context.Context,
	jobID int64,
	errorMsg string,
	backoffDuration time.Duration,
) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.mark_job_failed_and_reschedule", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.Int64("sqlq.job_id", jobID),
		attribute.String("sqlq.error_message", errorMsg),
		attribute.Float64("sqlq.backoff_seconds", backoffDuration.Seconds()),
	))

	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Calculate new scheduled time in milliseconds
	nowMs := time.Now().UnixMilli()
	scheduledMs := nowMs + backoffDuration.Milliseconds()

	_, err := d.db.ExecContext(ctx,
		"UPDATE jobs SET retry_count = retry_count + 1, last_error = ?, scheduled_at = ? WHERE id = ?",
		errorMsg, scheduledMs, jobID,
	)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

func (d *SQLiteDriver) MoveToDeadLetterQueue(ctx context.Context, jobID int64, reason string) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.move_to_dlq", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.Int64("sqlq.job_id", jobID),
		attribute.String("sqlq.dlq_reason", reason),
	))
	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(err)
		return err
	}
	defer tx.Rollback()

	// Get current time in milliseconds
	nowMs := time.Now().UnixMilli()

	var job struct {
		ID         int64
		JobType    string
		Payload    []byte
		CreatedAtMs int64
		RetryCount int
	}
	
	err = tx.QueryRowContext(ctx, `
		SELECT id, job_type, payload, created_at, retry_count
		FROM jobs WHERE id = ?
	`, jobID).Scan(&job.ID, &job.JobType, &job.Payload, &job.CreatedAtMs, &job.RetryCount)
	if err != nil {
		span.RecordError(err)
		if err == sql.ErrNoRows {
			return ErrJobNotFound
		}
		return err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO dead_letter_queue 
		(original_job_id, job_type, payload, created_at, failed_at, retry_count, failure_reason)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, job.ID, job.JobType, job.Payload, job.CreatedAtMs, nowMs, job.RetryCount, reason)
	if err != nil {
		span.RecordError(err)
		return err
	}

	_, err = tx.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, job.ID)
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
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.get_dlq_jobs", trace.WithAttributes(
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

	jobs, err := d.queryDeadLetterJobs(ctx, query, jobType, limit)
	if err != nil {
		span.RecordError(err)
	} else {
		span.SetAttributes(attribute.Int("sqlq.dlq_jobs_fetched", len(jobs)))
	}
	return jobs, err
}

func (d *SQLiteDriver) queryDeadLetterJobs(ctx context.Context, query string, args ...interface{}) ([]DeadLetterJob, error) {
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []DeadLetterJob
	for rows.Next() {
		var j DeadLetterJob
		var createdAtMs, failedAtMs int64
		
		if err := rows.Scan(
			&j.ID,
			&j.OriginalID,
			&j.JobType,
			&j.Payload,
			&createdAtMs,
			&failedAtMs,
			&j.RetryCount,
			&j.FailureReason,
		); err != nil {
			// Don't record error in span here, let the caller handle it
			return nil, err
		}
		
		// Convert milliseconds to time.Time
		j.CreatedAt = time.UnixMilli(createdAtMs)
		j.FailedAt = time.UnixMilli(failedAtMs)
		
		jobs = append(jobs, j)
	}

	if err := rows.Err(); err != nil {
		// Don't record error in span here, let the caller handle it
		return nil, err
	}

	return jobs, nil
}

func (d *SQLiteDriver) RequeueDeadLetterJob(ctx context.Context, dlqID int64) error {
	ctx, span := d.tracer.Start(ctx, "sqlq.driver.sqlite.requeue_dead_letter_job", trace.WithAttributes(
		semconv.DBSystemSqlite,
		attribute.Int64("sqlq.dlq_id", dlqID),
	))
	defer span.End()

	d.mutex.Lock()
	defer d.mutex.Unlock()

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(err)
		return err
	}
	defer tx.Rollback()

	var dlqJob DeadLetterJob
	err = tx.QueryRowContext(ctx, `
		SELECT id, job_type, payload
		FROM dead_letter_queue WHERE id = ?
	`, dlqID).Scan(&dlqJob.ID, &dlqJob.JobType, &dlqJob.Payload)
	if err != nil {
		span.RecordError(err)
		if err == sql.ErrNoRows {
			return ErrJobNotFound
		}
		return err
	}

	// Get current time in milliseconds
	nowMs := time.Now().UnixMilli()

	// reset retry count and scheduled_at = now
	_, err = tx.ExecContext(ctx, `
		INSERT INTO jobs (job_type, payload, retry_count, created_at, scheduled_at)
		VALUES (?, ?, 0, ?, ?)
	`, dlqJob.JobType, dlqJob.Payload, nowMs, nowMs)
	if err != nil {
		span.RecordError(err)
		return err
	}

	_, err = tx.ExecContext(ctx, "DELETE FROM dead_letter_queue WHERE id = ?", dlqID)
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
