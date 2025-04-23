package sqlq

import (
	"context"
	"database/sql"
	"time"
)

// Driver defines the interface for database-specific operations
type Driver interface {
	// InitSchema creates the necessary tables if they don't exist
	InitSchema(ctx context.Context) error

	// InsertJob executes the query for inserting a job, including trace context
	InsertJob(ctx context.Context, jobType string, payload []byte, delay time.Duration, traceContext map[string]string) error

	// GetJobsForConsumer executes the query for finding jobs for a consumer, returning trace context
	GetJobsForConsumer(ctx context.Context, consumerName, jobType string, prefetchCount uint16) ([]job, error)

	// MarkJobProcessed executes the query for marking a job as processed
	MarkJobProcessed(ctx context.Context, jobID int64, consumerName string) error

	// MarkJobFailedAndReschedule combines marking a job as failed and rescheduling it
	MarkJobFailedAndReschedule(ctx context.Context, jobID int64, errorMsg string, backoffDuration time.Duration) error

	// MoveToDeadLetterQueue moves a job to the dead letter queue
	MoveToDeadLetterQueue(ctx context.Context, jobID int64, reason string) error

	// GetDeadLetterJobs retrieves jobs from the dead letter queue
	GetDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]DeadLetterJob, error)

	// RequeueDeadLetterJob moves a job from the dead letter queue back to the main queue
	RequeueDeadLetterJob(ctx context.Context, dlqID int64) error

	// CleanupJobs deletes old processed jobs and old dead-letter queue jobs for a specific job type.
	// It deletes processed jobs of type jobType older than processedMaxAge, in batches of batchSize.
	// It deletes DLQ jobs of type jobType older than dlqMaxAge, in batches of batchSize.
	// It returns the total number of processed jobs deleted, DLQ jobs deleted, and any error.
	CleanupJobs(
		ctx context.Context,
		jobType string,
		processedMaxAge, dlqMaxAge time.Duration,
		batchSize uint16,
	) (processedDeleted int64, dlqDeleted int64, err error)
}

// getDriver returns the appropriate driver for the given database type
func getDriver(db *sql.DB, dbType DBType) (Driver, error) {
	switch dbType {
	case DBTypeSQLite:
		return newSQLiteDriver(db), nil
	case DBTypePostgres:
		return newPostgresDriver(db), nil
	default:
		return nil, ErrUnsupportedDBType
	}
}
