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

	// InsertJob executes the query for inserting a job
	InsertJob(ctx context.Context, jobType string, payload []byte, delay time.Duration) error

	// GetJobsForConsumer executes the query for finding jobs for a consumer
	GetJobsForConsumer(ctx context.Context, consumerName, jobType string, prefetchCount int) ([]job, error)

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
}

// GetDriver returns the appropriate driver for the given database type
func GetDriver(db *sql.DB, dbType DBType) (Driver, error) {
	switch dbType {
	case DBTypeSQLite:
		return NewSQLiteDriver(db), nil
	case DBTypePostgres:
		return NewPostgresDriver(db), nil
	default:
		return nil, ErrUnsupportedDBType
	}
}
