// Package sqlq provides a simple SQL-backed job queue.
package sqlq

import (
	"context"
	"database/sql"
	"time"
)

// driver defines the interface for database-specific operations
type driver interface {
	// initSchema creates the necessary tables if they don't exist
	// It should be safe to call multiple times, even though there is no reason to do so
	initSchema(ctx context.Context) error

	// insertJob executes the query for inserting a job
	insertJob(ctx context.Context, jobType string, payload []byte, delay time.Duration, traceContext map[string]string) error

	// getJobsForConsumer executes the query for finding jobs for a consumer
	// Jobs returned once should not be returned unless they were explicitly rescheduled
	getJobsForConsumer(ctx context.Context, consumerName, jobType string, prefetchCount uint16) ([]job, error)

	// markJobProcessed executes the query for marking a job as processed
	// Processed jobs are not returned to consumers and are eligible for cleanup
	markJobProcessed(ctx context.Context, jobID int64) error

	// markJobFailedAndReschedule combines marking a job as failed and rescheduling it
	// Rescheduled jobs can be returned to consumers again
	markJobFailedAndReschedule(ctx context.Context, jobID int64, errorMsg string, backoffDuration time.Duration) error

	// moveToDeadLetterQueue moves a job to the dead letter queue
	moveToDeadLetterQueue(ctx context.Context, jobID int64, reason string) error

	// getDeadLetterJobs retrieves jobs from the dead letter queue
	getDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]DeadLetterJob, error)

	// requeueDeadLetterJob moves a job from the dead letter queue back to the main queue
	requeueDeadLetterJob(ctx context.Context, dlqID int64) error

	// cleanupJobs deletes old processed jobs for a specific job type.
	// It deletes processed jobs of type jobType older than maxAge, in batches of batchSize.
	cleanupJobs(ctx context.Context, jobType string, maxAge time.Duration, batchSize uint16) (deletedCount int64, err error)

	// cleanupDeadLetterQueueJobs deletes old jobs from the dead-letter queue for a specific job type.
	// It deletes DLQ jobs of type jobType older than maxAge, in batches of batchSize.
	cleanupDeadLetterQueueJobs(ctx context.Context, jobType string, maxAge time.Duration, batchSize uint16) (deletedCount int64, err error)
}

// getDriver returns the appropriate driver for the given database type
func getDriver(db *sql.DB, dbType DBType) (driver, error) {
	switch dbType {
	case DBTypeSQLite:
		return newSQLiteDriver(db), nil
	case DBTypePostgres:
		return newPostgresDriver(db), nil
	default:
		return nil, ErrUnsupportedDBType
	}
}
