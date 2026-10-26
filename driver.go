// Package sqlq provides a simple SQL-backed job queue.
package sqlq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// driver defines the interface for database-specific operations
type driver interface {
	// initSchema creates the necessary tables if they don't exist
	// It should be safe to call multiple times, even though there is no reason to do so
	initSchema(ctx context.Context) error

	/*
		type job struct {
			JobType      string
			CreatedAt    time.Time
			TraceContext map[string]string
			Payload      []byte
			ID           int64
			RetryCount   uint32
		}
	*/

	// insertJob inserts through tx when provided, otherwise through the database.
	// The caller owns committing or rolling back tx.
	insertJob(ctx context.Context, tx *sql.Tx, jobType string, payload []byte, delay time.Duration, traceContext map[string]string) error

	// getJobsForConsumer executes the query for finding jobs for a consumer
	// Unprocessed jobs can be returned again after their claim expires.
	getJobsForConsumer(ctx context.Context, jobType string, prefetchCount uint16, claimTimeout time.Duration) ([]job, error)
	// extendClaim gives the claim on j a new length of timeout, if the claim is still valid.
	// If startAttempt is true, it also increments retry_count. Thus, the attempt counts also if the
	// worker process stops before it writes a result. It returns the new local deadline.
	extendClaim(ctx context.Context, j job, timeout time.Duration, startAttempt bool) (time.Time, error)
	// releaseClaim makes j available again, and sets retry_count back to j.RetryCount. Thus, an
	// attempt that the worker stopped on purpose, for example at shutdown, does not count.
	releaseClaim(ctx context.Context, j job) error

	// subscribeForConsumer will post new jobs to a channel according to tokenBucket
	subscribeForConsumer(ctx context.Context, jobType string, tokenBucket *tokenBucket) (<-chan struct{}, error)

	// markJobProcessed marks a claimed job as processed within the handler's transaction.
	// Processed jobs are not returned to consumers and are eligible for cleanup
	// If tx did not write and cannot become a writer, it returns errStaleReadTx.
	// The caller can then roll back tx and mark the job in a new transaction.
	markJobProcessed(ctx context.Context, tx *sql.Tx, jobID int64, claimToken string) error

	// markJobFailedAndReschedule combines marking a job as failed and rescheduling it
	// Rescheduled jobs can be returned to consumers again
	markJobFailedAndReschedule(ctx context.Context, jobID int64, claimToken string, errorMsg string, backoffDuration time.Duration) error

	// moveToDeadLetterQueue moves a job to the dead letter queue.
	// If inTx is not nil, it is called in the same transaction after the move,
	// and an error from it rolls the move back and is returned.
	moveToDeadLetterQueue(ctx context.Context, jobID int64, claimToken string, reason string, inTx func(tx *sql.Tx) error) error

	// getDeadLetterJobs retrieves jobs from the dead letter queue
	getDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]DeadLetterJob, error)

	// requeueDeadLetterJob moves a job from the dead letter queue back to the main queue
	requeueDeadLetterJob(ctx context.Context, dlqID int64) (jobType string, err error)

	// cleanupJobs deletes old processed jobs for a specific job type.
	// It deletes processed jobs of type jobType older than maxAge, in batches of batchSize.
	cleanupJobs(ctx context.Context, jobType string, maxAge time.Duration, batchSize uint16) (deletedCount int64, err error)

	// cleanupDeadLetterQueueJobs deletes old jobs from the dead-letter queue for a specific job type.
	// It deletes DLQ jobs of type jobType older than maxAge, in batches of batchSize.
	cleanupDeadLetterQueueJobs(ctx context.Context, jobType string, maxAge time.Duration, batchSize uint16) (deletedCount int64, err error)
}

// markJobProcessed returns errStaleReadTx if the handler transaction only read, and a different
// connection committed after that. Then the transaction cannot write.
// The handler did not write. Thus, a mark in a new transaction does not lose data.
var errStaleReadTx = errors.New("handler transaction only read, and its snapshot is stale, so it can't write")

// sqlExecutor is implemented by both *sql.DB and *sql.Tx.
type sqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
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

// runInTx executes fn within a transaction, rolling back on error.
func runInTx(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
