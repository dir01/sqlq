package sqlq

import (
	"database/sql"
	"time"
)

// Driver defines the interface for database-specific operations
type Driver interface {
	// InitSchema creates the necessary tables if they don't exist
	InitSchema() error

	// InsertJob executes the query for inserting a job
	InsertJob(jobType string, payload []byte, scheduledAt time.Time) error

	// GetJobsForConsumer executes the query for finding jobs for a consumer
	GetJobsForConsumer(consumerName, jobType string, prefetchCount int) ([]job, error)

	// MarkJobProcessed executes the query for marking a job as processed
	MarkJobProcessed(jobID int64, consumerName string) error

	// MarkJobFailedAndReschedule combines marking a job as failed and rescheduling it
	MarkJobFailedAndReschedule(jobID int64, errorMsg string, scheduledAt time.Time) error

	// MoveToDeadLetterQueue moves a job to the dead letter queue
	MoveToDeadLetterQueue(jobID int64, reason string) error

	// GetDeadLetterJobs retrieves jobs from the dead letter queue
	GetDeadLetterJobs(jobType string, limit int) ([]DeadLetterJob, error)

	// RequeueDeadLetterJob moves a job from the dead letter queue back to the main queue
	RequeueDeadLetterJob(dlqID int64) error

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
