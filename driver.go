package sqlq

import (
	"database/sql"
	"time"
)

// Driver defines the interface for database-specific operations
type Driver interface {
	// InitSchema creates the necessary tables if they don't exist
	InitSchema() error

	// InsertJob executes the query for inserting a new job
	InsertJob(jobType string, payload []byte, maxRetries int) error

	// InsertDelayedJob executes the query for inserting a delayed job
	InsertDelayedJob(jobType string, payload []byte, scheduledAt time.Time, maxRetries int) error

	// GetJobsForConsumer executes the query for finding jobs for a consumer
	GetJobsForConsumer(consumerName, jobType string, prefetchCount int) ([]job, error)

	// MarkJobProcessed executes the query for marking a job as processed
	MarkJobProcessed(jobID int64, consumerName string) error

	// MarkJobFailed executes the query for marking a job as failed with retry info
	MarkJobFailed(jobID int64, errorMsg string) error

	// RescheduleJob executes the query for rescheduling a job after a failure
	RescheduleJob(jobID int64, scheduledAt time.Time) error

	// GetCurrentTime executes the query to get the current database time
	GetCurrentTime() (time.Time, error)
}

// GetDriver returns the appropriate driver for the given database type
func GetDriver(db *sql.DB, dbType DBType) (Driver, error) {
	switch dbType {
	case DBTypeSQLite:
		return NewSQLiteDriver(db), nil
	case DBTypePostgres:
		return NewPostgresDriver(db), nil
	case DBTypeMySQL:
		return NewMySQLDriver(db), nil
	default:
		return nil, ErrUnsupportedDBType
	}
}
