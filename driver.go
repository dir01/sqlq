package sqlq

import (
	"database/sql"
	"time"
)

// Driver defines the interface for database-specific operations
type Driver interface {
	// InitSchema creates the necessary tables if they don't exist
	InitSchema(db *sql.DB) error

	// InsertJob executes the query for inserting a new job
	InsertJob(db *sql.DB, jobType string, payload []byte, maxRetries int) error

	// InsertDelayedJob executes the query for inserting a delayed job
	InsertDelayedJob(db *sql.DB, jobType string, payload []byte, scheduledAt time.Time, maxRetries int) error

	// GetJobsForConsumer executes the query for finding jobs for a consumer
	GetJobsForConsumer(db *sql.DB, consumerName, jobType string, prefetchCount int) ([]job, error)

	// MarkJobProcessed executes the query for marking a job as processed
	MarkJobProcessed(db *sql.DB, jobID int64, consumerName string) error

	// MarkJobFailed executes the query for marking a job as failed with retry info
	MarkJobFailed(db *sql.DB, jobID int64, errorMsg string) error

	// RescheduleJob executes the query for rescheduling a job after a failure
	RescheduleJob(db *sql.DB, jobID int64, scheduledAt time.Time) error

	// GetCurrentTime executes the query to get the current database time
	GetCurrentTime(db *sql.DB) (time.Time, error)
}

// GetDriver returns the appropriate driver for the given database type
func GetDriver(dbType DBType) (Driver, error) {
	switch dbType {
	case DBTypeSQLite:
		return &SQLiteDriver{}, nil
	case DBTypePostgres:
		return &PostgresDriver{}, nil
	case DBTypeMySQL:
		return &MySQLDriver{}, nil
	default:
		return nil, ErrUnsupportedDBType
	}
}
