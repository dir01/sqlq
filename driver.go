package sqlqueue

import (
	"database/sql"
	"time"
)

// Driver defines the interface for database-specific operations
type Driver interface {
	// InitSchema creates the necessary tables if they don't exist
	InitSchema(db *sql.DB) error

	// GetInsertJobQuery returns the query for inserting a new job
	GetInsertJobQuery() string
	
	// GetInsertDelayedJobQuery returns the query for inserting a delayed job
	GetInsertDelayedJobQuery() string

	// GetJobsForConsumerQuery returns the query for finding jobs for a consumer
	GetJobsForConsumerQuery() string

	// GetMarkJobProcessedQuery returns the query for marking a job as processed
	GetMarkJobProcessedQuery() string
	
	// GetMarkJobFailedQuery returns the query for marking a job as failed with retry info
	GetMarkJobFailedQuery() string
	
	// GetRescheduleJobQuery returns the query for rescheduling a job after a failure
	GetRescheduleJobQuery() string
	
	// GetCurrentTimeQuery returns the query to get the current database time
	GetCurrentTimeQuery() string

	// FormatQueryParams formats query parameters according to the database's requirements
	FormatQueryParams(args ...interface{}) []interface{}
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
