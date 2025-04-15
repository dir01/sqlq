package sqlq_test

import (
	"github.com/dir01/sqlq"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/mattn/go-sqlite3"
)

// TestPayload is the test data structure used across all tests
type TestPayload struct {
	Message string `json:"message"`
	Count   int    `json:"count"`
}

// TestCase contains actual logic of tests that will work across all the different drivers
// The idea is that you create per-driver test, and then call methods of test case
// This solution aims at providing an easy way to run and debug individual tests while sharing test logic
type TestCase struct {
	Q sqlq.JobsQueue
}
