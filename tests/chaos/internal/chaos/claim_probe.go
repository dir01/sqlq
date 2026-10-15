package chaos

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/dir01/sqlq"
)

// AcceptedClaimTimeout returns the claim timeout to run with: claimTimeout if a consumer
// accepts it with jobTimeout, or else the first accepted one found by doubling its excess over
// jobTimeout, and how many were rejected first. The harness does not know the queue's lower
// bound; whatever registration accepts must work, which the run then checks.
func AcceptedClaimTimeout(ctx context.Context, jobTimeout, claimTimeout time.Duration) (time.Duration, int, error) {
	db, err := sql.Open("sqlite3", "file::memory:")
	if err != nil {
		return 0, 0, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1) // Each connection would have a database of its own.
	q, err := sqlq.New(db, sqlq.DBTypeSQLite)
	if err != nil {
		return 0, 0, err
	}
	defer q.Shutdown()
	for rejected := 0; ; rejected++ {
		err := q.Consume(ctx, "claim_probe", func(context.Context, *sql.Tx, []byte) error { return nil },
			sqlq.WithConsumerJobTimeout(jobTimeout), sqlq.WithConsumerClaimTimeout(claimTimeout))
		if !errors.Is(err, sqlq.ErrClaimTimeoutTooShort) {
			return claimTimeout, rejected, err
		}
		claimTimeout = jobTimeout + 2*(claimTimeout-jobTimeout)
	}
}
