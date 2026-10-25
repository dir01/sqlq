package sqlq

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	"time"
)

// A claim gives one consumer the right to process one job for a limited time.
// The jobs table stores the claim token and the expiry time of each claim.
// Each write for the job must give the token. If the token is different, the write fails with ErrClaimLost.
// When a claim expires, a different consumer can claim the job with a new token.
// Thus, the jobs of a worker that stopped are processed again.
//
// The job timeout sets the length of each claim:
//
//	| start | job timeout | finish |
//	        ^ the handler starts
//
//   - Start: the worker gets a connection and, if necessary, extends the claim.
//   - Finish: the handler returns, and the worker writes the result.
//
// The start and the finish are each half of the claim gap.

// minJobTimeout is the shortest job timeout. The job timeout options increase a shorter value to it.
// Then each half of the claim gap is at least 12.5ms. That is sufficient for the rounding
// in claimBudget and for one round trip to the database.
const minJobTimeout = 50 * time.Millisecond

// claimGap returns the claim time after the job timeout: half the job timeout, but not more than one minute.
func claimGap(jobTimeout time.Duration) time.Duration {
	return min(time.Minute, jobTimeout/2)
}

// claimLength returns the length of each new claim and of each extension.
func claimLength(jobTimeout time.Duration) time.Duration {
	return jobTimeout + claimGap(jobTimeout)
}

// claimTimeToStart returns the claim time that must be left when the handler starts.
// If less time is left, processJob extends the claim before the handler starts.
func claimTimeToStart(jobTimeout time.Duration) time.Duration {
	return jobTimeout + claimGap(jobTimeout)/2
}

// claimBudget returns the time that a worker can use from a claim of the given length.
// It is the length minus up to 2ms. Thus, the local deadline is never later than the expiry in the database:
//   - The database gets the length in whole milliseconds.
//   - SQLite stores the start of the claim in whole milliseconds, so the start can be up to 1ms too early.
func claimBudget(timeout time.Duration) time.Duration {
	return timeout.Truncate(time.Millisecond) - time.Millisecond
}

// localClaimDeadline returns the time at which the worker must think that a claim expires.
// The drivers call it before they send the claim UPDATE, so the deadline is not later than
// the expiry that the database stores. The worker compares the deadline with its own clock,
// so it does not need a query for each check.
func localClaimDeadline(timeout time.Duration) time.Time {
	return time.Now().Add(claimBudget(timeout))
}

// claimKey returns the key of a claim in consumer.claims. The consumer uses that map to count
// the jobs that it holds, and to release them at shutdown. The key includes the token, because
// the consumer can claim the same job again after its old claim expires.
func claimKey(j job) string {
	return fmt.Sprintf("%d/%s", j.ID, j.ClaimToken)
}

// newClaimToken returns a random token for one claim query: 128 random bits in base32.
// The drivers add ":<job ID>", so each job in the batch gets a different token.
// The token is random, so consumers in different processes do not make the same token.
func newClaimToken() string {
	return rand.Text()
}

// checkClaimResult examines the result of a write that has a filter on the claim token.
// The write must change one row. If it changed no row, the function returns ErrClaimLost:
// a different consumer has the job, the claim was released, or the job is complete.
// If the write failed, the function returns its error.
func checkClaimResult(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrClaimLost
	}
	return nil
}
