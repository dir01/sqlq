package sqlq

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	"time"
)

// A claim lets one consumer work on one job for a limited time.
// The jobs table stores the claim token and the expiry time.
// Every later write for the job must give the same token, or it fails with ErrClaimLost.
// After a claim expires, another consumer can claim the job with a new token.
// This lets another worker finish jobs that a stopped worker left behind.

// claimKey returns the key for a claim in consumer.claims. The consumer uses that map
// to limit how many jobs it holds, and to release them at shutdown.
// The key includes the token, because the consumer can claim the same job again
// after its old claim expires.
func claimKey(j job) string {
	return fmt.Sprintf("%d/%s", j.ID, j.ClaimToken)
}

// claimBudget returns how long a worker can count on a claim, from the time it asked for it.
// It drops any part of a millisecond, because the database gets the timeout in whole milliseconds.
// It takes off 1ms more, because SQLite reads the clock in whole milliseconds and drops the rest,
// so the claim start it stores can be up to 1ms before the real time.
func claimBudget(timeout time.Duration) time.Duration {
	return timeout.Truncate(time.Millisecond) - time.Millisecond
}

// localClaimDeadline returns the last time a worker can start a job without asking the database.
// getJobsForConsumer and extendClaim in each driver call it before they send their UPDATE,
// so it is never later than the expiry the database stores. processJob compares it with
// the current time before it runs the handler (see claimTimeToStart).
func localClaimDeadline(timeout time.Duration) time.Time {
	return time.Now().Add(claimBudget(timeout))
}

// Each new claim and each extension lasts the job timeout plus a gap. The gap has two halves:
//
//	| start | job timeout | finish |
//	        ^ the handler starts; claimTimeToStart must be left
//
// Start: the worker gets a connection and, if necessary, extends the claim.
// Finish: the handler returns and the worker writes the result.

// claimGap returns the claim time in addition to the job timeout: half the job timeout,
// but at most one minute. It is at least 10ms, because claimBudget removes up to 2ms,
// and that must leave time for the start in very short claims.
func claimGap(jobTimeout time.Duration) time.Duration {
	return max(min(time.Minute, jobTimeout/2), 10*time.Millisecond)
}

// claimLength returns the length of each new claim and each extension.
func claimLength(jobTimeout time.Duration) time.Duration {
	return jobTimeout + claimGap(jobTimeout)
}

// claimTimeToStart returns the claim time that must be left when a handler starts.
// If less time is left, processJob extends the claim first.
func claimTimeToStart(jobTimeout time.Duration) time.Duration {
	return jobTimeout + claimGap(jobTimeout)/2
}

// newClaimToken returns a random string for one claim request (128 random bits, base32).
// The drivers add ":<job ID>", so each job in a batch gets its own token.
// It is random so that consumers in different processes never make the same token.
func newClaimToken() string {
	return rand.Text()
}

// checkClaimResult checks a write that filters on the claim token and must change one row.
// If it did not change one row, it returns ErrClaimLost. That happens when another consumer
// took the job, the claim was released, or the job is already done.
// If the write itself failed, it returns that error.
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
