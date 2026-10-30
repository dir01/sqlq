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
// A claim lasts the job timeout plus the claim overhead. The overhead is the time for
// the work that the claim must cover, apart from the handler. It has two halves:
//
//	| before the handler | job timeout | after the job timeout |
//	                     ^ the handler starts
//
// Before the handler, the worker:
//   - waits for a database connection for the handler's transaction.
//   - if necessary, extends the claim. That is one more round trip to the database.
//
// After the job timeout:
//   - the handler returns. It can return a short time after its context is canceled.
//   - the worker commits or rolls back the handler's transaction.
//   - the worker writes the result: it marks the job processed, or writes a retry,
//     or locks the job row to move it to the dead letter queue.
//
// The claim does not cover these times:
//   - The time in the buffer. When the job leaves the buffer, the worker extends the claim if necessary.
//   - The dead letter hook. It runs while the job row is locked, so no other consumer can claim the job.
//
// Both halves also absorb small differences between the local deadline and the expiry
// in the database, for example the rounding to whole milliseconds.

// minJobTimeout is the shortest job timeout. The job timeout options increase a shorter value to it.
// It keeps a small safety margin in the claim overhead, also when a user sets a very short job timeout.
const minJobTimeout = 50 * time.Millisecond

// claimOverhead returns the claim time in addition to the job timeout:
// half the job timeout, but not more than 10 seconds.
func claimOverhead(jobTimeout time.Duration) time.Duration {
	return min(10*time.Second, jobTimeout/2)
}

// heldClaim is a claim in consumer.claims. started is set when a worker starts the job.
// A worker does not start a job once shutdown has begun, so shutdown can release the claims
// of jobs that are not started while handlers still run.
type heldClaim struct {
	job     job
	started bool
}

// claimReleaseTimeout is the most time that one release of claims can use: the release of one claim
// after a failed attempt, or each of the two releases at shutdown.
const claimReleaseTimeout = 5 * time.Second

// claimLength returns the length of each new claim and of each extension.
func claimLength(jobTimeout time.Duration) time.Duration {
	return jobTimeout + claimOverhead(jobTimeout)
}

// minClaimLeftAtStart returns the minimum claim time that must be left when the handler starts:
// the job timeout and the half of the overhead after it.
// If less time is left, processJob extends the claim before the handler starts.
func minClaimLeftAtStart(jobTimeout time.Duration) time.Duration {
	return jobTimeout + claimOverhead(jobTimeout)/2
}

// localClaimDeadline returns the time at which the worker must think that a claim expires.
// The drivers call it before they send the claim UPDATE. Thus, the round trip and the waits
// for locks make the deadline earlier than the expiry that the database stores. The worker
// compares the deadline with its own clock, so it does not need a query for each check.
func localClaimDeadline(timeout time.Duration) time.Time {
	return time.Now().Add(timeout)
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
