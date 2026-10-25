package sqlq

import (
	"math"
	"math/rand/v2"
	"time"
)

// exponentialBackoff is the default backoff function: 2^retryNum seconds, plus up to
// 20% of that at random, so that jobs which failed together don't all retry together.
// Consumers cap it at their maxBackoff. For any other behavior, set a backoff function
// of your own with WithDefaultBackoffFunc or WithConsumerBackoffFunc.
func exponentialBackoff(retryNum uint16) time.Duration {
	// Past 2^32 seconds (about 136 years), the delay with its jitter would overflow
	// time.Duration into a negative or zero duration, and the job would be retried at once.
	if retryNum > 32 {
		return math.MaxInt64
	}
	delay := time.Duration(1<<retryNum) * time.Second // 1<<retryNum is 2^retryNum
	jitter := rand.N(delay / 5)

	return delay + jitter
}

const (
	// The first retry after a failed fetch comes no later than this,
	// even when the poll interval is longer.
	fetchRetryMaxFirstDelay = 100 * time.Millisecond
	// Retries after failed fetches are never further apart than this.
	fetchRetryMaxDelay = 30 * time.Second
)

// fetchRetryBackoff returns how long the fetch loop waits after retryNum + 1 failed fetches in a row.
//
// A failed fetch usually means brief lock contention, not a database outage. So the first retry
// comes one poll interval later, or sooner if the poll interval is long. If the fetches keep
// failing, the database may be down, so each further failure doubles the delay, up to a maximum.
func fetchRetryBackoff(retryNum uint16, pollInterval time.Duration) time.Duration {
	firstDelay := pollInterval
	firstDelay = max(firstDelay, time.Millisecond) // a zero delay would never grow by doubling
	firstDelay = min(firstDelay, fetchRetryMaxFirstDelay)

	// 16 doublings take even the smallest first delay past the maximum,
	// and many more would overflow time.Duration.
	doublings := min(retryNum, 16)
	delay := firstDelay * time.Duration(1<<doublings) // 1<<doublings is 2^doublings

	return min(delay, fetchRetryMaxDelay)
}
