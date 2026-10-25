package sqlq

import (
	"math"
	"math/rand"
	"time"
)

// exponentialBackoff is the default backoff function: 2^retryNum seconds,
// plus up to retryNum seconds of random jitter. Consumers cap it at their maxBackoff.
func exponentialBackoff(retryNum uint16) time.Duration {
	// 2^33 seconds (about 272 years) is the largest power of two in seconds
	// that a time.Duration can hold. Past it, the delay would overflow into a
	// negative or zero duration, and the job would be retried at once.
	if retryNum > 33 {
		return math.MaxInt64
	}
	delay := time.Duration(1<<retryNum) * time.Second

	var jitter time.Duration
	if retryNum > 0 {
		jitter = time.Duration(rand.Intn(int(retryNum))) * time.Second
	}

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
	delay := firstDelay * time.Duration(1<<doublings)

	return min(delay, fetchRetryMaxDelay)
}
