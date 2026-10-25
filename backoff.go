package sqlq

import (
	"math"
	"math/rand/v2"
	"time"
)

// exponentialBackoff is the default backoff function. The delay is 2^retryNum seconds,
// plus a random jitter of up to 20%. The jitter prevents a retry of all failed jobs at the same time.
// The consumer limits the delay to maxBackoff. For a different behavior, set a different
// function with WithDefaultBackoffFunc or WithConsumerBackoffFunc.
func exponentialBackoff(retryNum uint16) time.Duration {
	// Above 2^32 seconds (approximately 136 years), the delay and the jitter overflow time.Duration.
	// Then the delay is negative or zero, and the job retries immediately.
	if retryNum > 32 {
		return math.MaxInt64
	}
	delay := time.Duration(1<<retryNum) * time.Second // 1<<retryNum is 2^retryNum
	jitter := rand.N(delay / 5)

	return delay + jitter
}

const (
	// The maximum delay before the first retry of a failed fetch.
	// Most fetch errors occur because of a short lock, and a fast retry is successful.
	// During the delay, the consumer gets no jobs, also not from async push.
	// Thus, a long poll interval must not cause a long delay.
	fetchRetryMaxFirstDelay = 100 * time.Millisecond
	// The maximum delay between retries of a failed fetch.
	fetchRetryMaxDelay = 30 * time.Second
)

// fetchRetryBackoff returns the delay of the fetch loop after retryNum + 1 failed fetches in sequence.
//
// The first delay is the poll interval, but not more than fetchRetryMaxFirstDelay.
// If the failures continue, the database is possibly not available. Then each failure
// doubles the delay, to a maximum of fetchRetryMaxDelay.
func fetchRetryBackoff(retryNum uint16, pollInterval time.Duration) time.Duration {
	firstDelay := pollInterval
	firstDelay = max(firstDelay, time.Millisecond) // a zero delay does not increase when it doubles
	firstDelay = min(firstDelay, fetchRetryMaxFirstDelay)

	// After 16 doublings, all first delays are more than the maximum.
	// More doublings can overflow time.Duration.
	doublings := min(retryNum, 16)
	delay := firstDelay * time.Duration(1<<doublings) // 1<<doublings is 2^doublings

	return min(delay, fetchRetryMaxDelay)
}
