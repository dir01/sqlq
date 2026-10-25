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
	// Most fetch errors occur because of a short lock, and one fast retry is usually successful.
	fetchRetryMaxFirstDelay = 100 * time.Millisecond
	// The maximum delay between retries of a failed fetch, if the poll interval is shorter.
	fetchRetryMaxDelay = 30 * time.Second
)

// fetchRetryBackoff returns the delay of the fetch loop after retryNum + 1 failed fetches in sequence.
//
// The first delay is the poll interval, but not more than fetchRetryMaxFirstDelay.
// If the failures continue, the database is possibly not available. Then the delay is the
// poll interval, doubled one time for each further failure, to a maximum of fetchRetryMaxDelay
// or the poll interval, whichever is longer. Thus, after the first retry, a failure never
// makes the consumer fetch more frequently than its poll interval.
func fetchRetryBackoff(retryNum uint16, pollInterval time.Duration) time.Duration {
	delay := max(pollInterval, time.Millisecond) // a zero delay does not increase when it doubles
	if retryNum == 0 {
		return min(delay, fetchRetryMaxFirstDelay)
	}

	maxDelay := max(fetchRetryMaxDelay, delay)
	for range retryNum {
		if delay > maxDelay/2 { // doubling would make the delay more than maxDelay
			return maxDelay
		}
		delay *= 2
	}
	return delay
}
