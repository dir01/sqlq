package sqlq

import (
	"math"
	"math/rand/v2"
	"time"
)

// exponentialBackoff is the default backoff function. The delay is one second, doubled one time
// for each retry, plus a random jitter of up to 20%, to a maximum of 24 hours. The jitter prevents
// a retry of all failed jobs at the same time. For a different behavior, also a different maximum,
// set a different function with WithDefaultBackoffFunc or WithConsumerBackoffFunc.
func exponentialBackoff(retryNum uint16) time.Duration {
	maxDelay := 24 * time.Hour
	delay := time.Second
	for range retryNum {
		if delay > maxDelay/2 { // doubling would make the delay more than maxDelay
			return maxDelay
		}
		delay *= 2
	}

	jitter := rand.N(delay / 5)
	return min(delay+jitter, maxDelay)
}

// fetchRetryBackoff returns the delay of the fetch loop after retryNum + 1 failed fetches in sequence.
//
// The first delay is the poll delay, but not more than 100ms. Most fetch errors occur
// because of a short lock, and one fast retry is usually successful.
// If the failures continue, the database is possibly not available. Then the delay is the
// poll delay, doubled one time for each further failure, to a maximum of 30s or the poll
// delay, whichever is longer. Thus, after the first retry, a failure never makes the
// consumer fetch more frequently than its poll delay function allows.
//
// The function adds a random jitter of up to 20% to the delay. When the database stops,
// all consumers fail at almost the same time. Without the jitter, they would also retry
// at the same time, and the database would get all their queries at once when it starts again.
func fetchRetryBackoff(retryNum uint16, pollDelay time.Duration) time.Duration {
	delay := max(pollDelay, time.Millisecond) // a zero delay does not increase when it doubles
	if retryNum == 0 {
		delay = min(delay, 100*time.Millisecond)
	} else {
		maxDelay := max(30*time.Second, delay)
		for range retryNum {
			if delay > maxDelay/2 { // doubling would make the delay more than maxDelay
				delay = maxDelay
				break
			}
			delay *= 2
		}
	}

	jitter := rand.N(delay / 5)
	return delay + min(jitter, math.MaxInt64-delay) // the sum must not overflow time.Duration
}
