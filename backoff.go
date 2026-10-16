package sqlq

import (
	"math/rand"
	"time"
)

func exponentialBackoff(retryNum uint16) time.Duration {
	// Bound the exponent before shifting/converting. Unlimited retries must
	// never overflow time.Duration into a negative or zero retry delay.
	const maximum = 24 * time.Hour
	if retryNum >= 17 {
		return maximum
	}
	var jitter time.Duration
	if retryNum > 0 {
		// Add jitter: up to retryNum seconds
		jitter = time.Duration(rand.Intn(int(retryNum))) * time.Second
	}
	return min(time.Duration(uint64(1)<<retryNum)*time.Second+jitter, maximum)
}

// Claim fetch failures often represent short lock contention, not a database
// outage. Start near the polling interval, then back off exponentially for a
// persistent failure. Bound both the shift and delay so recovery stays possible.
func fetchRetryBackoff(retryNum uint16, poll time.Duration) time.Duration {
	base := min(max(poll, time.Millisecond), 100*time.Millisecond)
	return min(base*time.Duration(uint64(1)<<min(retryNum, 16)), 30*time.Second)
}
