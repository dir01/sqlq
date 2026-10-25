package sqlq

import (
	"math"
	"math/rand/v2"
	"time"
)

// defaultPollDelay is the default poll function. emptyPolls is the number of polls since
// the last poll that found jobs. After a poll that found jobs, the delay is 100ms. Each poll
// that finds no jobs makes the delay 10% longer, to a maximum of 30s. Thus, an idle consumer
// sends few queries, and a busy consumer gets new jobs quickly.
//
// The function adds a random jitter of up to 20%, so that idle consumers do not poll at the same time.
// For a different behavior, set a different function with WithDefaultPollFunc or WithConsumerPollFunc.
func defaultPollDelay(emptyPolls uint16) time.Duration {
	delay := time.Duration(min(
		float64(100*time.Millisecond)*math.Pow(1.1, float64(emptyPolls)),
		float64(30*time.Second),
	))
	return delay + rand.N(delay/5)
}
