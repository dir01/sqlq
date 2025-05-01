package sqlq

import (
	"math"
	"math/rand"
	"time"
)

func exponentialBackoff(retryNum uint16) time.Duration {
	var jitter float64
	if retryNum > 0 {
		// Add jitter: up to retryNum seconds
		jitter = float64(rand.Intn(int(retryNum)))
	}
	backoff := math.Pow(2, float64(retryNum))
	return time.Duration(backoff+jitter) * time.Second
}
