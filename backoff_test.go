package sqlq

import (
	"math"
	"testing"
	"time"
)

func TestExponentialBackoff(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		retryNum uint16
		minExp   time.Duration
		maxExp   time.Duration
	}{
		{
			name:     "retry 0",
			retryNum: 0,
			minExp:   time.Duration(math.Pow(2, 0)) * time.Second, // 1s
			// Adjust maxExp slightly for the zero case to make the range check work.
			// We expect exactly 1s, so the range should be [1s, 1s + epsilon).
			maxExp: time.Duration(math.Pow(2, 0))*time.Second + time.Nanosecond,
		},
		{
			name:     "retry 1",
			retryNum: 1,
			minExp:   time.Duration(math.Pow(2, 1)) * time.Second,   // 2s
			maxExp:   time.Duration(math.Pow(2, 1)+1) * time.Second, // 2s + 1s jitter = 3s
		},
		{
			name:     "retry 5",
			retryNum: 5,
			minExp:   time.Duration(math.Pow(2, 5)) * time.Second,   // 32s
			maxExp:   time.Duration(math.Pow(2, 5)+5) * time.Second, // 32s + 5s jitter = 37s
		},
		{
			name:     "retry 10",
			retryNum: 10,
			minExp:   time.Duration(math.Pow(2, 10)) * time.Second,    // 1024s
			maxExp:   time.Duration(math.Pow(2, 10)+10) * time.Second, // 1024s + 10s jitter = 1034s
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Run multiple times to account for randomness
			for range 100 {
				duration := exponentialBackoff(tc.retryNum)
				if duration < tc.minExp || duration >= tc.maxExp {
					t.Errorf("exponentialBackoff(%d) = %v; want between [%v, %v)",
						tc.retryNum, duration, tc.minExp, tc.maxExp)
				}
			}
		})
	}
}
