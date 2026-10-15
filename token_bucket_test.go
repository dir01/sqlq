package sqlq

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTokenBucket(t *testing.T) {
	t.Parallel()

	t.Run("allow burst", func(t *testing.T) {
		t.Parallel()

		b := &tokenBucket{Capacity: 10} //nolint: exhaustruct

		// consume all capacity
		for i := range 10 {
			allow := b.SpendToken(1)
			require.True(t, allow, fmt.Sprintf("iteration %d", i))
		}

		// all capacity is consumed, no more tokens allowed
		allow := b.SpendToken(1)
		require.False(t, allow)
	})

	t.Run("refill over time", func(t *testing.T) {
		t.Parallel()

		b := &tokenBucket{Capacity: 1, RPM: 60} //nolint: exhaustruct

		// consume existing 1 token
		require.True(t, b.SpendToken(0))
		require.False(t, b.SpendToken(0))

		// after 1/60th of a minute, 1/60th of 60 RPM worth of tokens should be added
		require.True(t, b.SpendToken(1000))
	})

	t.Run("no refill past capacity", func(t *testing.T) {
		t.Parallel()

		b := &tokenBucket{Capacity: 1, RPM: 60} //nolint: exhaustruct

		// consume existing token
		require.True(t, b.SpendToken(0))
		require.False(t, b.SpendToken(0))

		// can consume 1 token after whole minute
		require.True(t, b.SpendToken(60000))
		// but not more
		require.False(t, b.SpendToken(60000))
	})

	t.Run("failed spends don't affect refill", func(t *testing.T) {
		t.Parallel()
		// A bug where updating last checked timestamp
		// even though 0 tokens have been added interfered with token refill

		b := &tokenBucket{Capacity: 1, RPM: 60} //nolint: exhaustruct

		// consume existing token
		require.True(t, b.SpendToken(0))
		require.False(t, b.SpendToken(0))

		// can't consume a token before 1000 ms have passed
		require.False(t, b.SpendToken(100))
		require.False(t, b.SpendToken(200))
		require.False(t, b.SpendToken(300))
		require.False(t, b.SpendToken(400))
		require.False(t, b.SpendToken(500))

		// but once 1000 ms have passed, a token becomes available
		require.True(t, b.SpendToken(1000))
		// but only one
		require.False(t, b.SpendToken(1000))
	})

	// RPM times the elapsed milliseconds once overflowed 16 bits, and so did the elapsed
	// milliseconds alone after about a minute: refills came out wrong, mostly zero.
	t.Run("refill without overflow", func(t *testing.T) {
		t.Parallel()

		for _, c := range []struct {
			rpm      uint16
			gapMilli int64
		}{{60, 2000}, {60, 60000}, {600, 1000}, {6000, 1000}, {65535, 100}, {1, 120000}} {
			t.Run(fmt.Sprintf("%d RPM every %dms", c.rpm, c.gapMilli), func(t *testing.T) {
				t.Parallel()

				b := &tokenBucket{Capacity: c.rpm, RPM: c.rpm} //nolint: exhaustruct
				now := int64(1_700_000_000_000)
				for b.SpendToken(now) { // Empty it.
				}
				// Each spend comes later than the rate allows, so each must succeed.
				for i := range int64(30) {
					require.True(t, b.SpendToken(now+(i+1)*c.gapMilli), "spend %d", i)
				}
			})
		}
	})

	t.Run("refill after a long idle period", func(t *testing.T) {
		t.Parallel()

		b := &tokenBucket{Capacity: 60, RPM: 60} //nolint: exhaustruct
		now := int64(1_700_000_000_000)
		for b.SpendToken(now) {
		}
		// A minute refills the whole bucket, and an hour no more than that.
		for _, idle := range []int64{60_000, 3_600_000} {
			now += idle
			spent := 0
			for b.SpendToken(now) {
				spent++
			}
			require.Equal(t, 60, spent, "after %dms idle", idle)
		}
	})

	t.Run("fractions of a token carry over", func(t *testing.T) {
		t.Parallel()

		b := &tokenBucket{Capacity: 1, RPM: 60} //nolint: exhaustruct
		require.True(t, b.SpendToken(0))
		// 1.5s earns one token and half of the next...
		require.True(t, b.SpendToken(1500))
		// ...which the next half second completes.
		require.True(t, b.SpendToken(2000))
		require.False(t, b.SpendToken(2000))
	})
}

func TestAsyncPushRateLimitOption(t *testing.T) {
	t.Parallel()

	// The limit was narrowed to 16 bits: 65536 became 0, no limit at all, and 70000 became 4464.
	for rpm, want := range map[int]uint16{-5: 0, 0: 0, 600: 600, 65535: 65535, 65536: 65535, 70000: 65535} {
		c := &consumer{} //nolint: exhaustruct
		WithAsyncPushRateLimit(rpm)(c)
		require.Equal(t, want, c.asyncPushMaxRPM, "rpm %d", rpm)
	}
}
