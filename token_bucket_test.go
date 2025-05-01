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
}
