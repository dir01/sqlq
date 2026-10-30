package sqlq

type tokenBucket struct {
	tokensCurr         *uint16
	lastCheckUnixMilli int64
	// The part of a token that is earned but not added, in units of 1/60000 token.
	partialToken int64

	RPM      uint16
	Capacity uint16
}

func (b *tokenBucket) SpendToken(currUnixMilli int64) bool {
	var tokensCurr uint16

	if b.tokensCurr != nil {
		tokensCurr = *b.tokensCurr
	} else {
		tokensCurr = b.Capacity
	}

	// maybe add tokens. Use 64 bits: RPM times the elapsed milliseconds overflows 16 bits.
	// The remaining part of a token goes to the next call.
	// Thus, frequent calls get the full rate, but not more.
	if passedMilli := currUnixMilli - b.lastCheckUnixMilli; passedMilli > 0 {
		earned := b.partialToken + int64(b.RPM)*passedMilli
		toAdd := earned / 60000
		b.partialToken = earned % 60000
		b.lastCheckUnixMilli = currUnixMilli

		// but not more than Capacity
		tokensCurr = uint16(min(int64(tokensCurr)+toAdd, int64(b.Capacity)))
	}

	// deny spending if at zero
	if tokensCurr == 0 {
		return false
	}

	// spend token
	tokensCurr--
	b.tokensCurr = &tokensCurr

	return true
}
