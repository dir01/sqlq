package sqlq

type tokenBucket struct {
	tokensCurr         *uint16
	lastCheckUnixMilli int64
	// The part of a token earned but not yet added, in sixty-thousandths of a token.
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

	// maybe add tokens. Count in 64 bits: RPM times the elapsed milliseconds overflows 16.
	// The fraction of a token left over carries to the next call, so that calls coming
	// faster than tokens are earned still add up to the full rate, and no faster.
	if passedMilli := currUnixMilli - b.lastCheckUnixMilli; passedMilli > 0 {
		earned := b.partialToken + int64(b.RPM)*passedMilli
		toAdd := earned / 60000
		b.partialToken = earned % 60000
		b.lastCheckUnixMilli = currUnixMilli

		// but not past Capacity
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
