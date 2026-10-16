package sqlq

type tokenBucket struct {
	tokensCurr         *uint16
	lastCheckUnixMilli int64

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

	// maybe add tokens, in 64 bits: RPM times the elapsed milliseconds overflows 16
	passedMilli := max(currUnixMilli-b.lastCheckUnixMilli, 0)
	if toAdd := int64(b.RPM) * passedMilli / 60000; toAdd > 0 {
		// but not past Capacity
		tokensCurr = uint16(min(int64(tokensCurr)+toAdd, int64(b.Capacity)))
		// Only the time the added tokens took is used up, so a fraction of the next carries over.
		// Without that, failed spends would interfere with refill.
		b.lastCheckUnixMilli += toAdd * 60000 / int64(b.RPM)
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
