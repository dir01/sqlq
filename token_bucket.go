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

	// maybe add tokens
	passedMilli := currUnixMilli - b.lastCheckUnixMilli
	toAdd := b.RPM * uint16(passedMilli) / 60000
	if toAdd > 0 {
		tokensCurr += toAdd
		// only update if tokens were added
		// otherwise failed spending will interfere with refill
		b.lastCheckUnixMilli = currUnixMilli
	}

	// but not past Capacity
	if tokensCurr > b.Capacity {
		tokensCurr = b.Capacity
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
