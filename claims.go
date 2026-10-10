package sqlq

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"
)

func claimKey(j job) string {
	return fmt.Sprintf("%d/%s", j.ID, j.ClaimToken)
}

// claimBudget is how much of a claim timeout a worker can rely on locally.
// SQL stores millisecond durations. Allow for timestamp quantization too.
func claimBudget(timeout time.Duration) time.Duration {
	return timeout.Truncate(time.Millisecond) - time.Millisecond
}

func localClaimDeadline(timeout time.Duration) time.Time {
	return time.Now().Add(claimBudget(timeout))
}

func newClaimToken() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate claim token: %w", err)
	}
	return hex.EncodeToString(bytes[:]), nil
}

func checkClaimResult(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrClaimLost
	}
	return nil
}
