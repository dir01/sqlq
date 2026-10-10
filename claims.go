package sqlq

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"
)

// SQLite's clock is shared by all connections/processes. Local deadlines use the
// request start time instead, so network/lock wait never adds to the local budget.
const sqliteNow = "CAST(ROUND((julianday('now') - 2440587.5) * 86400000) AS INTEGER)"

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

func (d *SQLiteDriver) extendClaim(ctx context.Context, j job, timeout time.Duration) (time.Time, error) {
	deadline := localClaimDeadline(timeout)
	d.dbMutex.Lock()
	defer d.dbMutex.Unlock()
	result, err := d.db.ExecContext(ctx, `UPDATE jobs SET claim_expires_at = `+sqliteNow+` + ?
		WHERE id = ? AND claim_token = ? AND processed_at IS NULL AND claim_expires_at > `+sqliteNow,
		timeout.Milliseconds(), j.ID, j.ClaimToken)
	return deadline, checkClaimResult(result, err)
}

func (d *PostgresDriver) extendClaim(ctx context.Context, j job, timeout time.Duration) (time.Time, error) {
	deadline := localClaimDeadline(timeout)
	result, err := d.db.ExecContext(ctx, `UPDATE jobs SET claim_expires_at = clock_timestamp() + $1 * interval '1 millisecond'
		WHERE id = $2 AND claim_token = $3 AND processed_at IS NULL AND claim_expires_at > clock_timestamp()`,
		timeout.Milliseconds(), j.ID, j.ClaimToken)
	return deadline, checkClaimResult(result, err)
}

func (d *SQLiteDriver) releaseClaim(ctx context.Context, j job) error {
	// Use the database's locking rather than dbMutex so the cleanup context can
	// bound waiting even when another consumer is inside a dead-letter hook.
	result, err := d.db.ExecContext(ctx, `UPDATE jobs SET consumed_at = NULL, claim_token = NULL, claim_expires_at = NULL
		WHERE id = ? AND claim_token = ? AND processed_at IS NULL`, j.ID, j.ClaimToken)
	return checkClaimResult(result, err)
}

func (d *PostgresDriver) releaseClaim(ctx context.Context, j job) error {
	result, err := d.db.ExecContext(ctx, `UPDATE jobs SET consumed_at = NULL, claim_token = NULL, claim_expires_at = NULL
		WHERE id = $1 AND claim_token = $2 AND processed_at IS NULL`, j.ID, j.ClaimToken)
	return checkClaimResult(result, err)
}
