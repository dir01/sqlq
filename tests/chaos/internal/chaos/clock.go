package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// ClockProbe checks monotonic timer elapsed time against wall/database time.
// The host launcher measures actual acceleration separately.
func ClockProbe(ctx context.Context, c Config, duration time.Duration, out io.Writer) error {
	if c.Driver == "sqlite" && c.DSN == "" {
		c.DSN = ":memory:"
	}
	db, err := openDB(c, false)
	if err != nil {
		return err
	}
	defer db.Close()
	query := `SELECT CAST(ROUND((julianday('now')-2440587.5)*86400000) AS INTEGER)`
	if c.Driver == "postgres" {
		query = `SELECT CAST(EXTRACT(EPOCH FROM clock_timestamp())*1000 AS BIGINT)`
	}
	var before, after int64
	if err := db.QueryRowContext(ctx, query).Scan(&before); err != nil {
		return err
	}
	start := time.Now()
	if err := json.NewEncoder(out).Encode(map[string]any{"clock_probe": "start", "driver": c.Driver, "duration_ns": duration.Nanoseconds()}); err != nil {
		return err
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return ctx.Err()
	}
	elapsed := time.Since(start)
	if err := db.QueryRowContext(ctx, query).Scan(&after); err != nil {
		return err
	}
	dbElapsed := time.Duration(after-before) * time.Millisecond
	wallElapsed := time.Duration(time.Now().UnixNano() - start.UnixNano())
	// Include scheduling/query overhead, but reject clock-domain mismatches.
	tolerance := max(100*time.Millisecond, elapsed/5)
	if (dbElapsed-elapsed).Abs() > tolerance || (wallElapsed-elapsed).Abs() > tolerance {
		return fmt.Errorf("incoherent clocks: monotonic=%s wall=%s database=%s tolerance=%s", elapsed, wallElapsed, dbElapsed, tolerance)
	}
	return json.NewEncoder(out).Encode(map[string]any{"clock_probe": "end", "driver": c.Driver, "monotonic_ns": elapsed.Nanoseconds(), "wall_ns": wallElapsed.Nanoseconds(), "database_ns": dbElapsed.Nanoseconds()})
}
