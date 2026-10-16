package chaos

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type spanStats struct {
	Count   int            `json:"count"`
	Errors  int            `json:"errors"`
	Total   time.Duration  `json:"total_ns"`
	Max     time.Duration  `json:"max_ns"`
	Reasons map[string]int `json:"error_samples,omitempty"`
}

// diagnostics collects bounded, cumulative queue/driver span statistics. Unlike
// an external exporter it introduces no network traffic or background queue.
type diagnostics struct {
	mu    sync.Mutex
	spans map[string]*spanStats
}

func (d *diagnostics) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (d *diagnostics) Shutdown(context.Context) error                  { return nil }
func (d *diagnostics) ForceFlush(context.Context) error                { return nil }
func (d *diagnostics) OnEnd(span sdktrace.ReadOnlySpan) {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.spans[span.Name()]
	if s == nil {
		s = &spanStats{Reasons: make(map[string]int)}
		d.spans[span.Name()] = s
	}
	s.Count++
	elapsed := span.EndTime().Sub(span.StartTime())
	s.Total += elapsed
	s.Max = max(s.Max, elapsed)
	if span.Status().Code == codes.Error {
		s.Errors++
	}
	for _, event := range span.Events() {
		if event.Name != "exception" {
			continue
		}
		for _, attr := range event.Attributes {
			if attr.Key != "exception.message" {
				continue
			}
			reason := attr.Value.AsString()
			if len(reason) > 300 {
				reason = reason[:300]
			}
			if _, ok := s.Reasons[reason]; ok || len(s.Reasons) < 8 {
				s.Reasons[reason]++
			}
		}
	}
}

func (d *diagnostics) emit(e *emitter, db *sql.DB) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	b, err := json.Marshal(struct {
		Spans      map[string]*spanStats `json:"spans"`
		Pool       sql.DBStats           `json:"pool"`
		Goroutines int                   `json:"goroutines"`
		Heap       uint64                `json:"heap_bytes"`
		GCPause    uint64                `json:"gc_pause_total_ns"`
	}{d.spans, db.Stats(), runtime.NumGoroutine(), memory.HeapAlloc, memory.PauseTotalNs})
	if err != nil {
		e.emit("fatal", -1, err.Error())
		return
	}
	e.emit("worker_metrics", -1, string(b))
}

func diagnosticTracer() (*diagnostics, *sdktrace.TracerProvider, trace.Tracer) {
	d := &diagnostics{spans: make(map[string]*spanStats)}
	p := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(d))
	return d, p, p.Tracer("sqlq.simulation")
}

func backlog(ctx context.Context, c Config, db *sql.DB) (string, error) {
	now := "CAST((julianday('now')-2440587.5)*86400000 AS INTEGER)"
	if c.Driver == "postgres" {
		now = "clock_timestamp()"
	}
	var pending, delayed, claimed, expired, ready int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*),
		COALESCE(SUM(CASE WHEN scheduled_at > `+now+` THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN claim_expires_at > `+now+` THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN claim_expires_at <= `+now+` THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN scheduled_at <= `+now+` AND (claim_expires_at IS NULL OR claim_expires_at <= `+now+`) THEN 1 ELSE 0 END),0)
		FROM jobs WHERE processed_at IS NULL`).Scan(&pending, &delayed, &claimed, &expired, &ready)
	return fmt.Sprintf("pending=%d ready=%d delayed=%d claimed=%d expired_claims=%d pool=%+v", pending, ready, delayed, claimed, expired, db.Stats()), err
}
