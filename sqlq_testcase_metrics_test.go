package sqlq_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestMetrics checks the queue's metrics: published jobs, attempts by result, attempt durations,
// job lags, and the depth and lag of the queue. It uses a queue of its own on tc's database.
func (tc *TestCase) TestMetrics(ctx context.Context, t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	reader := sdkmetric.NewManualReader()
	q, err := sqlq.New(tc.DB, tc.DBType,
		sqlq.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))),
		sqlq.WithDefaultPollInterval(10*time.Millisecond),
		sqlq.WithDefaultMaxRetries(0),
		sqlq.WithDefaultCleanupProcessedInterval(0),
		sqlq.WithDefaultCleanupDLQInterval(0),
	)
	require.NoError(t, err)
	require.NoError(t, q.Run(ctx))
	defer func() { require.NoError(t, q.Shutdown(ctx)) }()

	release := make(chan struct{})
	succeed := func(context.Context, *sql.Tx, []byte) error { return nil }
	fail := func(context.Context, *sql.Tx, []byte) error { return errors.New("simulated failure") }
	block := func(ctx context.Context, _ *sql.Tx, _ []byte) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	require.NoError(t, q.Consume(ctx, "metrics_ok", succeed))
	require.NoError(t, q.Consume(ctx, "metrics_failed", fail))
	// One worker and a buffer of one: two blocked jobs are claimed, and the others are ready.
	require.NoError(t, q.Consume(ctx, "metrics_blocked", block,
		sqlq.WithConsumerConcurrency(1), sqlq.WithConsumerPrefetchCount(1)))

	for jobType, count := range map[string]int{"metrics_ok": 2, "metrics_failed": 1, "metrics_blocked": 5} {
		for range count {
			require.NoError(t, q.Publish(ctx, jobType, TestPayload{Message: "metrics"}))
		}
	}

	jobType := func(name string) attribute.KeyValue { return attribute.String("sqlq.job_type", name) }
	result := func(name string) attribute.KeyValue { return attribute.String("sqlq.result", name) }
	var rm metricdata.ResourceMetrics
	require.Eventually(t, func() bool {
		return reader.Collect(ctx, &rm) == nil &&
			metricValue(rm, "sqlq.job.attempts", jobType("metrics_ok"), result("success")) == 2 &&
			metricValue(rm, "sqlq.job.attempts", jobType("metrics_failed"), result("dead_letter")) == 1 &&
			metricValue(rm, "sqlq.queue.depth", jobType("metrics_blocked")) == 3
	}, 10*time.Second, 20*time.Millisecond, "attempts and queue depth were not reported")

	require.InDelta(t, 2, metricValue(rm, "sqlq.jobs.published", jobType("metrics_ok")), 0)
	require.InDelta(t, 5, metricValue(rm, "sqlq.jobs.published", jobType("metrics_blocked")), 0)
	require.InDelta(t, 2, metricValue(rm, "sqlq.job.duration", jobType("metrics_ok"), result("success")), 0,
		"the duration histogram must count each attempt")
	require.InDelta(t, 2, metricValue(rm, "sqlq.job.lag", jobType("metrics_ok")), 0,
		"the lag histogram must count each started job")
	require.Positive(t, metricValue(rm, "sqlq.queue.lag", jobType("metrics_blocked")),
		"ready jobs have been due for some time")
	require.Zero(t, metricValue(rm, "sqlq.queue.depth", jobType("metrics_ok")))
	require.Zero(t, metricValue(rm, "sqlq.queue.lag", jobType("metrics_ok")))

	close(release)
	require.Eventually(t, func() bool {
		return reader.Collect(ctx, &rm) == nil &&
			metricValue(rm, "sqlq.job.attempts", jobType("metrics_blocked"), result("success")) == 5 &&
			metricValue(rm, "sqlq.queue.depth", jobType("metrics_blocked")) == 0
	}, 10*time.Second, 20*time.Millisecond, "blocked jobs did not finish")
}

// metricValue returns the value of the named metric's data point with exactly attrs: the value of
// a sum or a gauge, or the count of a histogram. It returns -1 if there is no such data point.
func metricValue(rm metricdata.ResourceMetrics, name string, attrs ...attribute.KeyValue) float64 {
	want := attribute.NewSet(attrs...)
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				return pointValue(data.DataPoints, want)
			case metricdata.Gauge[int64]:
				return pointValue(data.DataPoints, want)
			case metricdata.Gauge[float64]:
				return pointValue(data.DataPoints, want)
			case metricdata.Histogram[float64]:
				for _, point := range data.DataPoints {
					if point.Attributes.Equals(&want) {
						return float64(point.Count)
					}
				}
			}
		}
	}
	return -1
}

func pointValue[N int64 | float64](points []metricdata.DataPoint[N], want attribute.Set) float64 {
	for _, point := range points {
		if point.Attributes.Equals(&want) {
			return float64(point.Value)
		}
	}
	return -1
}
