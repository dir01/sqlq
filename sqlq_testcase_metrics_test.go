package sqlq_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dir01/sqlq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func (tc *TestCase) TestMetrics(ctx context.Context, t *testing.T) {
	t.Helper()

	// The queue is shared with other tests, so every check is narrowed to this job type.
	jobType := "metrics_job"
	forJob := attribute.String("sqlq.job_type", jobType)

	// "ok" succeeds, "flaky" fails once and then succeeds, and "broken" fails both of its
	// attempts and is dead-lettered. That is 5 attempts, 3 of which fail.
	err := tc.Q.Consume(ctx, jobType, func(ctx context.Context, _ *sql.Tx, payloadBytes []byte) error {
		var payload TestPayload
		if err := json.Unmarshal(payloadBytes, &payload); err != nil {
			return err
		}

		info, _ := sqlq.JobInfoFromContext(ctx)
		if payload.Message == "broken" || (payload.Message == "flaky" && info.RetryCount == 0) {
			return errors.New("simulated failure")
		}

		return nil
	}, sqlq.WithConsumerMaxRetries(1))
	require.NoError(t, err)

	for _, message := range []string{"ok", "flaky", "broken"} {
		require.NoError(t, tc.Q.Publish(ctx, jobType, TestPayload{Message: message}))
	}
	require.Error(t, tc.Q.Publish(ctx, jobType, make(chan int)), "A channel can't be marshaled to JSON")

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		rm := collectMetrics(c, tc.Metrics)

		assert.Equal(c, int64(3), sumMetric(rm, "sqlq.jobs.published.count", forJob))
		assert.Equal(c, int64(1), sumMetric(rm, "sqlq.jobs.published.errors.count", forJob))
		assert.Equal(c, int64(5), sumMetric(rm, "sqlq.jobs.fetched.count", forJob))
		assert.Equal(c, int64(5), sumMetric(rm, "sqlq.jobs.consumed.count", forJob))
		assert.Equal(c, int64(2), sumMetric(rm, "sqlq.jobs.consumed.count", forJob, attribute.String("sqlq.failure_reason", "")))
		assert.Equal(c, int64(3), sumMetric(rm, "sqlq.jobs.consumed.count", forJob, attribute.String("sqlq.failure_reason", "fail_handle_job")))
		assert.Equal(c, int64(5), sumMetric(rm, "sqlq.jobs.handler.duration", forJob))
		assert.Equal(c, int64(2), sumMetric(rm, "sqlq.jobs.retried.count", forJob))
		assert.Equal(c, int64(1), sumMetric(rm, "sqlq.jobs.dlq.moved.count", forJob))
		assert.Equal(c, int64(0), sumMetric(rm, "sqlq.jobs.active.count", forJob))
	}, 5*time.Second, 25*time.Millisecond)

	dlqJobs, err := tc.Q.GetDeadLetterJobs(ctx, jobType, 10)
	require.NoError(t, err)
	require.Len(t, dlqJobs, 1)
	require.Equal(t, int64(1), sumMetric(collectMetrics(t, tc.Metrics), "sqlq.jobs.dlq.fetched.count", forJob))

	require.NoError(t, tc.Q.RequeueDeadLetterJob(ctx, dlqJobs[0].OriginalID))
	require.Equal(t, int64(1), sumMetric(collectMetrics(t, tc.Metrics), "sqlq.jobs.dlq.requeued.count", forJob))
}

// collectMetrics reads everything the queue has recorded so far.
func collectMetrics(t require.TestingT, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	return rm
}

// sumMetric adds up the data points of the named instrument that carry all of attrs.
// For a histogram, it adds up how many values were recorded.
func sumMetric(rm metricdata.ResourceMetrics, name string, attrs ...attribute.KeyValue) int64 {
	matches := func(set attribute.Set) bool {
		for _, kv := range attrs {
			if v, ok := set.Value(kv.Key); !ok || v != kv.Value {
				return false
			}
		}

		return true
	}

	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}

			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					if matches(dp.Attributes) {
						total += dp.Value
					}
				}
			case metricdata.Histogram[int64]:
				for _, dp := range data.DataPoints {
					if matches(dp.Attributes) {
						total += int64(dp.Count)
					}
				}
			}
		}
	}

	return total
}
