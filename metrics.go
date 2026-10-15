package sqlq

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	// jobTypeAttr names the job type in spans and metrics.
	jobTypeAttr = "sqlq.job_type"
	// resultAttr gives the result of a handler attempt in metrics. Its values are the result constants.
	resultAttr = "sqlq.result"
)

// Results of a handler attempt.
const (
	resultSuccess    = "success"     // The handler succeeded, and the job is processed.
	resultRetry      = "retry"       // The handler failed, and the job is scheduled again.
	resultDeadLetter = "dead_letter" // The handler failed, and the job is in the dead letter queue.
	resultClaimLost  = "claim_lost"  // Another attempt took the job, so this attempt changed nothing.
	resultCanceled   = "canceled"    // Shutdown stopped the attempt. The job is released, or its claim expires.
	resultError      = "error"       // The queue could not write the job's next state. The job is released, or its claim expires.
)

// durationBuckets are the histogram bounds, in seconds: from 5ms to one hour.
var durationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600}

// metrics holds the queue's OpenTelemetry instruments.
type metrics struct {
	published  metric.Int64Counter
	attempts   metric.Int64Counter
	duration   metric.Float64Histogram
	lag        metric.Float64Histogram
	queueDepth metric.Int64ObservableGauge
	queueLag   metric.Float64ObservableGauge
}

func newMetrics(meter metric.Meter) (*metrics, error) {
	var m metrics
	var errs [6]error
	m.published, errs[0] = meter.Int64Counter("sqlq.jobs.published", metric.WithUnit("{job}"),
		metric.WithDescription("Jobs inserted by Publish and PublishTx, also those whose transaction rolls back later."))
	m.attempts, errs[1] = meter.Int64Counter("sqlq.job.attempts", metric.WithUnit("{attempt}"),
		metric.WithDescription("Handler attempts, by result."))
	m.duration, errs[2] = meter.Float64Histogram("sqlq.job.duration", metric.WithUnit("s"),
		metric.WithDescription("Time from the start of a handler until the queue has written the job's next state."),
		metric.WithExplicitBucketBoundaries(durationBuckets...))
	m.lag, errs[3] = meter.Float64Histogram("sqlq.job.lag", metric.WithUnit("s"),
		metric.WithDescription("Time from when a job became due until its handler started."),
		metric.WithExplicitBucketBoundaries(durationBuckets...))
	m.queueDepth, errs[4] = meter.Int64ObservableGauge("sqlq.queue.depth", metric.WithUnit("{job}"),
		metric.WithDescription("Jobs that are due and not claimed, for each job type that the queue consumes."))
	m.queueLag, errs[5] = meter.Float64ObservableGauge("sqlq.queue.lag", metric.WithUnit("s"),
		metric.WithDescription("How long the oldest job that is due and not claimed has been due, for each consumed job type."))
	if err := errors.Join(errs[:]...); err != nil {
		return nil, fmt.Errorf("failed to create metrics: %w", err)
	}
	return &m, nil
}

// recordAttempt records a handler attempt that took d and ended with result.
func (m *metrics) recordAttempt(ctx context.Context, jobType, result string, d time.Duration) {
	attrs := metric.WithAttributes(attribute.String(jobTypeAttr, jobType), attribute.String(resultAttr, result))
	m.attempts.Add(ctx, 1, attrs)
	m.duration.Record(ctx, d.Seconds(), attrs)
}

// recordLag records how long a job waited for its handler to start, after the job became due.
// The database clock gives the due time, so clock skew can make lag negative: it is then recorded as zero.
func (m *metrics) recordLag(ctx context.Context, jobType string, lag time.Duration) {
	m.lag.Record(ctx, max(lag, 0).Seconds(), metric.WithAttributes(attribute.String(jobTypeAttr, jobType)))
}

// setUpMetrics creates the queue's instruments, and registers the callback that reports queue depth and lag.
func (q *sqlq) setUpMetrics() error {
	meter := q.meterProvider.Meter(tracerName)
	m, err := newMetrics(meter)
	if err != nil {
		return err
	}
	q.metrics = m // Before the callback is registered: the callback uses it.
	q.metricsCtx, q.cancelMetrics = context.WithCancel(context.Background())
	q.metricsDone = make(chan struct{})
	q.metricsRegistration, err = meter.RegisterCallback(q.observeQueue, m.queueDepth, m.queueLag)
	if err != nil {
		q.cancelMetrics()
		return fmt.Errorf("failed to register metrics callback: %w", err)
	}
	return nil
}

// observeQueue reports the depth and lag of each job type that the queue consumes.
// OpenTelemetry calls it each time it collects metrics, and each call queries the database once
// for each consumed job type. Each queue that consumes a job type reports the same values.
func (q *sqlq) observeQueue(ctx context.Context, o metric.Observer) error {
	q.consumersMapMutex.RLock()
	jobTypes := slices.Sorted(maps.Keys(q.consumersMap))
	shutDown := q.shutDown
	q.consumersMapMutex.RUnlock()
	if shutDown {
		return nil
	}

	// Preserve the collector's context while also interrupting pool and query
	// waits when the queue shuts down.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopCancelOnShutdown := context.AfterFunc(q.metricsCtx, cancel)
	defer stopCancelOnShutdown()

	var errs []error
	for _, jobType := range jobTypes {
		if q.metricsCtx.Err() != nil {
			return nil
		}
		stats, err := q.driver.getReadyJobStats(ctx, jobType)
		if err != nil {
			if q.metricsCtx.Err() != nil {
				return nil // Shutdown interrupted collection; that is no failure.
			}
			errs = append(errs, fmt.Errorf("failed to read queue depth of %s: %w", jobType, err))
			continue
		}
		attrs := metric.WithAttributes(attribute.String(jobTypeAttr, jobType))
		o.ObserveInt64(q.metrics.queueDepth, stats.count, attrs)
		o.ObserveFloat64(q.metrics.queueLag, stats.lag.Seconds(), attrs)
	}
	return errors.Join(errs...)
}
