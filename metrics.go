package sqlq

import (
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/metric"
)

const meterName = tracerName

// sqlqMetrics holds the metric instruments.
type sqlqMetrics struct {
	publishedCounter      metric.Int64Counter
	publishedErrCounter   metric.Int64Counter
	consumedCounter       metric.Int64Counter
	handlerDurationHist   metric.Int64Histogram
	retriedCounter        metric.Int64Counter
	dlqMovedCounter       metric.Int64Counter
	dlqRequeuedCounter    metric.Int64Counter
	dlqFetchedCounter     metric.Int64Counter
	fetchedCounter        metric.Int64Counter
	cleanupProcessedCount metric.Int64Counter
	cleanupDLQCount       metric.Int64Counter
	activeJobsUpDown      metric.Int64UpDownCounter
}

// newMetrics initializes all metric instruments.
func newMetrics(meter metric.Meter) (*sqlqMetrics, error) {
	var errs error
	var err error
	m := &sqlqMetrics{} //nolint:exhaustruct

	m.publishedCounter, err = meter.Int64Counter("sqlq.jobs.published.count", metric.WithDescription("Number of jobs published"), metric.WithUnit("{job}"))
	errs = errors.Join(errs, err)
	m.publishedErrCounter, err = meter.Int64Counter("sqlq.jobs.published.errors.count", metric.WithDescription("Number of failed attempts to publish jobs"), metric.WithUnit("{job}"))
	errs = errors.Join(errs, err)
	m.consumedCounter, err = meter.Int64Counter("sqlq.jobs.consumed.count", metric.WithDescription("Number of jobs consumed"), metric.WithUnit("{job}"))
	errs = errors.Join(errs, err)
	m.handlerDurationHist, err = meter.Int64Histogram("sqlq.jobs.handler.duration", metric.WithDescription("Duration of job handler execution"), metric.WithUnit("ms"))
	errs = errors.Join(errs, err)
	m.retriedCounter, err = meter.Int64Counter("sqlq.jobs.retried.count", metric.WithDescription("Number of job retries"), metric.WithUnit("{job}"))
	errs = errors.Join(errs, err)
	m.dlqMovedCounter, err = meter.Int64Counter("sqlq.jobs.dlq.moved.count", metric.WithDescription("Number of jobs moved to DLQ"), metric.WithUnit("{job}"))
	errs = errors.Join(errs, err)
	m.dlqRequeuedCounter, err = meter.Int64Counter("sqlq.jobs.dlq.requeued.count", metric.WithDescription("Number of DLQ jobs attempted to be requeued"), metric.WithUnit("{job}"))
	errs = errors.Join(errs, err)
	m.dlqFetchedCounter, err = meter.Int64Counter("sqlq.jobs.dlq.fetched.count", metric.WithDescription("Number of DLQ jobs fetched"), metric.WithUnit("{job}"))
	errs = errors.Join(errs, err)
	m.fetchedCounter, err = meter.Int64Counter("sqlq.jobs.fetched.count", metric.WithDescription("Number of jobs fetched by consumer poller"), metric.WithUnit("{job}"))
	errs = errors.Join(errs, err)
	m.cleanupProcessedCount, err = meter.Int64Counter("sqlq.jobs.cleanup.processed.count", metric.WithDescription("Number of processed jobs cleaned up"), metric.WithUnit("{job}"))
	errs = errors.Join(errs, err)
	m.cleanupDLQCount, err = meter.Int64Counter("sqlq.jobs.cleanup.dlq.count", metric.WithDescription("Number of DLQ jobs cleaned up"), metric.WithUnit("{job}"))
	errs = errors.Join(errs, err)
	m.activeJobsUpDown, err = meter.Int64UpDownCounter("sqlq.jobs.active.count", metric.WithDescription("Number of jobs currently being processed"), metric.WithUnit("{job}"))
	errs = errors.Join(errs, err)

	if errs != nil {
		return nil, fmt.Errorf("failed to initialize metrics: %w", errs)
	}

	return m, nil
}
