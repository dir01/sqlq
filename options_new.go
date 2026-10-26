package sqlq

import (
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// NewOption is an option that is accepted by sqlq.New function
type NewOption func(*sqlq)

// WithTracer allows setting opentelemetry tracer
func WithTracer(tracer trace.Tracer) NewOption {
	return func(q *sqlq) {
		q.tracer = tracer
	}
}

// WithMeterProvider allows setting the OpenTelemetry MeterProvider.
// If not set, the global MeterProvider will be used.
func WithMeterProvider(provider metric.MeterProvider) NewOption {
	return func(q *sqlq) {
		q.meterProvider = provider
	}
}

//////////////////////////////////////////
// Default values for per-consumer options
//////////////////////////////////////////

// WithDefaultPollDelayFunc sets the function that calculates the delay to the next poll for new jobs.
// emptyPolls is the number of polls since the last poll that found jobs. It stops at 65535.
// The default starts at 100ms and becomes 10% longer after each poll that finds no jobs,
// to a maximum of 30s. For a fixed interval, use func(emptyPolls uint16) time.Duration { return 100 * time.Millisecond }.
// A zero or negative delay polls again immediately. The option ignores nil.
// This value may be overridden per individual consumer, see WithConsumerPollDelayFunc.
func WithDefaultPollDelayFunc(pollDelayFunc func(emptyPolls uint16) time.Duration) NewOption {
	return func(q *sqlq) {
		if pollDelayFunc != nil {
			q.defaultPollDelayFunc = pollDelayFunc
		}
	}
}

// WithDefaultBackoffFunc allows to configure default backoff function.
// The retry argument stops at 65535. JobInfo.RetryCount keeps the full count.
// This value may be overridden per individual consumer, see WithConsumerBackoffFunc.
func WithDefaultBackoffFunc(backoffFunc func(retryNum uint16) time.Duration) NewOption {
	return func(q *sqlq) {
		q.defaultBackoffFunc = backoffFunc
	}
}

// WithDefaultConcurrency sets default number of concurrent workers for a new consumer.
// This value may be overridden per individual consumer, see WithConsumerConcurrency.
func WithDefaultConcurrency(concurrency uint16) NewOption {
	return func(o *sqlq) {
		if concurrency > 0 {
			o.defaultConcurrency = concurrency
		}
	}
}

//////////////////////////////////////////
// Default values for cleanup options
//////////////////////////////////////////

// WithDefaultCleanupProcessedInterval sets the default interval for cleaning up processed jobs.
// Individual consumers may override this using WithConsumerCleanupProcessedInterval.
func WithDefaultCleanupProcessedInterval(interval time.Duration) NewOption {
	return func(q *sqlq) {
		q.defaultCleanupProcessedInterval = interval
	}
}

// WithDefaultCleanupProcessedAge sets the default maximum age for successfully processed jobs before deletion.
// Individual consumers may override this using WithConsumerCleanupProcessedAge.
func WithDefaultCleanupProcessedAge(age time.Duration) NewOption {
	return func(q *sqlq) {
		if age > 0 {
			q.defaultCleanupProcessedAge = age
		}
	}
}

// WithDefaultCleanupDLQInterval sets the default interval for cleaning up DLQ jobs.
// Individual consumers may override this using WithConsumerCleanupDLQInterval.
func WithDefaultCleanupDLQInterval(interval time.Duration) NewOption {
	return func(q *sqlq) {
		q.defaultCleanupDLQInterval = interval
	}
}

// WithDefaultCleanupDLQAge sets the default maximum age for dead-letter queue jobs before deletion.
// Individual consumers may override this using WithConsumerCleanupDLQAge.
func WithDefaultCleanupDLQAge(age time.Duration) NewOption {
	return func(q *sqlq) {
		if age > 0 {
			q.defaultCleanupDLQAge = age
		}
	}
}

// WithDefaultCleanupBatch sets the default number of jobs to delete in a single cleanup batch.
// Default is 1000.
// This value may be overridden per individual consumer using WithConsumerCleanupBatch.
func WithDefaultCleanupBatch(batchSize uint16) NewOption {
	return func(q *sqlq) {
		if batchSize > 0 {
			q.defaultCleanupBatch = batchSize
		}
	}
}

// WithDefaultPrefetchCount sets default number of jobs to prefetch in a single query for a new consumer.
// By default it equals the concurrency of each consumer, and it is never lower than that.
// This value may be overridden per individual consumer, see WithConsumerPrefetchCount.
func WithDefaultPrefetchCount(prefetchCount uint16) NewOption {
	return func(o *sqlq) {
		if prefetchCount > 0 {
			o.defaultPrefetchCount = prefetchCount
		}
	}
}

// WithDefaultMaxRetries sets the default maximum number of job retries for a new consumer.
// Use -1 for infinite retries.
// This value may be overridden per individual consumer using WithConsumerMaxRetries.
func WithDefaultMaxRetries(maxRetries int32) NewOption {
	return func(o *sqlq) {
		if maxRetries >= infiniteRetries {
			o.defaultMaxRetries = maxRetries
		}
	}
}

// WithDefaultJobTimeout sets a maximum execution duration for a single job handler invocation.
// If the handler exceeds this duration, its context will be canceled.
// A timeout is treated as a job failure. The default is 2 seconds.
// The option ignores a zero or negative timeout, and increases a timeout shorter than 50ms to 50ms.
// This value may be overridden per individual consumer, see WithConsumerJobTimeout.
func WithDefaultJobTimeout(timeout time.Duration) NewOption {
	return func(o *sqlq) {
		if timeout > 0 {
			o.defaultJobTimeout = max(timeout, minJobTimeout)
		}
	}
}
