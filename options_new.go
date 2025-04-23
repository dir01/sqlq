package sqlq

import (
	"time"

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

//////////////////////////////////////////
// Default values for per-consumer options
//////////////////////////////////////////

// WithDefaultPollInterval allows to configure default poll interval.
// This value may be overriden per individual consumer, see WithConsumerPollInteval.
func WithDefaultPollInterval(tick time.Duration) NewOption {
	return func(q *sqlq) {
		q.defaultPollInterval = tick
	}
}

// WithDefaultBackoffFunc allows to configure default backoff function.
// This value may be overriden per individual consumer, see WithConsumerBackoffFunc.
func WithDefaultBackoffFunc(backoffFunc func(uint16) time.Duration) NewOption {
	return func(q *sqlq) {
		q.defaultBackoffFunc = backoffFunc
	}
}

// WithDefaultConcurrency sets default number of concurrent workers for a new consumer.
// This value may be overriden per individual consumer, see WithConsumerConcurrency.
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

// WithDefaultCleanupInterval sets the default interval at which consumer cleanup processes run.
// Default is 1 hour. Set to 0 or negative to disable cleanup by default.
// This value may be overridden per individual consumer using WithConsumerCleanupInterval.
func WithDefaultCleanupInterval(interval time.Duration) NewOption {
	return func(q *sqlq) {
		q.defaultCleanupInterval = interval
	}
}

// WithDefaultCleanupAge sets the default maximum age for successfully processed jobs before they are deleted.
// Default is 7 days.
// This value may be overridden per individual consumer using WithConsumerCleanupAge.
func WithDefaultCleanupAge(age time.Duration) NewOption {
	return func(q *sqlq) {
		if age > 0 {
			q.defaultCleanupAge = age
		}
	}
}

// WithDefaultCleanupDLQAge sets the default maximum age for dead-letter queue jobs before they are deleted.
// Default is 30 days.
// This value may be overridden per individual consumer using WithConsumerCleanupDLQAge.
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
// This value may be overriden per individual consumer, see WithConsumerPrefetchCount.
func WithDefaultPrefetchCount(prefetchCount uint16) NewOption {
	return func(o *sqlq) {
		if prefetchCount > 0 {
			o.defaultPrefetchCount = prefetchCount
		}
	}
}

// WithConsumerMaxRetries sets default maximum number of job retries for a new consumer.
// Use -1 for infinite retries.
// This value may be overriden per individual consumer, see WithConsumerMaxRetries.
func WithDefaultMaxRetries(maxRetries int32) NewOption {
	return func(o *sqlq) {
		if maxRetries >= infiniteRetries {
			o.defaultMaxRetries = maxRetries
		}
	}
}

// WithDefaultJobTimeout sets a maximum execution duration for a single job handler invocation.
// If the handler exceeds this duration, its context will be canceled.
// A timeout is treated as a job failure.
// This value may be overriden per individual consumer, see WithConsumerJobTimeout.
func WithDefaultJobTimeout(timeout time.Duration) NewOption {
	return func(o *sqlq) {
		if timeout > 0 {
			o.defaultJobTimeout = timeout
		}
	}
}
