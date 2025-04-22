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
func WithDefaultBackoffFunc(backoffFunc func(int) time.Duration) NewOption {
	return func(q *sqlq) {
		q.defaultBackoffFunc = backoffFunc
	}
}

// WithDefaultConcurrency sets default number of concurrent workers for a new consumer.
// This value may be overriden per individual consumer, see WithConsumerConcurrency.
func WithDefaultConcurrency(n int) NewOption {
	return func(o *sqlq) {
		if n > 0 {
			o.defaultConcurrency = n
		}
	}
}

// WithDefaultPrefetchCount sets default number of jobs to prefetch in a single query for a new consumer.
// This value may be overriden per individual consumer, see WithConsumerPrefetchCount.
func WithDefaultPrefetchCount(prefetchCount int) NewOption {
	return func(o *sqlq) {
		if prefetchCount > 0 {
			o.defaultPrefetchCount = prefetchCount
		}
	}
}

// WithConsumerMaxRetries sets default maximum number of job retries for a new consumer.
// Use -1 for infinite retries.
// This value may be overriden per individual consumer, see WithConsumerMaxRetries.
func WithDefaultMaxRetries(n int) NewOption {
	return func(o *sqlq) {
		if n >= -1 {
			o.defaultMaxRetries = n
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
