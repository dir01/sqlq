package sqlq

import (
	"log/slog"
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

// WithMeterProvider sets the OpenTelemetry meter provider for the queue's metrics.
// The default is the global meter provider. A nil provider is ignored.
func WithMeterProvider(provider metric.MeterProvider) NewOption {
	return func(q *sqlq) {
		if provider != nil {
			q.meterProvider = provider
		}
	}
}

// WithLogger sets the logger for errors in the queue's background work, such as failed polls,
// claim releases, and cleanups. The default is slog.Default() when New is called.
// A nil logger is ignored. To discard the logs, use slog.New(slog.DiscardHandler).
func WithLogger(logger *slog.Logger) NewOption {
	return func(q *sqlq) {
		if logger != nil {
			q.logger = logger
		}
	}
}

//////////////////////////////////////////
// Default values for per-consumer options
//////////////////////////////////////////

// WithDefaultPollInterval allows to configure default poll interval.
// A zero or negative interval is ignored, as it is by WithConsumerPollInterval.
// This value may be overridden per individual consumer, see WithConsumerPollInterval.
func WithDefaultPollInterval(tick time.Duration) NewOption {
	return func(q *sqlq) {
		if tick > 0 {
			q.defaultPollInterval = tick
		}
	}
}

// WithDefaultBackoffFunc allows to configure default backoff function.
// The retry argument saturates at 65535; JobInfo.RetryCount retains the wider count.
// If backoffFunc panics, the panic is logged and the retry uses the built-in exponential backoff
// with jitter, capped at 24 hours.
// Consume returns ErrNilBackoffFunc if backoffFunc is nil and the consumer does not set its own.
// This value may be overridden per individual consumer, see WithConsumerBackoffFunc.
func WithDefaultBackoffFunc(backoffFunc func(uint16) time.Duration) NewOption {
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
// Default is 500.
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
// A timeout is treated as a job failure.
// This value may be overridden per individual consumer, see WithConsumerJobTimeout.
func WithDefaultJobTimeout(timeout time.Duration) NewOption {
	return func(o *sqlq) {
		if timeout > 0 {
			o.defaultJobTimeout = timeout
		}
	}
}

// WithDefaultClaimTimeout sets the claim timeout for all consumers.
// Default is twice the job timeout of each consumer. See WithConsumerClaimTimeout for details.
func WithDefaultClaimTimeout(timeout time.Duration) NewOption {
	return func(q *sqlq) {
		if claimBudget(timeout) > 0 {
			q.defaultClaimTimeout = timeout
		}
	}
}
