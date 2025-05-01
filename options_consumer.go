package sqlq

import "time"

// ConsumerOption defines functional options for Subscribe
type ConsumerOption func(*consumer)

// WithConsumerConcurrency sets the number of concurrent workers for a given consumer
// You may also configure default value for all consumers, see WithDefaultConcurrency.
func WithConsumerConcurrency(concurrency uint16) ConsumerOption {
	return func(o *consumer) {
		if concurrency > 0 {
			o.concurrency = concurrency
		}
	}
}

// WithConsumerCleanupBatch sets the number of jobs to delete in a single cleanup batch for this consumer.
// Default is 1000.
func WithConsumerCleanupBatch(batchSize uint16) ConsumerOption {
	return func(o *consumer) {
		if batchSize > 0 {
			o.cleanupBatch = batchSize
		}
	}
}

// WithConsumerPrefetchCount sets the number of jobs to prefetch in a single query for a given consumer
// You may also configure default value for all consumers, see WithDefaultPrefetchCount.
func WithConsumerPrefetchCount(prefetchCount uint16) ConsumerOption {
	return func(o *consumer) {
		if prefetchCount > 0 {
			o.prefetchCount = prefetchCount
		}
	}
}

// WithConsumerPollInteval sets how frequently to check for new jobs for a given consumer
// You may also configure default value for all consumers, see WithDefaultPollInterval.
func WithConsumerPollInteval(interval time.Duration) ConsumerOption {
	return func(o *consumer) {
		if interval > 0 {
			o.pollInterval = interval
		}
	}
}

// WithConsumerBackoffFunc sets a function to calculate backoff for a given consumer
// You may also configure default value for all consumers, see WithDefaultBackoffFunc.
func WithConsumerBackoffFunc(fn func(uint16) time.Duration) ConsumerOption {
	return func(o *consumer) {
		o.backoffFunc = fn
	}
}

// WithConsumerMaxRetries sets the maximum number of retries for a job.
// Use -1 for infinite retries.
// You may also configure default value for all consumers, see WithDefaultMaxRetries.
func WithConsumerMaxRetries(maxRetries int32) ConsumerOption {
	return func(o *consumer) {
		if maxRetries >= infiniteRetries {
			o.maxRetries = maxRetries
		}
	}
}

// WithConsumerJobTimeout sets a maximum execution duration for a single job handler invocation.
// If the handler exceeds this duration, its context will be canceled.
// A timeout is treated as a job failure.
// You may also configure default value for all consumers, see WithDefaultJobTimeout.
func WithConsumerJobTimeout(timeout time.Duration) ConsumerOption {
	return func(o *consumer) {
		if timeout > 0 { // Only set positive timeouts
			o.jobTimeout = timeout
		}
	}
}

// WithConsumerCleanupProcessedInterval sets the interval for cleaning up old processed jobs for this consumer.
// Setting to 0 or negative disables automatic processed job cleanup.
func WithConsumerCleanupProcessedInterval(interval time.Duration) ConsumerOption {
	return func(o *consumer) {
		o.cleanupProcessedInterval = interval
	}
}

// WithConsumerCleanupProcessedAge sets the maximum age for successfully processed jobs
// before they are eligible for deletion by the cleanup task for this consumer.
func WithConsumerCleanupProcessedAge(age time.Duration) ConsumerOption {
	return func(o *consumer) {
		if age > 0 {
			o.cleanupProcessedAge = age
		}
	}
}

// WithConsumerCleanupDLQInterval sets the interval for cleaning up old DLQ jobs for this consumer.
// Setting to 0 or negative disables automatic DLQ cleanup.
func WithConsumerCleanupDLQInterval(interval time.Duration) ConsumerOption {
	return func(o *consumer) {
		o.cleanupDLQInterval = interval
	}
}

// WithConsumerCleanupDLQAge sets the maximum age for dead-letter queue jobs
// before they are eligible for deletion by the cleanup task for this consumer.
func WithConsumerCleanupDLQAge(age time.Duration) ConsumerOption {
	return func(o *consumer) {
		if age > 0 {
			o.cleanupDLQAge = age
		}
	}
}

func WithConsumerPushSubscription(rateLimitRPM int) ConsumerOption {
	return func(o *consumer) {
		o.rateLimitRPM = uint16(rateLimitRPM)
	}
}
