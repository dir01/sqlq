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

// WithConsumerCleanupInterval sets the interval at which the cleanup process runs for this consumer.
// Default is 1 hour. Set to 0 or negative to disable cleanup for this consumer.
func WithConsumerCleanupInterval(interval time.Duration) ConsumerOption {
	return func(o *consumer) {
		o.cleanupInterval = interval // Allow 0 or negative to disable
	}
}

// WithConsumerCleanupAge sets the maximum age for successfully processed jobs of this specific type
// before they are deleted. Default is 7 days.
func WithConsumerCleanupAge(age time.Duration) ConsumerOption {
	return func(o *consumer) {
		if age > 0 {
			o.cleanupAge = &age // Store as pointer to differentiate between 0 and not set
		}
	}
}

// WithConsumerCleanupDLQAge sets the maximum age for dead-letter queue jobs of this specific type
// before they are deleted. Default is 30 days.
func WithConsumerCleanupDLQAge(age time.Duration) ConsumerOption {
	return func(o *consumer) {
		if age > 0 {
			o.cleanupDLQAge = &age // Store as pointer
		}
	}
}
