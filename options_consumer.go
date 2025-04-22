package sqlq

import "time"

// ConsumerOption defines functional options for Subscribe
type ConsumerOption func(*consumer)

// WithConsumerConcurrency sets the number of concurrent workers for a given consumer
// You may also configure default value for all consumers, see WithDefaultConcurrency.
func WithConsumerConcurrency(n int) ConsumerOption {
	return func(o *consumer) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// WithConsumerPrefetchCount sets the number of jobs to prefetch in a single query for a given consumer
// You may also configure default value for all consumers, see WithDefaultPrefetchCount.
func WithConsumerPrefetchCount(n int) ConsumerOption {
	return func(o *consumer) {
		if n > 0 {
			o.prefetchCount = n
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
func WithConsumerBackoffFunc(fn func(int) time.Duration) ConsumerOption {
	return func(o *consumer) {
		o.backoffFunc = fn
	}
}

// WithConsumerMaxRetries sets the maximum number of retries for a job.
// Use -1 for infinite retries.
// You may also configure default value for all consumers, see WithDefaultMaxRetries.
func WithConsumerMaxRetries(n int) ConsumerOption {
	return func(o *consumer) {
		if n >= -1 {
			o.maxRetries = n
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
