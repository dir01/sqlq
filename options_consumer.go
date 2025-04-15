package sqlq

import "time"

// ConsumerOption defines functional options for Subscribe
type ConsumerOption func(*consumer)

// WithConsumerConcurrency sets the number of concurrent workers for a given consumer
func WithConsumerConcurrency(n int) ConsumerOption {
	return func(o *consumer) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// WithConsumerPrefetchCount sets the number of jobs to prefetch in a single query for a given consumer
func WithConsumerPrefetchCount(n int) ConsumerOption {
	return func(o *consumer) {
		if n > 0 {
			o.prefetchCount = n
		}
	}
}

// WithConsumerPollInteval sets how frequently to check for new jobs for a given consumer
func WithConsumerPollInteval(interval time.Duration) ConsumerOption {
	return func(o *consumer) {
		if interval > 0 {
			o.pollInterval = interval
		}
	}
}

// WithConsumerBackoffFunc sets a function to calculate backoff for a given consumer
func WithConsumerBackoffFunc(fn func(int) time.Duration) ConsumerOption {
	return func(o *consumer) {
		o.backoffFunc = fn
	}
}

// WithMaxRetries sets the maximum number of retries for a job
func WithMaxRetries(n int) ConsumerOption {
	return func(o *consumer) {
		if n >= 0 {
			o.maxRetries = n
		}
	}
}
