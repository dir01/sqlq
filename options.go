package sqlqueue

import "time"

// SubscriptionOption defines functional options for Subscribe
type SubscriptionOption func(*subscriptionOptions)

// subscriptionOptions holds configuration for a subscription
type subscriptionOptions struct {
	concurrency   int
	prefetchCount int
}

// WithConcurrency sets the number of concurrent workers for a subscription
func WithConcurrency(n int) SubscriptionOption {
	return func(o *subscriptionOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// WithPrefetchCount sets the number of jobs to prefetch in a single query
func WithPrefetchCount(n int) SubscriptionOption {
	return func(o *subscriptionOptions) {
		if n > 0 {
			o.prefetchCount = n
		}
	}
}

// PublishOption defines functional options for Publish
type PublishOption func(*publishOptions)

// publishOptions holds configuration for publishing a job
type publishOptions struct {
	delay      time.Duration
	maxRetries int
}

// WithDelay schedules a job to run after the specified delay
func WithDelay(delay time.Duration) PublishOption {
	return func(o *publishOptions) {
		if delay > 0 {
			o.delay = delay
		}
	}
}

// WithMaxRetries sets the maximum number of retries for a job
func WithMaxRetries(n int) PublishOption {
	return func(o *publishOptions) {
		if n >= 0 {
			o.maxRetries = n
		}
	}
}
