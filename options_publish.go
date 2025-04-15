package sqlq

import "time"

// PublishOption defines functional options for Publish
type PublishOption func(*publishOptions)

// publishOptions holds configuration for publishing a job
type publishOptions struct {
	delay time.Duration
}

// WithDelay schedules a job to run after the specified delay
func WithDelay(delay time.Duration) PublishOption {
	return func(o *publishOptions) {
		if delay > 0 {
			o.delay = delay
		}
	}
}
