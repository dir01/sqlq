package sqlq

import (
	"context"
	"database/sql"
	"time"
)

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

// WithAsyncPush enables async push in supported drivers.
// By default, consumer only receives new jobs via periodic polling.
// If enabled (and chosen driver supports it), hints to perform a poll will arrive
// right as they happen. You may also rate limit this process with WithAsyncPushRateLimit.
func WithAsyncPush() ConsumerOption {
	return func(c *consumer) {
		c.asyncPushEnabled = true
	}
}

// WithAsyncPushRateLimit says how many times per minute should async notifications
// from a publisher be delivered, at most
func WithAsyncPushRateLimit(rpm int) ConsumerOption {
	return func(o *consumer) {
		o.asyncPushMaxRPM = uint16(rpm)
	}
}

// DeadLetterHook is called when a job has exhausted its retries, inside the transaction that moves
// the job to the dead letter queue. handlerErr is the error returned by the final attempt.
//
// The move and the hook's writes through tx are committed together, and only if the hook returns nil.
// If the hook returns an error or panics, the move is rolled back and the job is rescheduled
// like a failed attempt: its handler runs again and, if it fails again, the hook is called again.
//
// ctx is canceled at shutdown and after the consumer's job timeout. A hook whose ctx was canceled
// counts as failed even if it returns nil.
//
// On SQLite the queue's write lock is held while the hook runs, so the hook must write only through tx
// and must not call queue methods such as Publish: they would wait for that lock forever.
type DeadLetterHook func(ctx context.Context, tx *sql.Tx, info JobInfo, payload []byte, handlerErr error) error

// WithConsumerOnDeadLetter registers a hook that is called when a job of this type is moved
// to the dead letter queue. Use it to put domain state into a terminal "failed" state through tx.
func WithConsumerOnDeadLetter(hook DeadLetterHook) ConsumerOption {
	return func(o *consumer) {
		o.onDeadLetter = hook
	}
}
