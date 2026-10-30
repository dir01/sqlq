package sqlq

import (
	"context"
	"database/sql"
	"math"
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
// Default is 500.
func WithConsumerCleanupBatch(batchSize uint16) ConsumerOption {
	return func(o *consumer) {
		if batchSize > 0 {
			o.cleanupBatch = batchSize
		}
	}
}

// WithConsumerPrefetchCount sets the number of jobs to prefetch in a single query for a given consumer.
// By default it equals the consumer's concurrency. A value below concurrency is raised to it.
// You may also configure default value for all consumers, see WithDefaultPrefetchCount.
func WithConsumerPrefetchCount(prefetchCount uint16) ConsumerOption {
	return func(o *consumer) {
		if prefetchCount > 0 {
			o.prefetchCount = prefetchCount
		}
	}
}

// WithConsumerPollDelayFunc sets the function that calculates the delay to the next poll for a given consumer.
// See WithDefaultPollDelayFunc for the argument and the default. The option ignores nil.
// You may also configure default value for all consumers, see WithDefaultPollDelayFunc.
func WithConsumerPollDelayFunc(pollDelayFunc func(emptyPolls uint16) time.Duration) ConsumerOption {
	return func(o *consumer) {
		if pollDelayFunc != nil {
			o.pollDelayFunc = pollDelayFunc
		}
	}
}

// WithConsumerBackoffFunc sets a function to calculate backoff for a given consumer
// The retry argument stops at 65535. JobInfo.RetryCount keeps the full count.
// If fn panics, the queue logs the panic, and the retry uses the default exponential backoff
// with jitter, to a maximum of 24 hours.
// Consume returns ErrNilBackoffFunc if fn is nil.
// You may also configure default value for all consumers, see WithDefaultBackoffFunc.
func WithConsumerBackoffFunc(fn func(retryNum uint16) time.Duration) ConsumerOption {
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
// The job timeout also sets the claim: other consumers can take a fetched job after the job
// timeout plus half of it (at most 10 seconds more). Before a handler starts, the worker
// extends the claim if necessary.
// The option ignores a zero or negative timeout, and increases a timeout shorter than 50ms to 50ms.
// You may also configure default value for all consumers, see WithDefaultJobTimeout.
func WithConsumerJobTimeout(timeout time.Duration) ConsumerOption {
	return func(o *consumer) {
		if timeout > 0 {
			o.jobTimeout = max(timeout, minJobTimeout)
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

// WithAsyncPushRateLimit sets the maximum number of async notifications from a publisher
// in one minute. Zero or less means no limit. A value above 65535 becomes 65535.
func WithAsyncPushRateLimit(rpm int) ConsumerOption {
	return func(o *consumer) {
		o.asyncPushMaxRPM = uint16(min(max(rpm, 0), math.MaxUint16))
	}
}

// DeadLetterHook is called when a job has exhausted its retries, inside the transaction that moves
// the job to the dead letter queue. handlerErr is the error returned by the final attempt.
// If the final attempt did not write a result, for example because the worker process stopped,
// or if this is a new try after a failed hook, handlerErr starts with "no attempts left".
// It then includes only the text of the last recorded error, so errors.Is and errors.As do not
// find the original error.
//
// The move and the hook's writes through tx are committed together, and only if the hook returns nil.
// The queue owns tx: hooks must not call Commit or Rollback, including in a defer.
// Return an error to abort the transaction; return nil to allow the queue to commit.
// If the hook returns an error or panics, the move is rolled back and the job is rescheduled.
// The job has no attempts left, so its handler does not run again: the next attempt only
// tries the move again, and calls the hook again.
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
