package sqlq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// consumer holds the configuration and state for processing a specific job type.
type consumer struct {
	db                       *sql.DB
	driver                   driver
	handler                  func(ctx context.Context, tx *sql.Tx, payloadBytes []byte) error
	onDeadLetter             DeadLetterHook                      // Called in the transaction that moves a job to the dead letter queue
	ctx                      context.Context                     // context that was passed to .Consume, but with cancelation
	cancel                   context.CancelFunc                  // cancel() cancels ctx, TODO: consider alternatives, see https://go.dev/blog/context-and-structs
	backoffFunc              func(retryNum uint16) time.Duration // Calculate a delay retried job is scheduled with. Defaults to exponential back off with jitter
	tracer                   trace.Tracer
	logger                   *slog.Logger
	metrics                  *metrics
	jobsChan                 chan job             // Written to by a db poller coroutine, consumed by worker goroutines
	claims                   map[string]heldClaim // Claims this consumer holds, by claimKey
	jobType                  string
	fetchWg                  sync.WaitGroup // Waits for the db poller goroutine
	workerWg                 sync.WaitGroup // Waits for the worker and cleanup goroutines
	claimsMutex              sync.Mutex
	claimTimeout             time.Duration
	pollInterval             time.Duration // How often should db poller run. You might want to set this value higher if you have push configured
	jobTimeout               time.Duration // After this amount a time job context will be canceled
	cleanupProcessedInterval time.Duration // How often this consumer cleans up processed jobs
	cleanupProcessedAge      time.Duration // Min age a processed job should be to be considered for cleanup
	cleanupDLQInterval       time.Duration // How often this consumer cleans up DLQ jobs
	cleanupDLQAge            time.Duration // Min age a DLQ job should be to be considered for cleanup
	maxRetries               int32         // Total attempts is maxRetries+1. -1 means unlimited
	prefetchCount            uint16        // LIMIT when fetching jobs from database into jobsChan
	concurrency              uint16        // How many goroutines will read from jobsChan
	cleanupBatch             uint16        // Number of jobs to delete per cleanup batch
	asyncPushEnabled         bool          // By default, only polling is used. If enabled, supporting drivers will also deliver async pushes for lower latency
	asyncPushMaxRPM          uint16        // How many times per minute will async push be delivered at most

}

// start launches the polling, worker, and cleanup goroutines for the consumer.
func (cons *consumer) start() {
	cons.fetchWg.Go(cons.fetchJobsLoop)

	for range cons.concurrency {
		cons.workerWg.Go(cons.workerLoop)
	}

	if cons.cleanupProcessedInterval > 0 {
		cons.workerWg.Go(cons.runProcessedCleanupLoop)
	}

	if cons.cleanupDLQInterval > 0 {
		cons.workerWg.Go(cons.runDLQCleanupLoop)
	}
}

// shutdown cancels the consumer's context, so that no worker starts another job. When the poller
// stops, no new claims come in: shutdown then releases the claims of jobs that no worker started.
// Then it waits for the workers, and releases the claims that they did not settle.
// If ctx ends first, it returns ctx's error. The claims that it did not release expire.
func (cons *consumer) shutdown(ctx context.Context) error {
	cons.cancel()
	if err := waitContext(ctx, &cons.fetchWg); err != nil {
		return err
	}
	cons.releaseClaims(ctx, false)
	if err := waitContext(ctx, &cons.workerWg); err != nil {
		return err
	}
	cons.releaseClaims(ctx, true)
	return nil
}

// waitContext waits for wg. If ctx ends first, it returns ctx's error.
func waitContext(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// releaseClaims releases the claims of jobs that no worker started, and with includeStarted, all claims.
// It stops when ctx ends or after claimReleaseTimeout. The claims that it does not release expire.
func (cons *consumer) releaseClaims(ctx context.Context, includeStarted bool) {
	var jobs []job
	cons.claimsMutex.Lock()
	for key, claim := range cons.claims {
		if claim.started && !includeStarted {
			continue
		}
		jobs = append(jobs, claim.job)
		delete(cons.claims, key)
	}
	cons.claimsMutex.Unlock()

	ctx, cancel := context.WithTimeout(ctx, claimReleaseTimeout)
	defer cancel()
	for i, j := range jobs {
		err := ctx.Err()
		if err == nil {
			err = cons.driver.releaseClaim(ctx, j)
		}
		if ctx.Err() != nil {
			cons.logger.WarnContext(ctx, "sqlq: no time left to release claims; their jobs are available again when the claims expire",
				slog.Int("claims", len(jobs)-i))
			return
		}
		if err != nil && !errors.Is(err, ErrClaimLost) {
			cons.logger.ErrorContext(ctx, "sqlq: failed to release claim", slog.Int64("job_id", j.ID), slog.Any("error", err))
		}
	}
}

// fetchJobsLoop periodically fetches jobs from the database and sends them to the jobs channel.
func (cons *consumer) fetchJobsLoop() {
	ticker := time.NewTicker(cons.pollInterval)
	defer ticker.Stop()

	// subChan is a channel that a driver might write to if async push is configured and supported
	// in cases where async push isn't used, this will remain a dummy channel
	subChan := make(<-chan struct{})
	if cons.asyncPushEnabled {
		var bucket *tokenBucket
		if cons.asyncPushMaxRPM != 0 {
			bucket = &tokenBucket{Capacity: cons.asyncPushMaxRPM, RPM: cons.asyncPushMaxRPM} //nolint:exhaustruct
		}
		driverSubChan, err := cons.driver.subscribeForConsumer(cons.ctx, cons.jobType, bucket)
		if err != nil {
			cons.logger.ErrorContext(cons.ctx, "sqlq: failed to subscribe for async push; only polling finds jobs", slog.Any("error", err))
		} else if driverSubChan != nil {
			subChan = driverSubChan
		}
	}

	var attempt uint16
	for {
		if err := cons.fetchJobs(cons.ctx); err != nil && !errors.Is(err, context.Canceled) {
			backoffTimeout := fetchRetryBackoff(attempt, cons.pollInterval)
			// only start span in case of error since we don't want
			// long-running traces of long-running background processes
			_, span := cons.tracer.Start(cons.ctx, "sqlq.consumer.fetch_jobs")

			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to fetch jobs")
			span.SetAttributes(
				attribute.String("job_type", cons.jobType),
				attribute.Int("attempt", int(attempt)),
				attribute.Int64("retry_delay_ms", backoffTimeout.Milliseconds()),
			)
			span.End()
			// The backoff limits how often this is logged while the database is unavailable.
			// A poll that shutdown interrupts can fail without context.Canceled; that is no failure.
			if cons.ctx.Err() == nil {
				cons.logger.ErrorContext(cons.ctx, "sqlq: failed to fetch jobs",
					slog.Int("attempt", int(attempt)), slog.Duration("retry_in", backoffTimeout), slog.Any("error", err))
			}

			if attempt < 16 {
				attempt++
			}
			select {
			case <-time.After(backoffTimeout):
				continue
			case <-cons.ctx.Done():
				return
			}
		}

		attempt = 0 // Reset attempt count on success

		select {
		case <-cons.ctx.Done():
			return
		case <-subChan: // This will always block if async push is not used
			// Got async push, so we'll fetch prematurely.
			// This means we need to reset the ticker so that fetches are pollInterval apart.
			ticker.Reset(cons.pollInterval)
			continue
		case <-ticker.C:
			// Timer expired, continue to next poll
			continue
		}
	}
}

// fetchJobs fetches jobs for this consumer and sends them to the jobs channel.
func (cons *consumer) fetchJobs(ctx context.Context) error {
	cons.claimsMutex.Lock()
	// Claim no more jobs than the workers and the buffer can hold. Extra jobs would
	// wait outside the channel while their claim time runs out.
	capacity := min(int(cons.prefetchCount), int(cons.prefetchCount)+int(cons.concurrency)-len(cons.claims))
	cons.claimsMutex.Unlock()
	if capacity <= 0 {
		return nil
	}
	jobs, err := cons.driver.getJobsForConsumer(ctx, cons.jobType, uint16(capacity), cons.claimTimeout)
	if err != nil {
		// Don't wrap error here, let the poller handle logging/backoff
		return err
	}
	cons.claimsMutex.Lock()
	for _, j := range jobs {
		cons.claims[claimKey(j)] = heldClaim{job: j, started: false}
	}
	cons.claimsMutex.Unlock()

	for _, job := range jobs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case cons.jobsChan <- job:
			// Successfully sent job to worker channel
		}
	}

	return nil
}

// workerLoop processes jobs from the jobsChan.
func (cons *consumer) workerLoop() {
	for {
		select {
		case <-cons.ctx.Done():
			return
		case j, isOpen := <-cons.jobsChan:
			if !isOpen {
				return
			}
			cons.processJob(&j)
		}
	}
}

// processJob handles a single job fetched by the poller.
func (cons *consumer) processJob(j *job) {
	// settled means there is no claim left to release: the job's next state was committed,
	// or the claim was already lost to another attempt.
	settled := false
	defer func() {
		// During shutdown, keep the claim in the map. shutdown releases it.
		if cons.ctx.Err() != nil {
			return
		}
		if !settled {
			// Derived from cons.ctx so that Shutdown does not wait for this release.
			ctx, cancel := context.WithTimeout(cons.ctx, claimReleaseTimeout)
			defer cancel()
			if err := cons.driver.releaseClaim(ctx, *j); err != nil && !errors.Is(err, ErrClaimLost) {
				if cons.ctx.Err() != nil {
					return // Shutdown began during the release; shutdown releases the claim.
				}
				cons.logger.ErrorContext(ctx, "sqlq: failed to release claim", slog.Int64("job_id", j.ID), slog.Any("error", err))
			}
		}
		cons.claimsMutex.Lock()
		delete(cons.claims, claimKey(*j))
		cons.claimsMutex.Unlock()
	}()
	if !cons.startClaim(*j) {
		return
	}
	var ok bool
	if ok, settled = cons.ensureClaimTime(j); !ok {
		return
	}
	parentCtx := otel.GetTextMapPropagator().Extract(cons.ctx, propagation.MapCarrier(j.TraceContext))
	// For context propagation ^^^^^^^^^^^ to work, there should be something like
	// `otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))`
	// somewhere in setup code

	ctx, span := cons.tracer.Start(parentCtx, "sqlq.consume",
		// this is what ties our span to a publisher span
		trace.WithLinks(trace.Link{SpanContext: trace.SpanContextFromContext(parentCtx), Attributes: nil}),
		trace.WithAttributes(
			attribute.String("sqlq.job_type", cons.jobType),
			attribute.Int64("sqlq.job_id", j.ID),
			attribute.Int("sqlq.retry_count", int(j.RetryCount)),
		),
		trace.WithSpanKind(trace.SpanKindConsumer),
	)

	defer span.End()

	info := cons.jobInfo(j)

	var tx *sql.Tx
	var err error
	for {
		tx, err = cons.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault, ReadOnly: false})
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to begin transaction")
			if cons.ctx.Err() == nil {
				cons.logger.ErrorContext(ctx, "sqlq: failed to begin the handler's transaction",
					slog.Int64("job_id", j.ID), slog.Any("error", err))
			}
			return
		}
		// Beginning can wait for a pool connection, during which the claim can expire and another
		// consumer can complete the job. Check the claim again before the handler runs.
		if time.Until(j.ClaimExpiresAt) >= claimTimeNeeded(cons.jobTimeout) {
			break
		}
		_ = tx.Rollback() // Extending the claim may need this connection.
		if ok, settled = cons.ensureClaimTime(j); !ok {
			return
		}
	}

	// The handler starts now. Its attempt is recorded with the result that each return below sets.
	start := time.Now()
	cons.metrics.recordLag(ctx, cons.jobType, start.Sub(j.ScheduledAt))
	result := resultError
	defer func() { cons.metrics.recordAttempt(ctx, cons.jobType, result, time.Since(start)) }()

	// attemptJob finishes the handler's transaction before returning, so whatever the handler
	// locked is released before the job is retried or dead-lettered (and the dead-letter hook runs).
	handlerErr := cons.attemptJob(ctx, span, j, info, tx)
	switch {
	case handlerErr == nil:
		settled, result = true, resultSuccess
		return
	case errors.Is(handlerErr, ErrClaimLost):
		settled, result = true, resultClaimLost
		return
	case cons.ctx.Err() != nil:
		result = resultCanceled
		return
	}

	// --- Handler failed or timed out ---

	span.SetAttributes(attribute.String("sqlq.handler_error", handlerErr.Error()))
	span.SetStatus(codes.Error, "handler failed or timed out")

	if !info.IsFinalAttempt() {
		settled, result = cons.writeOutcome(ctx, span, j, cons.retryJob(ctx, j, handlerErr.Error()), resultRetry)
		return
	}

	// Max retries exceeded or retries disabled, move to Dead Letter Queue
	if err = cons.moveJobToDLQ(ctx, j, info, handlerErr); err != nil && !errors.Is(err, ErrClaimLost) && cons.ctx.Err() == nil {
		// Reschedule the job rather than leave it consumed and never processed.
		// Its handler runs again, and if it fails again, moving it is attempted again.
		span.RecordError(err)
		cons.logger.ErrorContext(ctx, "sqlq: failed to move job to the dead letter queue; rescheduling it",
			slog.Int64("job_id", j.ID), slog.Any("error", err))
		settled, result = cons.writeOutcome(ctx, span, j, cons.retryJob(ctx, j, handlerErr.Error()), resultRetry)
		return
	}
	if err == nil {
		span.SetAttributes(attribute.Bool("sqlq.moved_to_dlq", true))
	}
	settled, result = cons.writeOutcome(ctx, span, j, err, resultDeadLetter)
}

// writeOutcome reports how the write of a failed job's next state ended: whether no claim is left
// to release, and the attempt's result, which is result if the write succeeded. It logs a failed write,
// unless the claim was lost or shutdown stopped it. The job is then released or its claim expires.
func (cons *consumer) writeOutcome(ctx context.Context, span trace.Span, j *job, err error, result string) (bool, string) {
	if err != nil {
		span.RecordError(err)
	}
	switch {
	case err == nil:
		return true, result
	case errors.Is(err, ErrClaimLost):
		return true, resultClaimLost
	case cons.ctx.Err() != nil:
		return false, resultCanceled
	default:
		cons.logger.ErrorContext(ctx, "sqlq: failed to write the next state of a failed job",
			slog.Int64("job_id", j.ID), slog.String("next_state", result), slog.Any("error", err))
		return false, resultError
	}
}

// startClaim marks j's claim as started by a worker. It returns false once shutdown has begun:
// the job must not start then, and shutdown releases its claim.
func (cons *consumer) startClaim(j job) bool {
	cons.claimsMutex.Lock()
	defer cons.claimsMutex.Unlock()
	if cons.ctx.Err() != nil {
		return false
	}
	if claim, ok := cons.claims[claimKey(j)]; ok {
		claim.started = true
		cons.claims[claimKey(j)] = claim
	}
	return true
}

// ensureClaimTime makes sure the claim on j lasts long enough to run the handler and record its result,
// extending it if needed. It returns whether it does, and if not, whether the claim is already lost
// (settled), so there is nothing to release.
func (cons *consumer) ensureClaimTime(j *job) (bool, bool) {
	remaining := time.Until(j.ClaimExpiresAt)
	if remaining <= 0 {
		return false, false
	}
	if remaining >= claimTimeNeeded(cons.jobTimeout) {
		return true, false
	}
	deadline, err := cons.driver.extendClaim(cons.ctx, *j, cons.claimTimeout)
	if err != nil {
		if !errors.Is(err, ErrClaimLost) && cons.ctx.Err() == nil {
			cons.logger.ErrorContext(cons.ctx, "sqlq: failed to extend claim", slog.Int64("job_id", j.ID), slog.Any("error", err))
		}
		return false, errors.Is(err, ErrClaimLost)
	}
	j.ClaimExpiresAt = deadline
	return time.Until(deadline) > 0 && cons.ctx.Err() == nil, false
}

// attemptJob runs the handler in tx, marks the job processed if the handler succeeded,
// and commits both changes together. Any failure is returned for retry or dead-lettering.
func (cons *consumer) attemptJob(ctx context.Context, span trace.Span, j *job, info JobInfo, tx *sql.Tx) error {
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			span.AddEvent("Failed to rollback transaction in processJob", trace.WithAttributes(attribute.String("error", err.Error())))
		}
	}()

	if err := cons.handleJob(ctx, j, info, tx); err != nil {
		return err
	}

	if err := cons.markJobDone(ctx, j, tx); err != nil {
		if !errors.Is(err, errMarkInFreshTx) {
			return fmt.Errorf("failed to mark job %d processed: %w", j.ID, err)
		}
		// The handler wrote nothing, so there is nothing to commit with the mark.
		_ = tx.Rollback()
		if err = cons.markJobDoneInFreshTx(ctx, j); err != nil {
			return fmt.Errorf("failed to mark job %d processed: %w", j.ID, err)
		}
		span.SetStatus(codes.Ok, "job processed successfully")
		return nil
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	span.SetStatus(codes.Ok, "job processed successfully")
	return nil
}

// markJobDoneInFreshTx marks a job processed in a transaction of its own. The mark is that transaction's
// first statement, so it waits for the write lock instead of failing on a stale snapshot. If even that
// wait times out, it keeps trying until the consumer stops, even once the claim has expired. Charging
// the job a retry would need the same lock, and giving up would run a successful handler again. The
// mark filters on the claim token, so it fails with ErrClaimLost if another worker took the job since.
func (cons *consumer) markJobDoneInFreshTx(ctx context.Context, j *job) error {
	delay := time.Millisecond
	for {
		err := runInTx(ctx, cons.db, func(tx *sql.Tx) error { return cons.markJobDone(ctx, j, tx) })
		if !errors.Is(err, errMarkInFreshTx) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(delay):
		}
		delay = min(2*delay, 50*time.Millisecond)
	}
}

func (cons *consumer) handleJob(ctx context.Context, j *job, info JobInfo, tx *sql.Tx) error {
	ctx, handlerSpan := cons.tracer.Start(ctx, "sqlq.handle")
	ctx = contextWithJobInfo(ctx, info)

	defer handlerSpan.End()

	var cancel context.CancelFunc
	if cons.jobTimeout > 0 {
		handlerSpan.SetAttributes(attribute.Int64("sqlq.job_timeout_ms", cons.jobTimeout.Milliseconds()))
		ctx, cancel = context.WithTimeout(ctx, cons.jobTimeout)

		defer cancel()
	}

	handlerErr := callRecovering("handler", func() error {
		return cons.handler(ctx, tx, j.Payload)
	})

	if handlerErr == nil {
		handlerErr = contextCompletionError(ctx)
		if errors.Is(handlerErr, context.DeadlineExceeded) {
			handlerSpan.SetAttributes(attribute.Bool("sqlq.job_timed_out", true))
		}
	}

	if handlerErr != nil {
		handlerSpan.RecordError(handlerErr)
		handlerSpan.SetStatus(codes.Error, "handler failed or timed out")

		return handlerErr
	}

	handlerSpan.SetStatus(codes.Ok, "handler succeeded")

	return handlerErr
}

func (cons *consumer) markJobDone(ctx context.Context, j *job, tx *sql.Tx) error {
	ctx, span := cons.tracer.Start(ctx, "sqlq.mark_processed")

	defer span.End()

	if markErr := cons.driver.markJobProcessed(ctx, tx, j.ID, j.ClaimToken); markErr != nil {
		span.RecordError(fmt.Errorf("failed to mark job processed: %w", markErr))
		span.SetStatus(codes.Error, "mark processed failed")

		return markErr
	}

	span.SetStatus(codes.Ok, "job marked processed")

	return nil
}

// retryJob increments the retry count and schedules the job for retry after a backoff period.
func (cons *consumer) retryJob(ctx context.Context, job *job, errorMsg string) error {
	ctx, span := cons.tracer.Start(ctx, "sqlq.retry_job", trace.WithAttributes(
		attribute.Int64("sqlq.job_id", job.ID),
		attribute.Int("sqlq.retry_count", int(job.RetryCount)),
		attribute.String("sqlq.error_message", errorMsg),
	))

	defer span.End()

	// Preserve the uint16 callback API without wrapping long-lived retries to zero.
	backoff := cons.backoffFunc(uint16(min(uint64(job.RetryCount)+1, 65535)))
	span.SetAttributes(attribute.Float64("sqlq.backoff_seconds", backoff.Seconds()))

	if err := cons.driver.markJobFailedAndReschedule(ctx, job.ID, job.ClaimToken, errorMsg, backoff); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to mark job as failed and reschedule: %w", err)
	}

	return nil
}

// moveJobToDLQ moves the job to the dead letter queue. The dead-letter hook, if any, runs in the
// same transaction, so the move and the hook's writes are either both committed or both rolled back.
func (cons *consumer) moveJobToDLQ(ctx context.Context, j *job, info JobInfo, handlerErr error) error {
	ctx, span := cons.tracer.Start(ctx, "sqlq.move_to_dlq")
	defer span.End()

	var inTx func(tx *sql.Tx) error
	if cons.onDeadLetter != nil {
		inTx = func(tx *sql.Tx) error {
			if err := cons.runDeadLetterHook(ctx, tx, info, j.Payload, handlerErr); err != nil {
				return fmt.Errorf("dead letter hook failed: %w", err)
			}

			return nil
		}
	}

	if err := cons.driver.moveToDeadLetterQueue(ctx, j.ID, j.ClaimToken, handlerErr.Error(), inTx); err != nil {
		span.RecordError(fmt.Errorf("failed to move job %d to DLQ: %w", j.ID, err))
		span.SetStatus(codes.Error, "failed to move job to DLQ")

		return err
	}

	span.SetStatus(codes.Ok, "job moved to DLQ")

	return nil
}

// runDeadLetterHook runs the consumer's dead-letter hook in tx.
// The hook's context is canceled at shutdown and after the job timeout, and a panic is returned
// as an error, so a misbehaving hook can neither pin the worker forever nor take it down.
func (cons *consumer) runDeadLetterHook(ctx context.Context, tx *sql.Tx, info JobInfo, payload []byte, handlerErr error) error {
	ctx, span := cons.tracer.Start(ctx, "sqlq.on_dead_letter")
	defer span.End()

	ctx, cancel := context.WithCancel(contextWithJobInfo(ctx, info))
	defer cancel()

	stopCancelOnShutdown := context.AfterFunc(cons.ctx, cancel)
	defer stopCancelOnShutdown()

	if cons.jobTimeout > 0 {
		var cancelTimeout context.CancelFunc
		ctx, cancelTimeout = context.WithTimeout(ctx, cons.jobTimeout)

		defer cancelTimeout()
	}

	err := callRecovering("dead letter hook", func() error {
		return cons.onDeadLetter(ctx, tx, info, payload, handlerErr)
	})
	if err == nil {
		// As with handlers, a hook that outlived its context failed even if it returned nil:
		// tx doesn't share ctx, so its writes would otherwise still be committed.
		err = contextCompletionError(ctx)
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "dead letter hook failed")
	}

	return err
}

// contextCompletionError also checks elapsed deadlines because cancellation is
// delivered asynchronously. After a process pause (or scheduler delay), a
// callback can return before the context's overdue timer callback executes.
func contextCompletionError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

// callRecovering calls f and turns a panic in it into an error,
// so a panicking callback fails the same way as one that returned an error.
func callRecovering(callback string, f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic recovered in %s: %v\nStack trace:\n%s", callback, r, debug.Stack())
		}
	}()

	return f()
}

// runProcessedCleanupLoop periodically cleans up old processed jobs for this consumer.
func (cons *consumer) runProcessedCleanupLoop() {
	ticker := time.NewTicker(cons.cleanupProcessedInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cons.ctx.Done(): // Use consumer's context for shutdown
			return
		case <-ticker.C:
			// Pass consumer's driver and tracer
			cons.performProcessedCleanup(cons.driver, cons.tracer, cons.cleanupProcessedAge)
		}
	}
}

// runDLQCleanupLoop periodically cleans up old DLQ jobs for this consumer.
func (cons *consumer) runDLQCleanupLoop() {
	ticker := time.NewTicker(cons.cleanupDLQInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cons.ctx.Done(): // Use consumer's context for shutdown
			return
		case <-ticker.C:
			// Pass consumer's driver and tracer
			cons.performDLQCleanup(cons.driver, cons.tracer, cons.cleanupDLQAge)
		}
	}
}

// performProcessedCleanup calls the driver to delete old processed jobs.
func (cons *consumer) performProcessedCleanup(driver driver, tracer trace.Tracer, processedMaxAge time.Duration) {
	// Cleanup must release blocked SQL/pool waits when the consumer shuts down.
	ctx, span := tracer.Start(cons.ctx, "sqlq.consumer.cleanup.processed", trace.WithAttributes(
		attribute.String("sqlq.job_type", cons.jobType),
		attribute.String("sqlq.cleanup.age.processed", processedMaxAge.String()),
		attribute.Int("sqlq.cleanup.batch_size", int(cons.cleanupBatch)),
	))
	defer span.End()

	processedDeleted, err := driver.cleanupJobs(ctx, cons.jobType, processedMaxAge, cons.cleanupBatch)

	span.SetAttributes(
		attribute.Int64("sqlq.cleanup.processed_deleted", processedDeleted),
	)

	cons.endCleanup(ctx, span, "processed", err)
}

// performDLQCleanup calls the driver to delete old DLQ jobs.
func (cons *consumer) performDLQCleanup(driver driver, tracer trace.Tracer, dlqMaxAge time.Duration) {
	ctx, span := tracer.Start(cons.ctx, "sqlq.consumer.cleanup.dlq", trace.WithAttributes(
		attribute.String("sqlq.job_type", cons.jobType),
		attribute.String("sqlq.cleanup.age.dlq", dlqMaxAge.String()),
		attribute.Int("sqlq.cleanup.batch_size", int(cons.cleanupBatch)),
	))
	defer span.End()

	dlqDeleted, err := driver.cleanupDeadLetterQueueJobs(ctx, cons.jobType, dlqMaxAge, cons.cleanupBatch)

	span.SetAttributes(
		attribute.Int64("sqlq.cleanup.dlq_deleted", dlqDeleted),
	)

	cons.endCleanup(ctx, span, "dlq", err)
}

// endCleanup records how a cleanup of the given kind ended, in its span and, if it failed, in the log.
func (cons *consumer) endCleanup(ctx context.Context, span trace.Span, kind string, err error) {
	if err == nil {
		span.SetStatus(codes.Ok, kind+" cleanup completed")
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, kind+" cleanup failed")
	if cons.ctx.Err() == nil { // Shutdown cancels cleanup; that is no failure.
		cons.logger.ErrorContext(ctx, "sqlq: "+kind+" cleanup failed", slog.Any("error", err))
	}
}
