package sqlq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math"
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
	onDeadLetter             DeadLetterHook                        // Called in the transaction that moves a job to the dead letter queue
	ctx                      context.Context                       // context that was passed to .Consume, but with cancelation
	cancel                   context.CancelFunc                    // cancel() cancels ctx, TODO: consider alternatives, see https://go.dev/blog/context-and-structs
	backoffFunc              func(retryNum uint16) time.Duration   // Calculate a delay retried job is scheduled with. Defaults to exponential back off with jitter
	pollDelayFunc            func(emptyPolls uint16) time.Duration // Calculate the delay to the next poll. Defaults to defaultPollDelay
	tracer                   trace.Tracer
	logger                   *slog.Logger
	metrics                  *metrics
	jobsChan                 chan job             // Written to by a db poller coroutine, consumed by worker goroutines
	claims                   map[string]heldClaim // Claims this consumer holds, by claimKey
	jobType                  string
	fetchWg                  sync.WaitGroup // Waits for the db poller goroutine
	workerWg                 sync.WaitGroup // Waits for the worker and cleanup goroutines
	claimsMutex              sync.Mutex
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
	// subChan is a channel that a driver might write to if async push is configured and supported
	// in cases where async push isn't used, this will remain a dummy channel.
	// The loop reads it in the wait between polls and in the wait after a failed fetch.
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
	var emptyPolls uint16 // the number of polls since the last poll that found jobs
	for {
		foundJobs, err := cons.fetchJobs(cons.ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			// Calculate the delay one time. It has a random jitter, so a second call gives a different value.
			// A failed fetch does not change emptyPolls.
			backoffTimeout := fetchRetryBackoff(attempt, cons.pollDelayFunc(emptyPolls))

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

			if attempt < math.MaxUint16 {
				attempt++
			}
			select {
			case <-time.After(backoffTimeout):
				continue
			case <-subChan:
				// An async push stops the wait. With SQLite, a publisher in this process
				// wrote a job, so the database is possibly available again. The push does
				// not set attempt to 0. Only a successful fetch does that. The driver
				// discards a push that nobody reads, so the wait must not ignore it.
				continue
			case <-cons.ctx.Done():
				return
			}
		}

		attempt = 0 // Reset attempt count on success
		if foundJobs {
			emptyPolls = 0
		} else if emptyPolls < math.MaxUint16 {
			emptyPolls++
		}

		select {
		case <-cons.ctx.Done():
			return
		case <-subChan: // This will always block if async push is not used
			// Got async push, so we'll fetch prematurely.
			continue
		case <-time.After(cons.pollDelayFunc(emptyPolls)):
			continue
		}
	}
}

// fetchJobs fetches jobs for this consumer and sends them to the jobs channel.
// It returns true if it found jobs. A consumer that has no free capacity holds jobs,
// so for it, fetchJobs also returns true. Then the next poll does not come later.
func (cons *consumer) fetchJobs(ctx context.Context) (bool, error) {
	cons.claimsMutex.Lock()
	// Claim no more jobs than the workers and the buffer can hold. Extra jobs would
	// wait outside the channel while their claim time runs out.
	capacity := min(int(cons.prefetchCount), int(cons.prefetchCount)+int(cons.concurrency)-len(cons.claims))
	cons.claimsMutex.Unlock()
	if capacity <= 0 {
		return true, nil
	}
	jobs, err := cons.driver.getJobsForConsumer(ctx, cons.jobType, uint16(capacity), claimLength(cons.jobTimeout))
	if err != nil {
		// Don't wrap error here, let the poller handle logging/backoff
		return false, err
	}
	cons.claimsMutex.Lock()
	for _, j := range jobs {
		cons.claims[claimKey(j)] = heldClaim{job: j, started: false}
	}
	cons.claimsMutex.Unlock()

	for _, job := range jobs {
		select {
		case <-ctx.Done():
			return true, ctx.Err()
		case cons.jobsChan <- job:
			// Successfully sent job to worker channel
		}
	}

	return len(jobs) > 0, nil
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
	// If settled is true, there is no claim to release. The next state of the job is committed,
	// or a different attempt has the claim.
	settled := false
	defer func() {
		// During shutdown, keep the claim in the map. shutdown releases it.
		if cons.ctx.Err() != nil {
			return
		}
		if !settled {
			// The context comes from cons.ctx. Thus, Shutdown does not wait for this release.
			ctx, cancel := context.WithTimeout(cons.ctx, claimReleaseTimeout)
			defer cancel()
			if err := cons.driver.releaseClaim(ctx, *j); err != nil && !errors.Is(err, ErrClaimLost) {
				if cons.ctx.Err() != nil {
					return // Shutdown started during the release. Shutdown releases the claim.
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
	if err := cons.ensureClaimTime(j, true); err != nil {
		settled = errors.Is(err, ErrClaimLost)
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

	if info.noAttemptsLeft() {
		// The earlier attempts used all retries, but at least one of them did not write a result.
		// For example, the worker process stopped during the handler. Do not run the handler again.
		// The metrics get the result of that attempt now, without a duration and a lag.
		var result string
		settled, result = cons.finishFailedAttempt(ctx, span, j, info, noAttemptsLeftError(j))
		cons.metrics.recordResult(ctx, cons.jobType, result)
		return
	}

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
		// BeginTx can wait for a pool connection. During that time, the claim can expire, and a
		// different consumer can complete the job. Thus, examine the claim again before the handler starts.
		if time.Until(j.ClaimExpiresAt) >= minClaimLeftAtStart(cons.jobTimeout) {
			break
		}
		_ = tx.Rollback() // The extension of the claim can need this connection.
		if err = cons.ensureClaimTime(j, false); err != nil {
			settled = errors.Is(err, ErrClaimLost)
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

	settled, result = cons.finishFailedAttempt(ctx, span, j, info, handlerErr)
}

// finishFailedAttempt schedules a retry, or moves the job to the dead letter queue after the final
// attempt. It returns the values of writeOutcome: if no claim is left to release, and the result.
func (cons *consumer) finishFailedAttempt(ctx context.Context, span trace.Span, j *job, info JobInfo, handlerErr error) (bool, string) {
	if !info.IsFinalAttempt() {
		return cons.writeOutcome(ctx, span, j, cons.retryJob(ctx, j, handlerErr.Error()), resultRetry)
	}

	// Max retries exceeded or retries disabled, move to Dead Letter Queue
	err := cons.moveJobToDLQ(ctx, j, info, handlerErr)
	if err != nil && !errors.Is(err, ErrClaimLost) && cons.ctx.Err() == nil {
		// Reschedule the job rather than leave it consumed and never processed. The job has
		// no attempts left. Thus, the next attempt moves it again, and does not run the handler.
		span.RecordError(err)
		cons.logger.ErrorContext(ctx, "sqlq: failed to move job to the dead letter queue; rescheduling it",
			slog.Int64("job_id", j.ID), slog.Any("error", err))
		return cons.writeOutcome(ctx, span, j, cons.retryJob(ctx, j, handlerErr.Error()), resultRetry)
	}
	if err == nil {
		span.SetAttributes(attribute.Bool("sqlq.moved_to_dlq", true))
	}
	return cons.writeOutcome(ctx, span, j, err, resultDeadLetter)
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

// noAttemptsLeftError returns the error for a job that used all its attempts, but did not get
// a result from each of them. It includes the last error that an attempt wrote, if there is one.
func noAttemptsLeftError(j *job) error {
	err := fmt.Errorf("no attempts left after %d attempts, the worker possibly stopped during the handler", j.RetryCount)
	if j.LastError != "" {
		err = fmt.Errorf("%w; last recorded error: %s", err, j.LastError)
	}
	return err
}

// ensureClaimTime returns errClaimExpired if the new claim expired before the extension query
// was complete. The claim in the database can still be valid, so the worker releases it.
var errClaimExpired = errors.New("claim expired during its extension")

// ensureClaimTime makes sure that the claim on j is sufficiently long for the handler and its result.
// If necessary, it extends the claim. If startAttempt is true, it always writes: the write counts
// the attempt (see driver.extendClaim) and gives the claim its full length.
// It returns ErrClaimLost if the claim expired before the write, or if a different consumer has
// the claim now. In both cases, the worker does not release the claim: the fetch query already
// sees an expired claim as available.
// It returns a different error if it cannot extend the claim for a different reason.
func (cons *consumer) ensureClaimTime(j *job, startAttempt bool) error {
	remaining := time.Until(j.ClaimExpiresAt)
	if remaining <= 0 {
		cons.metrics.recordClaim(cons.ctx, cons.metrics.claimsExpired, cons.jobType)
		return fmt.Errorf("job %d: claim expired before the handler started: %w", j.ID, ErrClaimLost)
	}
	short := remaining < minClaimLeftAtStart(cons.jobTimeout)
	if !short && !startAttempt {
		return nil
	}
	deadline, err := cons.driver.extendClaim(cons.ctx, *j, claimLength(cons.jobTimeout), startAttempt)
	if err != nil {
		if !errors.Is(err, ErrClaimLost) && cons.ctx.Err() == nil {
			cons.logger.ErrorContext(cons.ctx, "sqlq: failed to extend claim", slog.Int64("job_id", j.ID), slog.Any("error", err))
		}
		return err
	}
	if short {
		cons.metrics.recordClaim(cons.ctx, cons.metrics.claimsExtended, cons.jobType)
	}
	j.ClaimExpiresAt = deadline
	if err = cons.ctx.Err(); err != nil {
		return err
	}
	if time.Until(deadline) <= 0 {
		return errClaimExpired
	}
	return nil
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
		if !errors.Is(err, errStaleReadTx) {
			return fmt.Errorf("failed to mark job %d processed: %w", j.ID, err)
		}
		// The handler did not write. Thus, the transaction has no changes to commit with the mark.
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

// markJobDoneInFreshTx marks a job processed in a new transaction. The mark is the first statement
// of the transaction. Thus, it waits for the write lock, and does not fail on a stale snapshot.
// If the wait times out, it tries again until the consumer stops, also after the claim expires.
// A retry of the job needs the same lock. If it stops, a successful handler runs again.
// The mark has a filter on the claim token. Thus, it fails with ErrClaimLost if a different worker has the job now.
func (cons *consumer) markJobDoneInFreshTx(ctx context.Context, j *job) error {
	delay := time.Millisecond
	for {
		err := runInTx(ctx, cons.db, func(tx *sql.Tx) error { return cons.markJobDone(ctx, j, tx) })
		if !errors.Is(err, errStaleReadTx) {
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
		handlerErr = ctxErrOrPassedDeadline(ctx)
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

	// The callback gets a uint16. A retry count above 65535 becomes 65535, not zero.
	retryNum := uint16(min(uint64(job.RetryCount)+1, 65535))
	var backoff time.Duration
	if err := callRecovering("backoff function", func() error {
		backoff = cons.backoffFunc(retryNum)
		return nil
	}); err != nil {
		backoff = exponentialBackoff(retryNum)
		span.RecordError(err)
		span.SetStatus(codes.Error, "backoff function panicked; using default backoff")
		cons.logger.ErrorContext(ctx, "sqlq: backoff function panicked; using default backoff",
			slog.Int64("job_id", job.ID), slog.Duration("retry_in", backoff), slog.Any("error", err))
	}
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
		err = ctxErrOrPassedDeadline(ctx)
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "dead letter hook failed")
	}

	return err
}

// ctxErrOrPassedDeadline returns ctx.Err(). It also returns context.DeadlineExceeded
// if the deadline of ctx is in the past, but ctx is not canceled yet.
//
// Go cancels a context with a timer, and the timer can be late. For example, CPU throttling,
// CPU steal or a suspend can stop the process while a handler runs. When the process continues,
// the handler can run before the timer. Then the job timeout is in the past, but ctx.Err() is nil.
// The handler completes and returns nil.
//
// At that time, the claim can be expired, and a different worker can run the job.
// The handler transaction does not use ctx, thus it can commit. This function makes the
// handler fail. Then sqlq does a rollback of the handler writes, as for all handlers that time out.
func ctxErrOrPassedDeadline(ctx context.Context) error {
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
