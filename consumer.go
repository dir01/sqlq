package sqlq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
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
	ctx                      context.Context                     // context that was passed to .Consume, but with cancelation
	cancel                   context.CancelFunc                  // cancel() cancels ctx, TODO: consider alternatives, see https://go.dev/blog/context-and-structs
	backoffFunc              func(retryNum uint16) time.Duration // Calculate a delay retried job is scheduled with. Defaults to exponential back off with jitter
	tracer                   trace.Tracer
	jobsChan                 chan job // Written to by a db poller coroutine, consumed by worker goroutines
	jobType                  string
	workerWg                 sync.WaitGroup // Used to make sure that all scheduled goroutines stopped
	pollInterval             time.Duration  // How often should db poller run. You might want to set this value higher if you have push configured
	jobTimeout               time.Duration  // After this amount a time job context will be canceled
	cleanupProcessedInterval time.Duration  // How often this consumer cleans up processed jobs
	cleanupProcessedAge      time.Duration  // Min age a processed job should be to be considered for cleanup
	cleanupDLQInterval       time.Duration  // How often this consumer cleans up DLQ jobs
	cleanupDLQAge            time.Duration  // Min age a DLQ job should be to be considered for cleanup
	maxRetries               int32          // Total attempts is maxRetries+1. -1 means unlimited
	prefetchCount            uint16         // LIMIT when fetching jobs from database into jobsChan
	concurrency              uint16         // How many goroutines will read from jobsChan
	cleanupBatch             uint16         // Number of jobs to delete per cleanup batch
	asyncPushEnabled         bool           // By default, only polling is used. If enabled, supporting drivers will also deliver async pushes for lower latency
	asyncPushMaxRPM          uint16         // How many times per minute will async push be delivered at most

}

// start launches the polling, worker, and cleanup goroutines for the consumer.
func (cons *consumer) start() {
	go cons.fetchJobsLoop()

	for range cons.concurrency {
		go cons.workerLoop()
	}

	if cons.cleanupProcessedInterval > 0 {
		go cons.runProcessedCleanupLoop()
	}

	if cons.cleanupDLQInterval > 0 {
		go cons.runDLQCleanupLoop()
	}
}

// shutdown cancels the consumer's context and waits for its goroutines to finish.
func (cons *consumer) shutdown() {
	cons.cancel()
	cons.workerWg.Wait()
}

// fetchJobsLoop periodically fetches jobs from the database and sends them to the jobs channel.
func (cons *consumer) fetchJobsLoop() {
	cons.workerWg.Add(1)
	defer cons.workerWg.Done()

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
		if err == nil && driverSubChan != nil {
			subChan = driverSubChan
		}
	}

	var attempt uint16
	for {
		if err := cons.fetchJobs(cons.ctx); err != nil && !errors.Is(err, context.Canceled) {
			// only start span in case of error since we don't want
			// long-running traces of long-running background processes
			_, span := cons.tracer.Start(cons.ctx, "sqlq.consumer.fetch_jobs")

			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to fetch jobs")
			span.SetAttributes(
				attribute.String("job_type", cons.jobType),
				attribute.Int("attempt", int(attempt)),
			)
			span.End()

			backoffTimeout := exponentialBackoff(attempt)
			attempt++
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
	jobs, err := cons.driver.getJobsForConsumer(ctx, cons.jobType, cons.prefetchCount)
	if err != nil {
		// Don't wrap error here, let the poller handle logging/backoff
		return err
	}

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
	cons.workerWg.Add(1)
	defer cons.workerWg.Done()

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
	parentCtx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.MapCarrier(j.TraceContext))
	// For context propagation ^^^^^^^^^^^ to work, there should be something like
	// `otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))`
	// somewhere in setup code

	ctx, span := cons.tracer.Start(parentCtx, "sqlq.consume",
		// this is what ties our span to a publisher span
		trace.WithLinks(trace.Link{SpanContext: trace.SpanContextFromContext(parentCtx)}),
		trace.WithAttributes(
			attribute.String("sqlq.job_type", cons.jobType),
			attribute.Int64("sqlq.job_id", j.ID),
			attribute.Int("sqlq.retry_count", int(j.RetryCount)),
		),
		trace.WithSpanKind(trace.SpanKindConsumer),
	)

	defer span.End()

	tx, err := cons.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault, ReadOnly: false})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to begin transaction")
		// Attempt rollback, but ignore error as the transaction might not be valid
		_ = tx.Rollback()
		return
	}

	defer func() {
		if err := tx.Commit(); err == nil {
			// Happy path: Commit succeeded, we're done.
			span.SetStatus(codes.Ok, "job processed successfully")

			return
		} else if !errors.Is(err, sql.ErrTxDone) {
			span.RecordError(fmt.Errorf("failed to commit transaction: %w", err))
			span.SetStatus(codes.Error, "commit failed")
		}

		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			span.AddEvent("Failed to rollback transaction in processJob", trace.WithAttributes(attribute.String("error", err.Error())))
		}
	}()

	handlerErr := cons.handleJob(ctx, j, tx)

	if handlerErr == nil {
		// Happy path: Handler succeeded, mark done
		if markErr := cons.markJobDone(ctx, j, tx); markErr != nil {
			span.RecordError(fmt.Errorf("failed to mark job %d processed: %w", j.ID, markErr))
			span.SetStatus(codes.Error, "failed to mark job processed")

			return
		}

		return
	}

	// --- Handler failed or timed out ---

	span.SetAttributes(attribute.String("sqlq.handler_error", handlerErr.Error()))
	span.SetStatus(codes.Error, "handler failed or timed out")

	shouldRetry := cons.maxRetries == infiniteRetries || int32(j.RetryCount) < cons.maxRetries

	if shouldRetry {
		if err = cons.retryJob(ctx, j, handlerErr.Error()); err != nil {
			span.RecordError(err)
		}

		return
	}

	// Max retries exceeded or retries disabled, move to Dead Letter Queue
	if err = cons.moveJobToDLQ(ctx, j, handlerErr.Error()); err == nil {
		span.SetAttributes(attribute.Bool("sqlq.moved_to_dlq", true))
	}
}

func (cons *consumer) handleJob(ctx context.Context, j *job, tx *sql.Tx) error {
	ctx, handlerSpan := cons.tracer.Start(ctx, "sqlq.handle")

	defer handlerSpan.End()

	var cancel context.CancelFunc
	if cons.jobTimeout > 0 {
		handlerSpan.SetAttributes(attribute.Int64("sqlq.job_timeout_ms", cons.jobTimeout.Milliseconds()))
		ctx, cancel = context.WithTimeout(ctx, cons.jobTimeout)

		defer cancel()
	}

	handlerErr := cons.handler(ctx, tx, j.Payload)

	if handlerErr == nil && ctx.Err() != nil {
		handlerErr = ctx.Err()
		handlerSpan.SetAttributes(attribute.Bool("sqlq.job_timed_out", true))
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

	if markErr := cons.driver.markJobProcessed(ctx, j.ID); markErr != nil {
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

	backoff := cons.backoffFunc(job.RetryCount + 1)
	span.SetAttributes(attribute.Float64("sqlq.backoff_seconds", backoff.Seconds()))

	if err := cons.driver.markJobFailedAndReschedule(ctx, job.ID, errorMsg, backoff); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to mark job as failed and reschedule: %w", err)
	}

	return nil
}

func (cons *consumer) moveJobToDLQ(ctx context.Context, j *job, errorMsg string) error {
	ctx, span := cons.tracer.Start(ctx, "sqlq.move_to_dlq")
	defer span.End()

	if err := cons.driver.moveToDeadLetterQueue(ctx, j.ID, errorMsg); err != nil {
		span.RecordError(fmt.Errorf("failed to move job %d to DLQ: %w", j.ID, err))
		span.SetStatus(codes.Error, "failed to move job to DLQ")

		return err
	}

	span.SetStatus(codes.Ok, "job moved to DLQ")

	return nil
}

// runProcessedCleanupLoop periodically cleans up old processed jobs for this consumer.
func (cons *consumer) runProcessedCleanupLoop() {
	cons.workerWg.Add(1)       // Track the cleanup goroutine
	defer cons.workerWg.Done() // Signal completion

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
	cons.workerWg.Add(1)       // Track the cleanup goroutine
	defer cons.workerWg.Done() // Signal completion

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
	// Create a background context for the cleanup operation itself
	ctx, span := tracer.Start(context.Background(), "sqlq.consumer.cleanup.processed", trace.WithAttributes(
		attribute.String("sqlq.job_type", cons.jobType),
		attribute.String("sqlq.cleanup.age.processed", processedMaxAge.String()),
		attribute.Int("sqlq.cleanup.batch_size", int(cons.cleanupBatch)),
	))
	defer span.End()

	// Pass 0 for dlqMaxAge to signal only processed cleanup
	processedDeleted, err := driver.cleanupJobs(ctx, cons.jobType, processedMaxAge, cons.cleanupBatch)

	span.SetAttributes(
		attribute.Int64("sqlq.cleanup.processed_deleted", processedDeleted),
	)

	if err != nil {
		// Use a standard logger or integrate with a proper logging library
		log.Printf("sqlq: processed cleanup failed for job_type %s: %v", cons.jobType, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "processed cleanup failed")
	} else {
		span.SetStatus(codes.Ok, "processed cleanup completed")
	}
}

// performDLQCleanup calls the driver to delete old DLQ jobs.
func (cons *consumer) performDLQCleanup(driver driver, tracer trace.Tracer, dlqMaxAge time.Duration) {
	// Create a background context for the cleanup operation itself
	ctx, span := tracer.Start(context.Background(), "sqlq.consumer.cleanup.dlq", trace.WithAttributes(
		attribute.String("sqlq.job_type", cons.jobType),
		attribute.String("sqlq.cleanup.age.dlq", dlqMaxAge.String()),
		attribute.Int("sqlq.cleanup.batch_size", int(cons.cleanupBatch)),
	))
	defer span.End()

	// Pass 0 for processedMaxAge to signal only DLQ cleanup
	dlqDeleted, err := driver.cleanupDeadLetterQueueJobs(ctx, cons.jobType, dlqMaxAge, cons.cleanupBatch)

	span.SetAttributes(
		attribute.Int64("sqlq.cleanup.dlq_deleted", dlqDeleted),
	)

	if err != nil {
		// Use a standard logger or integrate with a proper logging library
		log.Printf("sqlq: dlq cleanup failed for job_type %s: %v", cons.jobType, err) // Corrected log message
		span.RecordError(err)
		span.SetStatus(codes.Error, "dlq cleanup failed") // Corrected status message
	} else {
		span.SetStatus(codes.Ok, "dlq cleanup completed") // Corrected status message
	}
}
