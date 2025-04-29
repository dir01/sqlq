package sqlq

import (
	"context"
	"database/sql"
	"encoding/json"
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
	cancel                   context.CancelFunc                  // cancel() cancels ctx, TODO: stop storing context in struct, https://go.dev/blog/context-and-structs
	backoffFunc              func(retryNum uint16) time.Duration // Calculate a delay retried job is scheduled with
	tracer                   trace.Tracer
	jobsChan                 chan job        // Written to by a db poller, consumed by worker goroutines
	shutdownChan             <-chan struct{} // Read-only view of the global shutdown channel
	jobType                  string
	consumerName             string
	workerWg                 sync.WaitGroup // Wait to make sure all scheduled goroutines stopped
	pollInterval             time.Duration  // How often should db poller run
	jobTimeout               time.Duration  // After this amount a time, job context will be canceled
	cleanupProcessedInterval time.Duration  // How often this consumer cleans up processed jobs
	cleanupProcessedAge      time.Duration  // Max age for processed jobs of this type
	cleanupDLQInterval       time.Duration  // How often this consumer cleans up DLQ jobs
	cleanupDLQAge            time.Duration  // Max age for DLQ jobs of this type
	maxRetries               int32          // Total attempts is maxRetries+1. -1 means unlimited
	prefetchCount            uint16         // LIMIT when fetching jobs from database into jobsChan
	concurrency              uint16         // How many goroutines will process jobsChan
	cleanupBatch             uint16         // Number of jobs to delete per cleanup batch
}

// start launches the polling, worker, and cleanup goroutines for the consumer.
func (cons *consumer) start() {
	go cons.poll()

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

// poll periodically fetches jobs from the database and sends them to the jobs channel.
func (cons *consumer) poll() {
	cons.workerWg.Add(1)
	defer cons.workerWg.Done()

	ticker := time.NewTicker(cons.pollInterval)
	defer ticker.Stop()

	var attempt uint16
	for {
		// Use the consumer's context for fetching jobs
		if err := cons.fetchJobs(cons.ctx); err != nil && !errors.Is(err, context.Canceled) {
			// only start span in case of error since we don't want
			// long-running traces of long-running background processes
			_, span := cons.tracer.Start(cons.ctx, "sqlq.consumer.fetch_jobs") // Use consumer's tracer

			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to fetch jobs")
			span.SetAttributes(
				attribute.String("consumer_name", cons.consumerName),
				attribute.String("job_type", cons.jobType),
			)
			span.End()

			// backoffFunc is more about backing off from rescheduling,
			// and not for backing off attempts to poll the queue,
			//
			backoffTimeout := cons.backoffFunc(attempt)
			attempt++
			select {
			case <-time.After(backoffTimeout):
				continue // Continue after backoff
			case <-cons.ctx.Done():
				return // Exit if context canceled during backoff
			case <-cons.shutdownChan:
				return // Exit if global shutdown triggered during backoff
			}
		}

		attempt = 0 // Reset attempt count on success

		select {
		case <-cons.ctx.Done():
			// Context was canceled
			return
		case <-cons.shutdownChan: // Use consumer's view of the shutdown channel
			// Check global shutdown signal
			return
		case <-ticker.C:
			// Timer expired, continue to next poll
			continue
		}
	}
}

// fetchJobs fetches jobs for this consumer and sends them to the jobs channel.
func (cons *consumer) fetchJobs(ctx context.Context) error {
	// Use consumer's driver
	jobs, err := cons.driver.getJobsForConsumer(ctx, cons.consumerName, cons.jobType, cons.prefetchCount)
	if err != nil {
		// Don't wrap error here, let the poller handle logging/backoff
		return err
	}

	for _, job := range jobs {
		select {
		case <-ctx.Done(): // Check the context passed to fetchJobs (usually consumer's ctx)
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
		case <-cons.ctx.Done(): // Check consumer's context
			return
		case j, ok := <-cons.jobsChan:
			if !ok {
				return // Channel closed
			}
			// Call the consumer's processJob method
			cons.processJob(&j)
		}
	}
}

// processJob handles a single job fetched by the poller.
func (cons *consumer) processJob(job *job) {
	parentCtx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.MapCarrier(job.TraceContext))

	traceContextBytes, _ := json.Marshal(job.TraceContext)
	// Use consumer's tracer
	consumeCtx, consumeSpan := cons.tracer.Start(parentCtx, "sqlq.consume",
		trace.WithLinks(trace.Link{
			SpanContext: trace.SpanContextFromContext(parentCtx),
			Attributes:  nil, // Explicitly initialize Attributes
		}),
		trace.WithAttributes(
			attribute.String("sqlq.job_type", cons.jobType),
			attribute.Int64("sqlq.job_id", job.ID),
			attribute.Int("sqlq.retry_count", int(job.RetryCount)),
			attribute.String("sqlq.trace_context", string(traceContextBytes)),
		),
		trace.WithSpanKind(trace.SpanKindConsumer),
	)
	defer consumeSpan.End() // Ensure span is always ended

	// Use consumer's db connection
	tx, err := cons.db.BeginTx(consumeCtx, &sql.TxOptions{Isolation: sql.LevelDefault, ReadOnly: false}) // Explicitly set ReadOnly
	if err != nil {
		consumeSpan.RecordError(err)
		consumeSpan.SetStatus(codes.Error, "failed to begin transaction")
		// Attempt rollback, but ignore error as the transaction might not be valid
		_ = tx.Rollback()
		return
	}
	// Defer rollback; it will be ignored if tx.Commit() is called later.
	// Check the rollback error unless it's because the transaction was already committed/rolled back.
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			consumeSpan.AddEvent("Failed to rollback transaction in processJob", trace.WithAttributes(attribute.String("error", rErr.Error())))
		}
	}()

	// Prepare handler context, potentially with timeout
	handlerCtx := consumeCtx
	var handlerCancel context.CancelFunc
	if cons.jobTimeout > 0 {
		handlerCtx, handlerCancel = context.WithTimeout(consumeCtx, cons.jobTimeout)
		defer handlerCancel() // Ensure cancellation resource is released
		consumeSpan.SetAttributes(attribute.Int64("sqlq.job_timeout_ms", cons.jobTimeout.Milliseconds()))
	}

	// Use consumer's tracer
	handlerCtx, handlerSpan := cons.tracer.Start(handlerCtx, "sqlq.handle") // Start span with potentially timed-out context

	// --- THE MEAT: Call the user-provided handler ---
	handlerErr := cons.handler(handlerCtx, tx, job.Payload)
	// --- Handler finished or timed out ---

	// Check for handler error *or* context error (like timeout)
	finalErr := handlerErr // Start with the explicit handler error
	if finalErr == nil && handlerCtx.Err() != nil {
		// If handler returned nil but context has an error (e.g., timeout), use the context error.
		finalErr = handlerCtx.Err()
		handlerSpan.SetAttributes(attribute.Bool("sqlq.job_timed_out", true))
	}

	if finalErr != nil {
		handlerSpan.RecordError(finalErr)
		handlerSpan.SetStatus(codes.Error, "handler failed or timed out")
	} else {
		handlerSpan.SetStatus(codes.Ok, "handler succeeded")
	}
	handlerSpan.End() // End handler span here

	// --- Decide action based on finalErr ---

	// Happy path: Handler succeeded without error or timeout
	if finalErr == nil {
		// Commit the transaction first
		commitErr := tx.Commit()
		if commitErr != nil {
			// If commit fails, we can't reliably mark processed. Record error and return.
			consumeSpan.RecordError(fmt.Errorf("failed to commit transaction after successful handle: %w", commitErr))
			consumeSpan.SetStatus(codes.Error, "commit failed after handle")
			// Rollback was already deferred.
			return
		}

		// Transaction committed successfully, now mark the job processed
		// Use consumer's tracer and driver
		markCtx, markSpan := cons.tracer.Start(consumeCtx, "sqlq.mark_processed") // Use original consumeCtx
		markErr := cons.driver.markJobProcessed(markCtx, job.ID, cons.consumerName)
		if markErr != nil {
			// Log or trace the error, but the job is technically processed as the TX committed.
			markSpan.RecordError(fmt.Errorf("failed to mark job processed after commit: %w", markErr))
			markSpan.SetStatus(codes.Error, "mark processed failed after commit")
		} else {
			markSpan.SetStatus(codes.Ok, "job processed successfully")
		}
		markSpan.End()

		// Successfully processed and marked
		consumeSpan.SetStatus(codes.Ok, "job processed successfully")
		return
	}

	// --- Handler failed or timed out ---
	// Rollback is handled by the deferred call earlier.

	// Add handler error details to the main consume span
	consumeSpan.SetAttributes(attribute.String("sqlq.handler_error", finalErr.Error()))
	consumeSpan.SetStatus(codes.Error, "handler failed or timed out")

	// Decide whether to retry or move to DLQ
	if cons.maxRetries == infiniteRetries || int32(job.RetryCount) < cons.maxRetries {
		// Use consumer's tracer
		retryCtx, retrySpan := cons.tracer.Start(consumeCtx, "sqlq.retry") // Use original consumeCtx
		// Call consumer's retryJob method
		retryErr := cons.retryJob(retryCtx, job, finalErr.Error())
		if retryErr != nil {
			retrySpan.RecordError(retryErr)
			retrySpan.SetStatus(codes.Error, "retry failed")
		} else {
			retrySpan.SetStatus(codes.Ok, "job scheduled for retry")
		}
		retrySpan.End()
		return // Job scheduled for retry
	}

	// Max retries exceeded or retries disabled, move to Dead Letter Queue
	// Use consumer's tracer and driver
	dlqCtx, dlqSpan := cons.tracer.Start(consumeCtx, "sqlq.move_to_dlq") // Use original consumeCtx
	moveErr := cons.driver.moveToDeadLetterQueue(dlqCtx, job.ID, finalErr.Error())
	if moveErr != nil {
		dlqSpan.RecordError(moveErr)
		dlqSpan.SetStatus(codes.Error, "failed to move job to DLQ")
	} else {
		consumeSpan.SetAttributes(attribute.Bool("sqlq.moved_to_dlq", true))
		dlqSpan.SetStatus(codes.Ok, "job moved to DLQ")
	}
	dlqSpan.End()
}

// retryJob increments the retry count and schedules the job for retry after a backoff period.
func (cons *consumer) retryJob(ctx context.Context, job *job, errorMsg string) error {
	// Use consumer's tracer
	ctx, span := cons.tracer.Start(ctx, "sqlq.retry_job", trace.WithAttributes(
		attribute.Int64("sqlq.job_id", job.ID),
		attribute.Int("sqlq.retry_count", int(job.RetryCount)),
		attribute.String("sqlq.error_message", errorMsg),
	))
	defer span.End()

	// Use consumer's backoff function
	backoff := cons.backoffFunc(job.RetryCount + 1)
	span.SetAttributes(attribute.Float64("sqlq.backoff_seconds", backoff.Seconds()))

	// Use consumer's driver
	if err := cons.driver.markJobFailedAndReschedule(ctx, job.ID, errorMsg, backoff); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to mark job as failed and reschedule: %w", err)
	}

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
		case <-cons.shutdownChan: // Check global shutdown
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
		case <-cons.shutdownChan: // Check global shutdown
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
		attribute.String("sqlq.consumer_name", cons.consumerName),
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
		attribute.String("sqlq.consumer_name", cons.consumerName),
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
