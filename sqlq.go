package sqlq

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"

	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/dir01/sqlq"

type JobsQueue interface {
	Publish(ctx context.Context, jobType string, payload any, opts ...PublishOption) error
	PublishTx(ctx context.Context, tx *sql.Tx, jobType string, payload any, opts ...PublishOption) error
	Consume(
		ctx context.Context,
		jobType string,
		consumerName string,
		f func(ctx context.Context, tx *sql.Tx, payloadBytes []byte) error, opts ...ConsumerOption,
	) error
	GetDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]DeadLetterJob, error)
	RequeueDeadLetterJob(ctx context.Context, dlqID int64) error
	Shutdown()
	Run()
}

type DBType string

const (
	DBTypeSQLite   DBType = "sqlite"
	DBTypePostgres DBType = "postgres"
)

var (
	ErrDuplicateConsumer  = errors.New(".Consume was called twice for same job type")
	ErrUnsupportedDBType  = errors.New("unsupported database type")
	ErrMaxRetriesExceeded = errors.New("maximum retries exceeded")
	ErrJobNotFound        = errors.New("job not found")
)

var (
	defaultMaxRetries      int32  = 3
	infiniteRetries        int32  = -1
	defaultConcurrency     uint16 = uint16(min(runtime.NumCPU(), runtime.GOMAXPROCS(0)))
	defaultPrefetchCount   uint16 = defaultConcurrency
	defaultJobTimeout             = 15 * time.Minute
	defaultCleanupInterval        = 1 * time.Hour
	defaultCleanupAge             = 7 * 24 * time.Hour
	defaultCleanupDLQAge          = 30 * 24 * time.Hour
	defaultCleanupBatch    uint16 = 500
)

// sqlq implements the JobsQueue interface using SQL databases
type sqlq struct {
	db                *sql.DB
	driver            Driver
	consumersMap      map[string]*consumer // jobType -> consumer
	consumersMapMutex sync.RWMutex         // guard consumersMap
	shutdown          chan struct{}
	tracer            trace.Tracer

	// Function for calculating how much of a delay to use when rescheduling a failed job.
	// This is a default that will be used for all consumers
	// unless they define their own backoff function using WithConsumerBackoffFunc.
	defaultBackoffFunc func(retryNum uint16) time.Duration

	// How often to poll database for new jobs. Default is 100ms.
	// Individual consumers may override this using WithConsumerPollInteval.
	defaultPollInterval time.Duration

	// Default maximum duration a job can run before being considered timed out. Default is 15 minutes.
	// Individual consumers may override this using WithConsumerJobTimeout.
	defaultJobTimeout time.Duration

	// Default number of concurrent workers per consumer. Default is min(runtime.NumCPU(), runtime.GOMAXPROCS(0)).
	// Individual consumers may override this using WithConsumerConcurrency.
	defaultConcurrency uint16

	// Default number of jobs to fetch in advance per consumer. Default matches defaultConcurrency.
	// Individual consumers may override this using WithConsumerPrefetchCount.
	defaultPrefetchCount uint16

	// Default maximum number of retries for a failed job. Default is 3.
	// Individual consumers may override this using WithConsumerMaxRetries.
	defaultMaxRetries int32

	// Default interval for running cleanup tasks per consumer. Default is 1 hour.
	// Individual consumers may override this using WithConsumerCleanupInterval.
	defaultCleanupInterval time.Duration

	// Default maximum age for successfully processed jobs before deletion. Default is 7 days.
	// Individual consumers may override this using WithConsumerCleanupAge.
	defaultCleanupAge time.Duration

	// Default maximum age for dead-letter queue jobs before deletion. Default is 30 days.
	// Individual consumers may override this using WithConsumerCleanupDLQAge.
	defaultCleanupDLQAge time.Duration

	// Default number of jobs to delete in a single cleanup batch. Default is 1000.
	// Individual consumers may override this using WithConsumerCleanupBatch.
	defaultCleanupBatch uint16
}

type consumer struct {
	jobType      string
	consumerName string
	handler      func(ctx context.Context, tx *sql.Tx, payloadBytes []byte) error
	jobsChan     chan job           // Written to by a db poller, consumed by worker goroutines
	ctx          context.Context    // context that was passed to .Consume, but with cancelation
	cancel       context.CancelFunc // cancel() cancels ctx
	workerWg     sync.WaitGroup     // Wait to make sure all scheduled goroutines stopped

	pollInterval    time.Duration                       // How often should db poller run
	jobTimeout      time.Duration                       // After this amount a time, job context will be canceled
	backoffFunc     func(retryNum uint16) time.Duration // Calculate a delay retried job is scheduled with
	maxRetries      int32                               // Total attempts is maxRetries+1. -1 means unlimited
	prefetchCount   uint16                              // LIMIT when fetching jobs from database into jobsChan
	concurrency     uint16                              // How many goroutines will process jobsChan
	cleanupBatch    uint16                              // Number of jobs to delete per cleanup batch
	cleanupInterval time.Duration                       // How often this consumer cleans up its job type
	cleanupAge      *time.Duration                      // Max age for processed jobs of this type
	cleanupDLQAge   *time.Duration                      // Max age for DLQ jobs of this type

}

type job struct {
	CreatedAt    time.Time
	ID           int64
	JobType      string
	Payload      []byte
	TraceContext map[string]string // For trace propagation
	RetryCount   uint16
}

// DeadLetterJob represents a job that has been moved to the dead letter queue
type DeadLetterJob struct {
	ID            int64
	OriginalID    int64
	CreatedAt     time.Time
	FailedAt      time.Time
	JobType       string
	Payload       []byte
	FailureReason string
	MaxRetries    int32
	RetryCount    uint16
}

// New creates a new SQL-backed job queue
func New(db *sql.DB, dbType DBType, opts ...NewOption) (JobsQueue, error) {
	driver, err := getDriver(db, dbType)
	if err != nil {
		return nil, err
	}

	q := &sqlq{
		db:                   db,
		driver:               driver,
		consumersMap:         make(map[string]*consumer),
		shutdown:             make(chan struct{}),
		defaultPollInterval:  100 * time.Millisecond,
		defaultConcurrency:   defaultConcurrency,
		defaultPrefetchCount: defaultPrefetchCount,
		defaultMaxRetries:    defaultMaxRetries,
		defaultJobTimeout:    defaultJobTimeout,
		defaultBackoffFunc: func(retryNum uint16) time.Duration {
			jitter := float64(rand.Intn(int(retryNum)))
			backoff := math.Pow(2, float64(retryNum))
			return time.Duration(backoff+jitter) * time.Second
		},
		defaultCleanupInterval: defaultCleanupInterval,
		defaultCleanupAge:      defaultCleanupAge,
		defaultCleanupDLQAge:   defaultCleanupDLQAge,
		defaultCleanupBatch:    defaultCleanupBatch,
		tracer:                 otel.Tracer(tracerName),
	}

	for _, o := range opts {
		o(q)
	}

	return q, nil
}

func (q *sqlq) Run() {
	initCtx, initSpan := q.tracer.Start(context.Background(), "sqlq.init_schema")
	defer initSpan.End()

	if err := q.driver.InitSchema(initCtx); err != nil {
		initSpan.RecordError(err)
		// Consider if initialization failure should prevent startup
	}
}

// Publish adds a new job to the queue
func (q *sqlq) Publish(ctx context.Context, jobType string, payload any, opts ...PublishOption) error {
	ctx, span := q.tracer.Start(ctx, "sqlq.publish",
		trace.WithAttributes(
			attribute.String("sqlq.job_type", jobType),
		),
		trace.WithSpanKind(trace.SpanKindProducer),
	)

	defer span.End()

	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Note: Rollback is deferred before PublishTx, so if PublishTx fails,
	// the rollback happens automatically. If PublishTx succeeds, we commit.
	// Check the rollback error unless it's because the transaction was already committed/rolled back.
	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			span.AddEvent("Failed to rollback transaction in Publish", trace.WithAttributes(attribute.String("error", rErr.Error())))
		}
	}()

	if err := q.PublishTx(ctx, tx, jobType, payload, opts...); err != nil {
		// PublishTx already records its internal errors in its own span
		return err // Return the error from PublishTx
	}

	err = tx.Commit()
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// PublishTx adds a new job to the queue within an existing transaction
func (q *sqlq) PublishTx(ctx context.Context, tx *sql.Tx, jobType string, payload any, opts ...PublishOption) error {
	// Start a new span as a child of the context passed in.
	// It's assumed the caller (Publish or external) started a relevant parent span.
	ctx, span := q.tracer.Start(ctx, "sqlq.publish_tx",
		trace.WithAttributes(
			attribute.String("sqlq.job_type", jobType),
		),
		trace.WithSpanKind(trace.SpanKindProducer),
	)
	defer span.End()

	options := &publishOptions{}

	// Apply provided options
	for _, o := range opts {
		o(options)
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to marshal payload: %w", err)
	}
	span.SetAttributes(
		attribute.Int("sqlq.payload_size", len(payloadBytes)),
		attribute.Int("sqlq.delay_ms", int(options.delay.Milliseconds())),
	)

	traceContextMap := propagation.MapCarrier(make(map[string]string))
	otel.GetTextMapPropagator().Inject(ctx, traceContextMap) // Use the context returned by Start

	err = q.driver.InsertJob(ctx, jobType, payloadBytes, options.delay, traceContextMap)
	if err != nil {
		// The driver method should record the specific DB error in its span.
		// We record a higher-level error here.
		span.RecordError(err)
		return fmt.Errorf("failed to insert job: %w", err)
	}

	return nil
}

// Consume registers a handler for a specific job type.
// ctx passed to handler will be a child of ctx passed to Consume.
// Registering multiple handlers for same jobType will cause ErrDuplicateConsumer.
func (q *sqlq) Consume(
	ctx context.Context,
	jobType string,
	consumerName string,
	handler func(ctx context.Context, tx *sql.Tx, payloadBytes []byte) error,
	opts ...ConsumerOption,
) error {
	consCtx, cancel := context.WithCancel(ctx)

	cons := &consumer{
		jobType:         jobType,
		consumerName:    consumerName,
		handler:         handler,
		concurrency:     q.defaultConcurrency,
		prefetchCount:   q.defaultPrefetchCount,
		maxRetries:      q.defaultMaxRetries,
		pollInterval:    q.defaultPollInterval,
		jobTimeout:      q.defaultJobTimeout,
		backoffFunc:     q.defaultBackoffFunc,
		cleanupInterval: q.defaultCleanupInterval, // Use default from sqlq struct
		cleanupBatch:    q.defaultCleanupBatch,    // Use default from sqlq struct
		// cleanupAge and cleanupDLQAge start as nil, consumer options can override
		ctx:    consCtx,
		cancel: cancel,
	}

	for _, o := range opts {
		o(cons)
	}

	// Options sanity adjustments
	if cons.prefetchCount < cons.concurrency {
		cons.prefetchCount = cons.concurrency
	}

	// It makes sense to have channel be of size `prefetchCount`
	// since it allows channel to essentially never be empty
	// in cases where `prefetchCount` > `concurrency`
	cons.jobsChan = make(chan job, cons.prefetchCount)

	q.consumersMapMutex.Lock()
	if _, exists := q.consumersMap[jobType]; exists {
		q.consumersMapMutex.Unlock()
		return fmt.Errorf("%w: %s", ErrDuplicateConsumer, jobType)
	}
	q.consumersMap[jobType] = cons
	q.consumersMapMutex.Unlock()

	// Start one poller that will query database and post to channel
	go q.pollConsumer(cons)

	// Start `concurrency` worker goroutines that will read the channel and do work
	for range cons.concurrency {
		go q.workerLoop(cons)
	}

	// Start the consumer-specific cleanup loop if interval is positive
	if cons.cleanupInterval > 0 {
		go cons.runCleanupLoop(q.driver, q.tracer)
	}

	return nil
}

func (q *sqlq) pollConsumer(cons *consumer) {
	cons.workerWg.Add(1)
	defer cons.workerWg.Done()

	ticker := time.NewTicker(cons.pollInterval)
	defer ticker.Stop()

	var attempt uint16
	for {
		if err := q.fetchJobsForConsumer(cons.ctx, cons); err != nil && !errors.Is(err, context.Canceled) {
			// only start span in case of error since we don't want
			// long-running traces of long-running background processes
			_, span := q.tracer.Start(cons.ctx, "sqlq.fetch_jobs_for_consumer")

			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to fetch jobs")
			span.SetAttributes(
				attribute.String("consumer_name", cons.consumerName),
				attribute.String("job_type", cons.jobType),
			)
			span.End()

			backoffTimeout := q.defaultBackoffFunc(attempt)
			attempt++
			<-time.After(backoffTimeout)
			continue
		}

		attempt = 0

		select {
		case <-cons.ctx.Done():
			// Context was canceled
			return
		case <-q.shutdown:
			// Check global shutdown signal
			return
		case <-ticker.C:
			// Timer expired, continue to next poll
			continue
		}
	}
}

// workerLoop processes jobs from the processingJobs channel
func (q *sqlq) workerLoop(cons *consumer) {
	cons.workerWg.Add(1)
	defer cons.workerWg.Done()

	for {
		select {
		case <-cons.ctx.Done():
			return
		case job, ok := <-cons.jobsChan:
			if !ok {
				return // Channel closed
			}

			q.processJob(cons, &job)
		}
	}
}

func (q *sqlq) processJob(cons *consumer, job *job) {
	parentCtx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.MapCarrier(job.TraceContext))

	traceContextBytes, _ := json.Marshal(job.TraceContext)
	consumeCtx, consumeSpan := q.tracer.Start(parentCtx, "sqlq.consume",
		trace.WithLinks(trace.Link{
			SpanContext: trace.SpanContextFromContext(parentCtx),
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

	tx, err := q.db.BeginTx(consumeCtx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		consumeSpan.RecordError(err)
		consumeSpan.RecordError(err)
		consumeSpan.SetStatus(codes.Error, "failed to begin transaction")
		_ = tx.Rollback() // Ensure rollback on begin error
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

	handlerCtx, handlerSpan := q.tracer.Start(handlerCtx, "sqlq.handle") // Start span with potentially timed-out context

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
		markCtx, markSpan := q.tracer.Start(consumeCtx, "sqlq.mark_processed") // Use original consumeCtx
		markErr := q.driver.MarkJobProcessed(markCtx, job.ID, cons.consumerName)
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
		retryCtx, retrySpan := q.tracer.Start(consumeCtx, "sqlq.retry") // Use original consumeCtx
		retryErr := q.retryJob(retryCtx, cons, job, finalErr.Error())
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
	dlqCtx, dlqSpan := q.tracer.Start(consumeCtx, "sqlq.move_to_dlq") // Use original consumeCtx
	moveErr := q.driver.MoveToDeadLetterQueue(dlqCtx, job.ID, finalErr.Error())
	if moveErr != nil {
		dlqSpan.RecordError(moveErr)
		dlqSpan.SetStatus(codes.Error, "failed to move job to DLQ")
	} else {
		consumeSpan.SetAttributes(attribute.Bool("sqlq.moved_to_dlq", true))
		dlqSpan.SetStatus(codes.Ok, "job moved to DLQ")
	}
	dlqSpan.End()
}

// retryJob increments the retry count and schedules the job to be retried after a backoff period
func (q *sqlq) retryJob(ctx context.Context, consumer *consumer, job *job, errorMsg string) error {
	ctx, span := q.tracer.Start(ctx, "sqlq.retry_job", trace.WithAttributes(
		attribute.Int64("sqlq.job_id", job.ID),
		attribute.Int("sqlq.retry_count", int(job.RetryCount)),
		attribute.String("sqlq.error_message", errorMsg),
	))
	defer span.End()

	// Calculate exponential backoff
	bFn := q.defaultBackoffFunc
	if consumer.backoffFunc != nil {
		bFn = consumer.backoffFunc
	}
	// Increment retry count *before* calculating backoff for the *next* attempt's delay
	backoff := bFn(job.RetryCount + 1)
	span.SetAttributes(attribute.Float64("sqlq.backoff_seconds", backoff.Seconds()))

	// Mark job as failed and reschedule in a single operation
	if err := q.driver.MarkJobFailedAndReschedule(ctx, job.ID, errorMsg, backoff); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to mark job as failed and reschedule: %w", err)
	}

	return nil
}

// GetDeadLetterJobs retrieves jobs from the dead letter queue
func (q *sqlq) GetDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]DeadLetterJob, error) {
	ctx, span := q.tracer.Start(ctx, "sqlq.get_dlq_jobs", trace.WithAttributes(
		attribute.String("sqlq.job_type", jobType),
		attribute.Int("sqlq.limit", limit),
	))
	defer span.End()

	jobs, err := q.driver.GetDeadLetterJobs(ctx, jobType, limit)
	if err != nil {
		span.RecordError(err)
	} else {
		span.SetAttributes(attribute.Int("sqlq.dlq_jobs_fetched", len(jobs)))
	}

	return jobs, err
}

// RequeueDeadLetterJob moves a job from the dead letter queue back to the main queue
func (q *sqlq) RequeueDeadLetterJob(ctx context.Context, dlqID int64) error {
	ctx, span := q.tracer.Start(ctx, "sqlq.requeue_dlq_job", trace.WithAttributes( // Corrected span name
		attribute.Int64("sqlq.dlq_id", dlqID),
	))
	defer span.End()

	err := q.driver.RequeueDeadLetterJob(ctx, dlqID)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

// Shutdown stops the job processing
func (q *sqlq) Shutdown() {
	close(q.shutdown)

	// Cancel all consumer contexts and wait for workers to finish
	q.consumersMapMutex.Lock()
	for _, cons := range q.consumersMap {
		cons.cancel()
		cons.workerWg.Wait()
	}
	q.consumersMapMutex.Unlock()
}

// runCleanupLoop periodically cleans up old jobs for a specific consumer/jobType
func (cons *consumer) runCleanupLoop(driver Driver, tracer trace.Tracer) {
	cons.workerWg.Add(1)       // Track the cleanup goroutine in the consumer's WaitGroup
	defer cons.workerWg.Done() // Signal completion to the consumer's WaitGroup

	// Use the consumer's specific interval
	ticker := time.NewTicker(cons.cleanupInterval)
	defer ticker.Stop()

	// Determine effective cleanup ages, using consumer's configured defaults (which came from sqlq initially)
	// These are pointers, so we need to dereference them if set, otherwise use the package-level defaults.
	processedAge := defaultCleanupAge // Fallback to package default if not set via options
	if cons.cleanupAge != nil {
		processedAge = *cons.cleanupAge
	}
	dlqAge := defaultCleanupDLQAge // Fallback to package default if not set via options
	if cons.cleanupDLQAge != nil {
		dlqAge = *cons.cleanupDLQAge
	}

	for {
		select {
		case <-cons.ctx.Done(): // Use consumer's context for shutdown
			return
		case <-ticker.C:
			cons.performCleanup(driver, tracer, processedAge, dlqAge)
		}
	}
}

// performCleanup calls the driver to delete old jobs for this consumer's job type
func (c *consumer) performCleanup(driver Driver, tracer trace.Tracer, processedMaxAge, dlqMaxAge time.Duration) {
	// Create a background context for the cleanup operation itself
	ctx, span := tracer.Start(context.Background(), "sqlq.consumer.cleanup", trace.WithAttributes(
		attribute.String("sqlq.job_type", c.jobType),
		attribute.String("sqlq.consumer_name", c.consumerName),
		attribute.String("sqlq.cleanup.age.processed", processedMaxAge.String()),
		attribute.String("sqlq.cleanup.age.dlq", dlqMaxAge.String()),
		attribute.Int("sqlq.cleanup.batch_size", int(c.cleanupBatch)),
	))
	defer span.End()

	processedDeleted, dlqDeleted, err := driver.CleanupJobs(ctx, c.jobType, processedMaxAge, dlqMaxAge, c.cleanupBatch)

	span.SetAttributes(
		attribute.Int64("sqlq.cleanup.processed_deleted", processedDeleted),
		attribute.Int64("sqlq.cleanup.dlq_deleted", dlqDeleted),
	)

	if err != nil {
		// Use a standard logger or integrate with a proper logging library
		log.Printf("sqlq: cleanup failed for job_type %s: %v", c.jobType, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "cleanup failed")
	} else {
		span.SetStatus(codes.Ok, "cleanup completed")
	}
}

// fetchJobsForConsumer fetches jobs for a specific consumer and sends them to worker goroutines
func (q *sqlq) fetchJobsForConsumer(ctx context.Context, cons *consumer) error {
	jobs, err := q.driver.GetJobsForConsumer(ctx, cons.consumerName, cons.jobType, cons.prefetchCount)
	if err != nil {
		return fmt.Errorf("failed to query jobs: %w", err)
	}

	for _, job := range jobs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case cons.jobsChan <- job:
		}
	}

	return nil
}
