package sqlq

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/dir01/sqlq"

type JobsQueue interface {
	Publish(ctx context.Context, jobType string, payload any, opts ...PublishOption) error
	PublishTx(ctx context.Context, tx *sql.Tx, jobType string, payload any, opts ...PublishOption) error
	Consume(ctx context.Context, jobType string, consumerName string, f func(ctx context.Context, tx *sql.Tx, payloadBytes []byte) error, opts ...ConsumerOption)
	GetDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]DeadLetterJob, error)
	RequeueDeadLetterJob(ctx context.Context, dlqID int64) error
	Shutdown()
	Run()
}

// DBType represents the type of database being used
type DBType string

const (
	DBTypeSQLite   DBType = "sqlite"
	DBTypePostgres DBType = "postgres"
	DBTypeMySQL    DBType = "mysql"
)

// Error definitions
var (
	ErrUnsupportedDBType  = errors.New("unsupported database type")
	ErrMaxRetriesExceeded = errors.New("maximum retries exceeded")
	ErrJobNotFound        = errors.New("job not found")
)

const (
	defaultMaxRetries = 3
	infiniteRetries   = -1
)

var (
	defaultConcurrency = min(runtime.NumCPU(), runtime.GOMAXPROCS(0))
	defaultPrefetch    = defaultConcurrency
)

// sqlq implements the JobsQueue interface using SQL databases
type sqlq struct {
	db                  *sql.DB
	driver              Driver
	consumersMap        map[string][]*consumer
	consumersMapMutex   sync.RWMutex
	shutdown            chan struct{}
	wg                  sync.WaitGroup
	backoffFunc         func(retryNum int) time.Duration
	defaultPollInterval time.Duration
	tracer              trace.Tracer
}

type consumer struct {
	jobType       string
	consumerName  string
	handler       func(ctx context.Context, tx *sql.Tx, payloadBytes []byte) error
	concurrency   int
	prefetchCount int
	maxRetries    int
	backoffFunc   func(retryNum int) time.Duration
	pollInterval  time.Duration

	processingJobs chan job
	workerWg       sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
}

type job struct {
	ID           int64
	JobType      string
	Payload      []byte
	CreatedAt    time.Time
	RetryCount   int
	TraceContext map[string]string // Added for trace propagation
}

func (j job) String() string {
	bytes, _ := json.MarshalIndent(j, "", " ")
	return string(bytes)
}

// DeadLetterJob represents a job that has been moved to the dead letter queue
type DeadLetterJob struct {
	ID            int64
	OriginalID    int64
	JobType       string
	Payload       []byte
	CreatedAt     time.Time
	FailedAt      time.Time
	RetryCount    int
	MaxRetries    int
	FailureReason string
}

// New creates a new SQL-backed job queue
func New(db *sql.DB, dbType DBType, opts ...NewOption) (JobsQueue, error) {
	driver, err := GetDriver(db, dbType)
	if err != nil {
		return nil, err
	}

	q := &sqlq{
		db:           db,
		driver:       driver,
		consumersMap: make(map[string][]*consumer),
		shutdown:     make(chan struct{}),
		backoffFunc: func(retryNum int) time.Duration {
			jitter := float64(rand.Intn(retryNum))
			backoff := math.Pow(2, float64(retryNum))
			return time.Duration(backoff+jitter) * time.Second
		},
		defaultPollInterval: 1 * time.Second,
		tracer:              otel.Tracer(tracerName), // Initialize the tracer
	}

	for _, o := range opts {
		o(q)
	}

	// Initialize the database schema (using background context for initialization)
	initCtx, initSpan := q.tracer.Start(context.Background(), "sqlq.InitSchema")
	defer initSpan.End()

	if err := q.driver.InitSchema(initCtx); err != nil {
		initSpan.RecordError(err)
		initSpan.End()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return q, nil
}

// Publish adds a new job to the queue
func (q *sqlq) Publish(ctx context.Context, jobType string, payload any, opts ...PublishOption) error {
	ctx, span := q.tracer.Start(ctx, "sqlq.Publish", trace.WithAttributes(
		attribute.String("sqlq.job_type", jobType),
	))
	defer span.End()

	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Note: Rollback is deferred before PublishTx, so if PublishTx fails,
	// the rollback happens automatically. If PublishTx succeeds, we commit.
	defer tx.Rollback()

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
	ctx, span := q.tracer.Start(ctx, "sqlq.PublishTx", trace.WithAttributes(
		attribute.String("sqlq.job_type", jobType),
	))
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
	span.SetAttributes(attribute.Int("sqlq.payload_size", len(payloadBytes)))

	// Inject trace context for propagation
	traceContextMap := make(map[string]string)
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(traceContextMap))
	span.SetAttributes(attribute.String("sqlq.injected_trace_context", fmt.Sprintf("%v", traceContextMap)))

	// Pass the context and trace context map to the driver method
	err = q.driver.InsertJob(ctx, jobType, payloadBytes, options.delay, traceContextMap)
	if err != nil {
		// The driver method should record the specific DB error in its span.
		// We record a higher-level error here.
		span.RecordError(err)
		return fmt.Errorf("failed to insert job: %w", err)
	}

	return nil
}

// Consume registers a handler for a specific job type
func (q *sqlq) Consume(
	ctx context.Context,
	jobType string,
	consumerName string,
	f func(ctx context.Context, tx *sql.Tx, payloadBytes []byte) error,
	opts ...ConsumerOption,
) {
	q.consumersMapMutex.Lock()
	defer q.consumersMapMutex.Unlock()

	consCtx, cancel := context.WithCancel(ctx)

	cons := &consumer{
		jobType:       jobType,
		consumerName:  consumerName,
		handler:       f,
		concurrency:   defaultConcurrency,
		prefetchCount: defaultPrefetch,
		maxRetries:    defaultMaxRetries,
		ctx:           consCtx,
		pollInterval:  q.defaultPollInterval,
		cancel:        cancel,
	}

	// Apply provided options
	for _, o := range opts {
		o(cons)
	}

	// Options sanity adjustments
	if cons.prefetchCount < cons.concurrency {
		cons.prefetchCount = cons.concurrency
	}
	cons.processingJobs = make(chan job, cons.prefetchCount)

	q.consumersMap[jobType] = append(q.consumersMap[jobType], cons)

	// Start worker goroutines
	for i := range cons.concurrency {
		cons.workerWg.Add(1)
		// Pass the consumer's context and worker index to the loop
		go q.workerLoop(cons, i)
	}
}

// workerLoop processes jobs from the processingJobs channel
func (q *sqlq) workerLoop(cons *consumer, workerID int) {
	defer cons.workerWg.Done()
	workerName := fmt.Sprintf("%s-worker-%d", cons.consumerName, workerID)

	for {
		// Create a root span for waiting for a job.
		// This might be long-running if the channel is empty.
		waitCtx, waitSpan := q.tracer.Start(cons.ctx, fmt.Sprintf("%s.WaitForJob", workerName))
		// Use the waitCtx for the select
		select {
		case <-waitCtx.Done(): // Use waitCtx here
			waitSpan.End() // End span if context is cancelled
			return
		case job, ok := <-cons.processingJobs:
			waitSpan.End() // End the waiting span once a job is received
			if !ok {
				return // Channel closed
			}

			// Extract the parent trace context from the job
			// Use context.Background() as the base, as cons.ctx might be cancelled independently.
			parentCtx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.MapCarrier(job.TraceContext))

			// Start a new span for processing this specific job, linked to the parent context.
			// Use cons.ctx as the *local* parent context for cancellation signals, etc.
			// but link it to the *remote* parentCtx for tracing.
			jobCtx, jobSpan := q.tracer.Start(
				trace.ContextWithRemoteSpanContext(cons.ctx, trace.SpanContextFromContext(parentCtx)),
				fmt.Sprintf("%s.ProcessJob", workerName),
				trace.WithAttributes(
					attribute.String("sqlq.job_type", cons.jobType),
					attribute.Int64("sqlq.job_id", job.ID),
					attribute.Int("sqlq.retry_count", job.RetryCount),
					attribute.String("sqlq.extracted_trace_context", fmt.Sprintf("%v", job.TraceContext)),
				),
				trace.WithSpanKind(trace.SpanKindConsumer), // Mark this as a consumer span
			)

			tx, err := q.db.BeginTx(jobCtx, &sql.TxOptions{Isolation: sql.LevelDefault}) // Use jobCtx which has the correct span
			if err != nil {
				//log.Printf("[%s] Failed to begin transaction for job %d: %v", workerName, job.ID, err)
				jobSpan.RecordError(err)
				jobSpan.End() // End span since we can't process further
				continue      // Try the next job
			}

			// Process the job within its own span
			processCtx, processSpan := q.tracer.Start(jobCtx, fmt.Sprintf("%s.Handler", workerName))
			log.Printf("[%s] Pre handler of job %d", workerName, job.ID)
			consErr := cons.handler(processCtx, tx, job.Payload) // Pass processCtx to handler
			log.Printf("[%s] Post handler of job %d", workerName, job.ID)
			if consErr != nil {
				processSpan.RecordError(consErr)
				jobSpan.SetAttributes(attribute.String("sqlq.handler_error", consErr.Error()))
			}
			processSpan.End()

			// Handle transaction and job state based on handler result
			if consErr == nil {
				// Mark processed within the job span
				markCtx, markSpan := q.tracer.Start(jobCtx, fmt.Sprintf("%s.MarkProcessed", workerName))
				markErr := q.driver.MarkJobProcessed(markCtx, job.ID, cons.consumerName) // Pass markCtx
				markSpan.End()

				if markErr != nil && !errors.Is(markErr, context.Canceled) {
					log.Printf("[%s] Failed to mark job %d as processed: %v", workerName, job.ID, markErr)
					jobSpan.RecordError(fmt.Errorf("mark job processed failed: %w", markErr))
					_ = tx.Rollback() // Rollback on failure to mark processed
				} else {
					// Commit transaction
					commitErr := tx.Commit()
					if commitErr != nil && !errors.Is(commitErr, sql.ErrTxDone) && !errors.Is(commitErr, context.Canceled) {
						log.Printf("[%s] Failed to commit transaction for job %d: %v", workerName, job.ID, commitErr)
						jobSpan.RecordError(fmt.Errorf("commit failed: %w", commitErr))
						// Job was processed by handler, but commit failed. State is ambiguous.
					}
				}
			} else {
				// Handler failed, rollback transaction
				_ = tx.Rollback() // Rollback is best-effort

				// Handle retry or DLQ logic within the job span
				if cons.maxRetries == infiniteRetries || job.RetryCount < cons.maxRetries {
					retryCtx, retrySpan := q.tracer.Start(jobCtx, fmt.Sprintf("%s.RetryJob", workerName))
					retryErr := q.retryJob(retryCtx, cons, job, consErr.Error()) // Pass retryCtx
					if retryErr != nil {
						log.Printf("[%s] Failed to schedule retry for job %d: %v", workerName, job.ID, retryErr)
						retrySpan.RecordError(retryErr)
						jobSpan.RecordError(fmt.Errorf("retry scheduling failed: %w", retryErr))
					}
					retrySpan.End()
				} else {
					dlqCtx, dlqSpan := q.tracer.Start(jobCtx, fmt.Sprintf("%s.MoveToDLQ", workerName))
					moveErr := q.driver.MoveToDeadLetterQueue(dlqCtx, job.ID, consErr.Error()) // Pass dlqCtx
					if moveErr != nil {
						log.Printf("[%s] Failed to move job %d to dead letter queue: %v", workerName, job.ID, moveErr)
						dlqSpan.RecordError(moveErr)
						jobSpan.RecordError(fmt.Errorf("move to DLQ failed: %w", moveErr))
					} else {
						log.Printf("[%s] Job %d exceeded max retries (%d) and moved to DLQ: %v", workerName, job.ID, cons.maxRetries, consErr)
						jobSpan.SetAttributes(attribute.Bool("sqlq.moved_to_dlq", true))
					}
					dlqSpan.End()
				}
			}
			jobSpan.End() // End the overall job processing span
		}
	}
}

// retryJob increments the retry count and schedules the job to be retried after a backoff period
func (q *sqlq) retryJob(ctx context.Context, consumer *consumer, job job, errorMsg string) error {
	ctx, span := q.tracer.Start(ctx, "sqlq.retryJob", trace.WithAttributes(
		attribute.Int64("sqlq.job_id", job.ID),
		attribute.Int("sqlq.retry_count", job.RetryCount),
		attribute.String("sqlq.error_message", errorMsg),
	))
	defer span.End()

	// Calculate exponential backoff
	bFn := q.backoffFunc
	if consumer.backoffFunc != nil {
		bFn = consumer.backoffFunc
	}
	backoff := bFn(job.RetryCount)
	span.SetAttributes(attribute.Float64("sqlq.backoff_seconds", backoff.Seconds()))

	// Mark job as failed and reschedule in a single operation
	if err := q.driver.MarkJobFailedAndReschedule(ctx, job.ID, errorMsg, backoff); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to mark job as failed and reschedule: %w", err)
	}

	return nil
}

// Run starts the job processing loop
func (q *sqlq) Run() {
	q.wg.Add(1)

	go func() {
		defer q.wg.Done()

		// Each consumer will be polled at their own interval
		for {
			select {
			case <-q.shutdown:
				return
			default:
				if err := q.processJobs(); err != nil {
					log.Printf("Error processing jobs: %v", err)
				}
				time.Sleep(100 * time.Millisecond) // Small sleep to prevent tight loop
			}
		}
	}()
}

// GetDeadLetterJobs retrieves jobs from the dead letter queue
func (q *sqlq) GetDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]DeadLetterJob, error) {
	ctx, span := q.tracer.Start(ctx, "sqlq.GetDeadLetterJobs", trace.WithAttributes(
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
	ctx, span := q.tracer.Start(ctx, "sqlq.RequeueDeadLetterJob", trace.WithAttributes(
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
	for _, consumers := range q.consumersMap {
		for _, cons := range consumers {
			cons.cancel()
			cons.workerWg.Wait()
		}
	}
	q.consumersMapMutex.Unlock()

	q.wg.Wait()
}

// processJobs polls for and processes available jobs
func (q *sqlq) processJobs() error {
	q.consumersMapMutex.RLock()
	defer q.consumersMapMutex.RUnlock()

	// If no consumers, nothing to do
	if len(q.consumersMap) == 0 {
		return nil
	}

	// Process for each job type
	for jobType, consumers := range q.consumersMap {
		for _, cons := range consumers {

			// Use a separate goroutine for each consumer to respect their individual poll intervals
			go func(cons *consumer, jobType string) {
				timer := time.NewTimer(cons.pollInterval)
				defer timer.Stop()

				// Fetch jobs within a span, using the consumer's context
				fetchCtx, fetchSpan := q.tracer.Start(cons.ctx, "sqlq.fetchJobsForConsumer", trace.WithAttributes(
					attribute.String("sqlq.consumer_name", cons.consumerName),
					attribute.String("sqlq.job_type", jobType),
				))

				if err := q.fetchJobsForConsumer(fetchCtx, cons); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf(
						"Error fetching jobs for consumer %s of type %s: %v",
						cons.consumerName, jobType, err,
					)
					fetchSpan.RecordError(err)
				}
				fetchSpan.End()

				// Wait for poll interval or context cancellation
				select {
				case <-timer.C:
					// Timer expired, continue to next poll
				case <-cons.ctx.Done(): // Check consumer's context
					// Context was canceled
					return
				case <-q.shutdown: // Also check global shutdown signal
					return
				}
			}(cons, jobType)
		}
	}

	return nil
}

// fetchJobsForConsumer fetches jobs for a specific consumer and sends them to worker goroutines
func (q *sqlq) fetchJobsForConsumer(ctx context.Context, cons *consumer) error {
	// Span is started by the caller (processJobs)

	// FIXME: drop this
	// log.Printf("fetching jobs for consumer %s", cons.consumerName)

	// Find jobs that haven't been processed by this consumer
	jobs, err := q.driver.GetJobsForConsumer(ctx, cons.consumerName, cons.jobType, cons.prefetchCount) // Pass context
	if err != nil {
		// Let caller record error in span
		return fmt.Errorf("failed to query jobs: %w", err)
	}

	// Add fetched count to the existing span from the caller
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(attribute.Int("sqlq.jobs_fetched", len(jobs)))
	}

	// FIXME: drop this
	// log.Printf("found %d jobs for consumer %s", len(jobs), cons.consumerName)

	for _, job := range jobs {
		select {
		case <-ctx.Done(): // Use the passed context which includes the span
			return ctx.Err() // Return context error
		case cons.processingJobs <- job:
			// TODO: Potentially add an event/log here if needed
		}
	}

	return nil
}
