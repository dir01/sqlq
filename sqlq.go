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
	consumersMap        map[string][]*consumer // jobType -> consumerList
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
	TraceContext map[string]string // For trace propagation
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

	return q, nil
}

func (q *sqlq) Run() {
	initCtx, initSpan := q.tracer.Start(context.Background(), "sqlq.init_schema")
	defer initSpan.End()

	if err := q.driver.InitSchema(initCtx); err != nil {
		initSpan.RecordError(err)
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

	traceContextMap := make(map[string]string)
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(traceContextMap))

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
	cons.processingJobs = make(chan job, cons.prefetchCount)

	q.consumersMapMutex.Lock()
	q.consumersMap[jobType] = append(q.consumersMap[jobType], cons)
	q.consumersMapMutex.Unlock()

	// Start one poller that will query database and post to channel
	cons.workerWg.Add(1)
	go q.pollConsumer(cons)

	// Start `concurrency` worker goroutines that will read the channel and do work
	for range cons.concurrency {
		cons.workerWg.Add(1)
		go q.workerLoop(cons)
	}
}

func (q *sqlq) pollConsumer(cons *consumer) {
	timer := time.NewTimer(cons.pollInterval)
	defer timer.Stop()
	defer cons.workerWg.Done()

	var attempt uint
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

			backoffTimeout := q.backoffFunc(int(attempt))
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
		case <-timer.C:
			// Timer expired, continue to next poll
			continue
		}
	}
}

// workerLoop processes jobs from the processingJobs channel
func (q *sqlq) workerLoop(cons *consumer) {
	defer cons.workerWg.Done()

	for {
		select {
		case <-cons.ctx.Done():
			return
		case job, ok := <-cons.processingJobs:
			if !ok {
				return // Channel closed
			}

			q.processJob(cons, &job)
		}
	}
}

func (q *sqlq) processJob(cons *consumer, job *job) {
	parentCtx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.MapCarrier(job.TraceContext))

	consumeCtx, consumeSpan := q.tracer.Start(parentCtx, "sqlq.consume",
		trace.WithLinks(trace.Link{
			SpanContext: trace.SpanContextFromContext(parentCtx),
		}),
		trace.WithAttributes(
			attribute.String("sqlq.job_type", cons.jobType),
			attribute.Int64("sqlq.job_id", job.ID),
			attribute.Int("sqlq.retry_count", job.RetryCount),
		),
		trace.WithSpanKind(trace.SpanKindConsumer),
	)

	defer consumeSpan.End()

	tx, err := q.db.BeginTx(consumeCtx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		consumeSpan.RecordError(err)
		return
	}

	handlerCtx, handlerSpan := q.tracer.Start(consumeCtx, "sqlq.handle")

	// THE MEAT
	handlerErr := cons.handler(handlerCtx, tx, job.Payload)
	if handlerErr != nil {
		handlerSpan.RecordError(handlerErr)
		consumeSpan.SetAttributes(attribute.String("sqlq.handler_error", handlerErr.Error()))
	}
	handlerSpan.End()

	// happy path, mark as done and bail
	if handlerErr == nil {
		markCtx, markSpan := q.tracer.Start(consumeCtx, "sqlq.mark_processed")
		defer markSpan.End()

		markErr := q.driver.MarkJobProcessed(markCtx, job.ID, cons.consumerName)

		if markErr != nil && !errors.Is(markErr, context.Canceled) {
			markSpan.RecordError(markErr)
			_ = tx.Rollback()
		} else {
			commitErr := tx.Commit()
			if commitErr != nil && !errors.Is(commitErr, sql.ErrTxDone) && !errors.Is(commitErr, context.Canceled) {
				markSpan.RecordError(commitErr)
			}
		}

		return
	}

	// Handler failed, rollback transaction
	_ = tx.Rollback()

	// retry
	if cons.maxRetries == infiniteRetries || job.RetryCount < cons.maxRetries {
		retryCtx, retrySpan := q.tracer.Start(consumeCtx, "sqlq.retry")
		defer retrySpan.End()

		retryErr := q.retryJob(retryCtx, cons, job, handlerErr.Error())
		if retryErr != nil {
			retrySpan.RecordError(retryErr)
		}

		return
	}

	// dead letter queue
	dlqCtx, dlqSpan := q.tracer.Start(consumeCtx, "sqlq.move_to_dlq")
	defer dlqSpan.End()

	moveErr := q.driver.MoveToDeadLetterQueue(dlqCtx, job.ID, handlerErr.Error())
	if moveErr != nil {
		dlqSpan.RecordError(moveErr)
	} else {
		consumeSpan.SetAttributes(attribute.Bool("sqlq.moved_to_dlq", true))
	}
}

// retryJob increments the retry count and schedules the job to be retried after a backoff period
func (q *sqlq) retryJob(ctx context.Context, consumer *consumer, job *job, errorMsg string) error {
	ctx, span := q.tracer.Start(ctx, "sqlq.retry_job", trace.WithAttributes(
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
	ctx, span := q.tracer.Start(ctx, "sqlq.requeue_dlq_jobs", trace.WithAttributes(
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
		case cons.processingJobs <- job:
		}
	}

	return nil
}
