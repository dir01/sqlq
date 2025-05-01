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
	"go.opentelemetry.io/otel/propagation"

	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/dir01/sqlq"

// JobsQueue defines the interface for interacting with the job queue.
type JobsQueue interface {
	// Publish adds a new job to the queue.
	Publish(ctx context.Context, jobType string, payload any, opts ...PublishOption) error

	// PublishTx adds a new job to the queue within an existing transaction.
	PublishTx(ctx context.Context, tx *sql.Tx, jobType string, payload any, opts ...PublishOption) error

	// Consume registers a handler function for a specific job type.
	Consume(
		ctx context.Context,
		jobType string,
		f func(ctx context.Context, tx *sql.Tx, payloadBytes []byte) error, opts ...ConsumerOption,
	) error

	// GetDeadLetterJobs retrieves jobs from the dead letter queue, optionally filtered by job type.
	GetDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]DeadLetterJob, error)

	// RequeueDeadLetterJob moves a job from the dead letter queue back to the main queue, identified by its original job ID.
	RequeueDeadLetterJob(ctx context.Context, originalJobID int64) error

	// Shutdown gracefully stops all consumers and background processes.
	Shutdown()

	// Run performs initial setup like schema initialization.
	Run()
}

// DBType represents the supported database types.
type DBType string

const (
	// DBTypeSQLite identifies the SQLite database type.
	DBTypeSQLite DBType = "sqlite"
	// DBTypePostgres identifies the PostgreSQL database type.
	DBTypePostgres DBType = "postgres"
)

var (
	// ErrDuplicateConsumer is returned when Consume is called multiple times for the same job type.
	ErrDuplicateConsumer = errors.New(".Consume was called twice for same job type")
	// ErrUnsupportedDBType is returned when an unsupported database type is provided to New.
	ErrUnsupportedDBType = errors.New("unsupported database type")
	// ErrMaxRetriesExceeded indicates a job failed more times than its configured maximum retries.
	ErrMaxRetriesExceeded = errors.New("maximum retries exceeded")
	// ErrJobNotFound indicates that a job with the specified ID was not found (e.g., in DLQ operations).
	ErrJobNotFound = errors.New("job not found")
)

var (
	defaultMaxRetries               int32  = 3
	infiniteRetries                 int32  = -1
	defaultConcurrency                     = uint16(min(runtime.NumCPU(), runtime.GOMAXPROCS(0)))
	defaultPrefetchCount                   = defaultConcurrency
	defaultJobTimeout                      = 15 * time.Minute
	defaultCleanupProcessedInterval        = 1 * time.Hour
	defaultCleanupProcessedAge             = 7 * 24 * time.Hour
	defaultCleanupDLQInterval              = 6 * time.Hour
	defaultCleanupDLQAge                   = 30 * 24 * time.Hour
	defaultCleanupBatch             uint16 = 500
)

// sqlq implements the JobsQueue interface using SQL databases
type sqlq struct {
	db                 *sql.DB
	driver             driver
	defaultBackoffFunc func(retryNum uint16) time.Duration
	tracer             trace.Tracer
	consumersMap       map[string]*consumer // jobType -> consumer
	consumersMapMutex  sync.RWMutex         // guard consumersMap //nolint:revive // Field name is descriptive

	// Function for calculating how much of a delay to use when rescheduling a failed job.
	// This is a default that will be used for all consumers
	// unless they define their own backoff function using WithConsumerBackoffFunc.

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

	// Default interval for cleaning up processed jobs. Default is 1 hour.
	// Individual consumers may override this using WithConsumerCleanupProcessedInterval.
	defaultCleanupProcessedInterval time.Duration

	// Default maximum age for successfully processed jobs before deletion. Default is 7 days.
	// Individual consumers may override this using WithConsumerCleanupProcessedAge.
	defaultCleanupProcessedAge time.Duration

	// Default interval for cleaning up DLQ jobs. Default is 6 hours.
	// Individual consumers may override this using WithConsumerCleanupDLQInterval.
	defaultCleanupDLQInterval time.Duration

	// Default maximum age for dead-letter queue jobs before deletion. Default is 30 days.
	// Individual consumers may override this using WithConsumerCleanupDLQAge.
	defaultCleanupDLQAge time.Duration

	// Default number of jobs to delete in a single cleanup batch. Default is 500.
	// Individual consumers may override this using WithConsumerCleanupBatch.
	defaultCleanupBatch uint16
}

type job struct {
	JobType      string
	CreatedAt    time.Time
	TraceContext map[string]string
	Payload      []byte
	ID           int64
	RetryCount   uint16
}

// DeadLetterJob represents a job that has been moved to the dead letter queue
type DeadLetterJob struct {
	OriginalID    int64 // This is the primary key now
	JobType       string
	FailureReason string
	CreatedAt     time.Time
	FailedAt      time.Time
	Payload       []byte
	RetryCount    uint16
}

// New creates a new JobsQueue
func New(db *sql.DB, dbType DBType, opts ...NewOption) (JobsQueue, error) {
	driver, err := getDriver(db, dbType)
	if err != nil {
		return nil, err
	}

	q := &sqlq{
		db:                   db,
		driver:               driver,
		consumersMap:         make(map[string]*consumer),
		consumersMapMutex:    sync.RWMutex{},
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
		defaultCleanupProcessedInterval: defaultCleanupProcessedInterval,
		defaultCleanupProcessedAge:      defaultCleanupProcessedAge,
		defaultCleanupDLQInterval:       defaultCleanupDLQInterval,
		defaultCleanupDLQAge:            defaultCleanupDLQAge,
		defaultCleanupBatch:             defaultCleanupBatch,
		tracer:                          otel.Tracer(tracerName),
	}

	for _, o := range opts {
		o(q)
	}

	return q, nil
}

func (q *sqlq) Run() {
	initCtx, initSpan := q.tracer.Start(context.Background(), "sqlq.init_schema")
	defer initSpan.End()

	if err := q.driver.initSchema(initCtx); err != nil {
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

	defer func() {
		if rErr := tx.Rollback(); rErr != nil && !errors.Is(rErr, sql.ErrTxDone) {
			span.AddEvent(
				"Failed to rollback transaction in Publish",
				trace.WithAttributes(attribute.String("error", rErr.Error())),
			)
		}
	}()

	if err = q.PublishTx(ctx, tx, jobType, payload, opts...); err != nil {
		// PublishTx already records its internal errors in its own span
		return err
	}

	if err = tx.Commit(); err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// PublishTx adds a new job to the queue within an existing transaction.
func (q *sqlq) PublishTx(ctx context.Context, _ *sql.Tx, jobType string, payload any, opts ...PublishOption) error {
	ctx, span := q.tracer.Start(ctx, "sqlq.publish_tx",
		trace.WithAttributes(
			attribute.String("sqlq.job_type", jobType),
		),
		trace.WithSpanKind(trace.SpanKindProducer),
	)
	defer span.End()

	options := &publishOptions{delay: 0}
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
	otel.GetTextMapPropagator().Inject(ctx, traceContextMap)

	err = q.driver.insertJob(ctx, jobType, payloadBytes, options.delay, traceContextMap)
	if err != nil {
		// The driver method should record the specific DB error in its span. We record a higher-level error here.
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
	handler func(ctx context.Context, tx *sql.Tx, payloadBytes []byte) error,
	opts ...ConsumerOption,
) error {
	consCtx, cancel := context.WithCancel(ctx)

	cons := &consumer{
		jobType: jobType,
		handler: handler,
		// Inject dependencies from sqlq
		db:     q.db,
		driver: q.driver,
		tracer: q.tracer,
		// Set defaults from sqlq. Those might be overridden by opts later
		concurrency:              q.defaultConcurrency,
		prefetchCount:            q.defaultPrefetchCount,
		maxRetries:               q.defaultMaxRetries,
		pollInterval:             q.defaultPollInterval,
		jobTimeout:               q.defaultJobTimeout,
		backoffFunc:              q.defaultBackoffFunc,
		cleanupBatch:             q.defaultCleanupBatch,
		cleanupProcessedInterval: q.defaultCleanupProcessedInterval,
		cleanupProcessedAge:      q.defaultCleanupProcessedAge,
		cleanupDLQInterval:       q.defaultCleanupDLQInterval,
		cleanupDLQAge:            q.defaultCleanupDLQAge,
		// Initialize consumer-specific fields
		jobsChan: nil, // Will be initialized below
		workerWg: sync.WaitGroup{},
		ctx:      consCtx,
		cancel:   cancel,
	}

	// Apply consumer-specific options
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

	cons.start()

	return nil
}

// GetDeadLetterJobs retrieves jobs from the dead letter queue
func (q *sqlq) GetDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]DeadLetterJob, error) {
	ctx, span := q.tracer.Start(ctx, "sqlq.get_dlq_jobs", trace.WithAttributes(
		attribute.String("sqlq.job_type", jobType),
		attribute.Int("sqlq.limit", limit),
	))
	defer span.End()

	jobs, err := q.driver.getDeadLetterJobs(ctx, jobType, limit)
	if err != nil {
		span.RecordError(err)
	} else {
		span.SetAttributes(attribute.Int("sqlq.dlq_jobs_fetched", len(jobs)))
	}

	return jobs, err
}

// RequeueDeadLetterJob moves a job from the dead letter queue back to the main queue, identified by its original job ID.
func (q *sqlq) RequeueDeadLetterJob(ctx context.Context, originalJobID int64) error {
	ctx, span := q.tracer.Start(ctx, "sqlq.requeue_dlq_job", trace.WithAttributes(
		attribute.Int64("sqlq.original_job_id", originalJobID), // Use original_job_id attribute
	))
	defer span.End()

	err := q.driver.requeueDeadLetterJob(ctx, originalJobID) // Pass originalJobID to driver
	if err != nil {
		span.RecordError(err)
	}
	return err
}

// Shutdown stops the job processing
func (q *sqlq) Shutdown() {
	wg := sync.WaitGroup{}

	q.consumersMapMutex.Lock()
	for _, cons := range q.consumersMap {
		wg.Add(1)
		go func(c *consumer) {
			defer wg.Done()
			c.shutdown()
		}(cons)
	}
	q.consumersMapMutex.Unlock()

	wg.Wait()
}
