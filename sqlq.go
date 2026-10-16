package sqlq

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"runtime"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/dir01/sqlq"

// JobsQueue defines the interface for interacting with the job queue.
type JobsQueue interface {
	// Publish adds a new job to the queue.
	Publish(ctx context.Context, jobType string, payload any, opts ...PublishOption) error

	// PublishTx adds a new job to the queue within an existing transaction.
	// It never commits or rolls back tx. If you created tx, you must finish it;
	// if a handler or hook received tx from the queue, the queue finishes it.
	// A nil tx publishes immediately.
	PublishTx(ctx context.Context, tx *sql.Tx, jobType string, payload any, opts ...PublishOption) error

	// Consume registers a handler function for a specific job type.
	// The queue owns the handler's transaction: handlers must not commit or roll it back.
	// Do not defer tx.Rollback either. Return nil on success or an error to abort.
	// Handler writes and job completion commit together on success; failures roll back.
	Consume(
		ctx context.Context,
		jobType string,
		f func(ctx context.Context, tx *sql.Tx, payloadBytes []byte) error, opts ...ConsumerOption,
	) error

	// GetDeadLetterJobs retrieves jobs from the dead letter queue, optionally filtered by job type.
	GetDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]DeadLetterJob, error)

	// RequeueDeadLetterJob moves a job from the dead letter queue back to the main queue, identified by its original job ID.
	RequeueDeadLetterJob(ctx context.Context, originalJobID int64) error

	// Shutdown stops all consumers and their background work, and waits for them to stop.
	// It cancels the consumers first, so ctx limits only the wait. If ctx ends first, Shutdown
	// returns ctx's error: a handler that ignores its context keeps running, and the job it holds
	// becomes available again when its claim expires. Do not give a context that is already done,
	// such as the one that a shutdown signal canceled.
	// Consume fails with ErrShutDown once Shutdown has been called.
	Shutdown(ctx context.Context) error

	// Run creates or migrates the queue's tables. Call it before Publish and Consume, and stop if it
	// returns an error. ctx limits the wait, which on PostgreSQL includes the wait for the lock that
	// keeps processes from migrating at the same time.
	Run(ctx context.Context) error
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
	// ErrShutDown is returned when Consume is called once Shutdown has been, even while Shutdown is
	// still waiting for consumers: a consumer started then would outlive it.
	ErrShutDown = errors.New("queue is shut down")
	// ErrUnsupportedDBType is returned when an unsupported database type is provided to New.
	ErrUnsupportedDBType = errors.New("unsupported database type")
	// ErrMaxRetriesExceeded indicates a job failed more times than its configured maximum retries.
	ErrMaxRetriesExceeded = errors.New("maximum retries exceeded")
	// ErrJobNotFound indicates that a job with the specified ID was not found (e.g., in DLQ operations).
	ErrJobNotFound = errors.New("job not found")
	// ErrClaimLost means the consumer no longer owns the job, so its write had no effect.
	ErrClaimLost = errors.New("job is no longer claimed by this attempt")
	// ErrClaimTimeoutTooShort means the claim timeout cannot cover the job timeout and the margin
	// a worker needs before it starts a handler (see WithConsumerClaimTimeout), so handlers would
	// either outlive their claims or never start.
	ErrClaimTimeoutTooShort = errors.New("claim timeout is too short for the job timeout")
	// ErrPushNotSupported indicates that async push was configured, but selected db driver does not support it.
	ErrPushNotSupported = errors.New("async push is not supported")
	// ErrNilHandler is returned when Consume is given a nil handler.
	ErrNilHandler = errors.New("handler is nil")
	// ErrNilBackoffFunc is returned when Consume would use a nil backoff function, given by
	// WithConsumerBackoffFunc or WithDefaultBackoffFunc.
	ErrNilBackoffFunc = errors.New("backoff function is nil")
)

var (
	defaultMaxRetries               int32  = 3
	infiniteRetries                 int32  = -1
	defaultConcurrency                     = uint16(min(runtime.NumCPU(), runtime.GOMAXPROCS(0)))
	defaultJobTimeout                      = 15 * time.Minute
	defaultCleanupProcessedInterval        = 1 * time.Hour
	defaultCleanupProcessedAge             = 7 * 24 * time.Hour
	defaultCleanupDLQInterval              = 6 * time.Hour
	defaultCleanupDLQAge                   = 30 * 24 * time.Hour
	defaultCleanupBatch             uint16 = 500
)

// sqlq implements the JobsQueue interface using SQL databases
type sqlq struct {
	db     *sql.DB
	driver driver
	// Function for calculating how much of a delay to use when rescheduling a failed job.
	// This is a default that will be used for all consumers
	// unless they define their own backoff function using WithConsumerBackoffFunc.
	defaultBackoffFunc func(retryNum uint16) time.Duration
	tracer             trace.Tracer
	meterProvider      metric.MeterProvider
	// Stops the callback that reports queue depth and lag. Shutdown calls Unregister.
	metricsRegistration metric.Registration
	metricsCtx          context.Context
	cancelMetrics       context.CancelFunc
	metricsDone         chan struct{} // Closed after Unregister returns.
	logger              *slog.Logger
	metrics             *metrics
	consumersMap        map[string]*consumer // jobType -> consumer
	dbType              DBType
	consumersMapMutex   sync.RWMutex // guard consumersMap //nolint:revive // Field name is descriptive

	// How often to poll database for new jobs. Default is 100ms.
	// Individual consumers may override this using WithConsumerPollInterval.
	defaultPollInterval time.Duration
	// Zero means twice the job timeout of each consumer. See WithDefaultClaimTimeout.
	defaultClaimTimeout time.Duration

	// Default maximum duration a job can run before being considered timed out. Default is 15 minutes.
	// Individual consumers may override this using WithConsumerJobTimeout.
	defaultJobTimeout time.Duration

	// Default number of concurrent workers per consumer. Default is min(runtime.NumCPU(), runtime.GOMAXPROCS(0)).
	// Individual consumers may override this using WithConsumerConcurrency.
	defaultConcurrency uint16

	// Default number of jobs to fetch in advance per consumer.
	// Zero means the concurrency of each consumer. See WithDefaultPrefetchCount.
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

	// Set by Shutdown, under consumersMapMutex. Consume fails once it is.
	shutDown bool
}

type job struct {
	// ClaimToken shows that this consumer owns the job. A write fails if the jobs table has a different token.
	ClaimToken string
	// ClaimExpiresAt is the last time this consumer can start the job. It is a little before the expiry in the jobs table.
	ClaimExpiresAt time.Time
	JobType        string
	CreatedAt      time.Time
	// ScheduledAt is when the job became due, by the database clock.
	ScheduledAt  time.Time
	TraceContext map[string]string
	Payload      []byte
	ID           int64
	RetryCount   uint32
}

// DeadLetterJob represents a job that has been moved to the dead letter queue
type DeadLetterJob struct {
	JobType   string
	CreatedAt time.Time
	// TraceContext? Does it make sense to bring it along?
	FailureReason string
	FailedAt      time.Time
	Payload       []byte
	OriginalID    int64 // This is the primary key now
	RetryCount    uint32
}

// New creates a new JobsQueue
func New(db *sql.DB, dbType DBType, opts ...NewOption) (JobsQueue, error) {
	dbDriver, err := getDriver(db, dbType)
	if err != nil {
		return nil, err
	}

	q := &sqlq{
		db:                              db,
		dbType:                          dbType,
		driver:                          dbDriver,
		consumersMap:                    make(map[string]*consumer),
		consumersMapMutex:               sync.RWMutex{},
		shutDown:                        false,
		defaultPollInterval:             100 * time.Millisecond,
		defaultConcurrency:              defaultConcurrency,
		defaultPrefetchCount:            0,
		defaultMaxRetries:               defaultMaxRetries,
		defaultJobTimeout:               defaultJobTimeout,
		defaultClaimTimeout:             0,
		defaultBackoffFunc:              exponentialBackoff,
		defaultCleanupProcessedInterval: defaultCleanupProcessedInterval,
		defaultCleanupProcessedAge:      defaultCleanupProcessedAge,
		defaultCleanupDLQInterval:       defaultCleanupDLQInterval,
		defaultCleanupDLQAge:            defaultCleanupDLQAge,
		defaultCleanupBatch:             defaultCleanupBatch,
		tracer:                          otel.Tracer(tracerName),
		meterProvider:                   otel.GetMeterProvider(),
		metricsRegistration:             nil,
		metricsCtx:                      nil,
		cancelMetrics:                   nil,
		metricsDone:                     nil,
		metrics:                         nil,
		logger:                          slog.Default(),
	}

	for _, o := range opts {
		o(q)
	}

	if err := q.setUpMetrics(); err != nil {
		return nil, err
	}

	return q, nil
}

func (q *sqlq) Run(ctx context.Context) error {
	ctx, span := q.tracer.Start(ctx, "sqlq.init_schema")
	defer span.End()

	if err := q.driver.initSchema(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "schema setup failed")
		return fmt.Errorf("failed to set up queue schema: %w", err)
	}
	return nil
}

// Publish adds a new job to the queue
func (q *sqlq) Publish(ctx context.Context, jobType string, payload any, opts ...PublishOption) error {
	ctx, span := q.tracer.Start(ctx, "sqlq.publish",
		trace.WithAttributes(attribute.String("sqlq.job_type", jobType)),
		trace.WithSpanKind(trace.SpanKindProducer),
	)
	defer span.End()

	return q.PublishTx(ctx, nil, jobType, payload, opts...)
}

// PublishTx adds a new job to the queue within an existing transaction.
// It never commits or rolls back tx. If you created tx, you must finish it;
// if a handler or hook received tx from the queue, the queue finishes it.
// A nil tx publishes immediately.
func (q *sqlq) PublishTx(ctx context.Context, tx *sql.Tx, jobType string, payload any, opts ...PublishOption) error {
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

	err = q.driver.insertJob(ctx, tx, jobType, payloadBytes, options.delay, traceContextMap)
	if err != nil {
		// The driver method should record the specific DB error in its span. We record a higher-level error here.
		span.RecordError(err)
		return fmt.Errorf("failed to insert job: %w", err)
	}
	q.metrics.published.Add(ctx, 1, metric.WithAttributes(attribute.String(jobTypeAttr, jobType)))

	return nil
}

// Consume registers a handler for a specific job type.
// The queue commits the handler's transaction together with job completion on success,
// and rolls it back on failure. Handlers must not commit or roll back the transaction.
// Do not defer tx.Rollback either. Return nil on success or an error to abort.
// ctx passed to handler will be a child of ctx passed to Consume.
// Registering multiple handlers for same jobType will cause ErrDuplicateConsumer.
func (q *sqlq) Consume(
	ctx context.Context,
	jobType string,
	handler func(ctx context.Context, tx *sql.Tx, payloadBytes []byte) error,
	opts ...ConsumerOption,
) error {
	if handler == nil {
		return fmt.Errorf("%w: cannot consume %s", ErrNilHandler, jobType)
	}
	consCtx, cancel := context.WithCancel(ctx)

	cons := &consumer{
		jobType: jobType,
		handler: handler,
		// Inject dependencies from sqlq
		db:      q.db,
		driver:  q.driver,
		tracer:  q.tracer,
		metrics: q.metrics,
		logger:  q.logger.With(slog.String("job_type", jobType)),
		// Set defaults from sqlq. Those might be overridden by opts later
		concurrency:              q.defaultConcurrency,
		prefetchCount:            q.defaultPrefetchCount,
		maxRetries:               q.defaultMaxRetries,
		pollInterval:             q.defaultPollInterval,
		jobTimeout:               q.defaultJobTimeout,
		claimTimeout:             q.defaultClaimTimeout,
		claims:                   make(map[string]heldClaim),
		backoffFunc:              q.defaultBackoffFunc,
		cleanupBatch:             q.defaultCleanupBatch,
		cleanupProcessedInterval: q.defaultCleanupProcessedInterval,
		cleanupProcessedAge:      q.defaultCleanupProcessedAge,
		cleanupDLQInterval:       q.defaultCleanupDLQInterval,
		cleanupDLQAge:            q.defaultCleanupDLQAge,
		asyncPushEnabled:         false,
		asyncPushMaxRPM:          0,
		onDeadLetter:             nil,
		// Initialize consumer-specific fields
		jobsChan:    nil, // Will be initialized below
		fetchWg:     sync.WaitGroup{},
		workerWg:    sync.WaitGroup{},
		claimsMutex: sync.Mutex{},
		ctx:         consCtx,
		cancel:      cancel,
	}

	// Apply consumer-specific options
	for _, o := range opts {
		o(cons)
	}

	// Options sanity checks and adjustments
	if cons.backoffFunc == nil {
		cancel()
		return fmt.Errorf("%w: cannot consume %s", ErrNilBackoffFunc, jobType)
	}
	// Prefetch follows concurrency unless set. A smaller explicit value is raised:
	// it would only leave workers idle between polls.
	if cons.prefetchCount < cons.concurrency {
		cons.prefetchCount = cons.concurrency
	}
	if cons.claimTimeout == 0 {
		// Leaves about as much claim time for the buffer as for the handler.
		cons.claimTimeout = max(2*cons.jobTimeout, minClaimTimeout(cons.jobTimeout))
	}
	if cons.claimTimeout < minClaimTimeout(cons.jobTimeout) {
		cancel()
		return fmt.Errorf("%w: claim timeout %s, job timeout %s, at least %s needed",
			ErrClaimTimeoutTooShort, cons.claimTimeout, cons.jobTimeout, minClaimTimeout(cons.jobTimeout))
	}
	if cons.asyncPushEnabled && q.dbType != DBTypeSQLite {
		cancel()
		return fmt.Errorf("%w: %s", ErrPushNotSupported, q.dbType)
	}

	// It makes sense to have channel be of size `prefetchCount`
	// since it allows channel to essentially never be empty
	// in cases where `prefetchCount` > `concurrency`
	cons.jobsChan = make(chan job, cons.prefetchCount)

	q.consumersMapMutex.Lock()
	if q.shutDown {
		q.consumersMapMutex.Unlock()
		cancel()
		return fmt.Errorf("%w: cannot consume %s", ErrShutDown, jobType)
	}
	if _, exists := q.consumersMap[jobType]; exists {
		q.consumersMapMutex.Unlock()
		cancel()
		return fmt.Errorf("%w: %s", ErrDuplicateConsumer, jobType)
	}
	q.consumersMap[jobType] = cons
	cons.start()
	q.consumersMapMutex.Unlock()

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
		attribute.Int64("sqlq.original_job_id", originalJobID),
	))
	defer span.End()

	err := q.driver.requeueDeadLetterJob(ctx, originalJobID)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

// Shutdown stops the job processing. Consume fails with ErrShutDown from then on.
func (q *sqlq) Shutdown(ctx context.Context) error {
	q.consumersMapMutex.Lock()
	firstShutdown := !q.shutDown
	q.shutDown = true
	consumers := slices.Collect(maps.Values(q.consumersMap))
	q.consumersMapMutex.Unlock()

	// Collection may be waiting for a connection held by a handler. Cancel both
	// before unregistering, which can wait for active collection to finish.
	for _, cons := range consumers {
		cons.cancel()
	}
	q.cancelMetrics()
	if firstShutdown {
		go func() {
			defer close(q.metricsDone)
			if err := q.metricsRegistration.Unregister(); err != nil {
				q.logger.ErrorContext(ctx, "sqlq: failed to unregister the metrics callback", slog.Any("error", err))
			}
		}()
	}

	errs := make([]error, len(consumers))
	var wg sync.WaitGroup
	wg.Go(func() { <-q.metricsDone })
	for i, cons := range consumers {
		wg.Go(func() { errs[i] = cons.shutdown(ctx) })
	}
	if err := waitContext(ctx, &wg); err != nil {
		return err
	}

	for _, err := range errs {
		if err != nil {
			return err // Each one is ctx's error.
		}
	}
	return nil
}
