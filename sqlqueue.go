package sqlqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"runtime"
	"sync"
	"time"
)

// SubscriptionOption defines functional options for Subscribe
type SubscriptionOption func(*subscriptionOptions)

// subscriptionOptions holds configuration for a subscription
type subscriptionOptions struct {
	concurrency   int
	prefetchCount int
}

// WithConcurrency sets the number of concurrent workers for a subscription
func WithConcurrency(n int) SubscriptionOption {
	return func(o *subscriptionOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// WithPrefetchCount sets the number of jobs to prefetch in a single query
func WithPrefetchCount(n int) SubscriptionOption {
	return func(o *subscriptionOptions) {
		if n > 0 {
			o.prefetchCount = n
		}
	}
}

// PublishOption defines functional options for Publish
type PublishOption func(*publishOptions)

// publishOptions holds configuration for publishing a job
type publishOptions struct {
	delay      time.Duration
	maxRetries int
}

// WithDelay schedules a job to run after the specified delay
func WithDelay(delay time.Duration) PublishOption {
	return func(o *publishOptions) {
		if delay > 0 {
			o.delay = delay
		}
	}
}

// WithMaxRetries sets the maximum number of retries for a job
func WithMaxRetries(n int) PublishOption {
	return func(o *publishOptions) {
		if n >= 0 {
			o.maxRetries = n
		}
	}
}

type JobsQueue interface {
	Publish(ctx context.Context, jobType string, payload any, opts ...PublishOption) error
	PublishTx(ctx context.Context, tx *sql.Tx, jobType string, payload any, opts ...PublishOption) error
	Subscribe(ctx context.Context, jobType string, consumerName string, f func(ctx context.Context, tx *sql.Tx, payloadBytes []byte) error, opts ...SubscriptionOption)
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
)

var (
	defaultConcurrency = min(runtime.NumCPU(), runtime.GOMAXPROCS(0))
	defaultPrefetch    = defaultConcurrency
	defaultMaxRetries  = 3
)

// SQLQueue implements the JobsQueue interface using SQL databases
type SQLQueue struct {
	db               *sql.DB
	dbType           DBType
	driver           Driver
	subscribers      map[string][]*subscriber
	subscribersMutex sync.RWMutex
	pollInterval     time.Duration
	shutdown         chan struct{}
	wg               sync.WaitGroup
}

type subscriber struct {
	jobType        string
	consumerName   string
	handler        func(ctx context.Context, tx *sql.Tx, payloadBytes []byte) error
	concurrency    int
	prefetchCount  int
	processingJobs chan job
	workerWg       sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
}

type job struct {
	ID         int64
	JobType    string
	Payload    []byte
	CreatedAt  time.Time
	RetryCount int
	MaxRetries int
}

// NewSQLQueue creates a new SQL-backed job queue
func NewSQLQueue(db *sql.DB, dbType DBType, pollInterval time.Duration) (*SQLQueue, error) {
	if pollInterval == 0 {
		pollInterval = 1 * time.Second
	}

	driver, err := GetDriver(dbType)
	if err != nil {
		return nil, err
	}

	q := &SQLQueue{
		db:           db,
		dbType:       dbType,
		driver:       driver,
		subscribers:  make(map[string][]*subscriber),
		pollInterval: pollInterval,
		shutdown:     make(chan struct{}),
	}

	// Initialize the database schema
	if err := q.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return q, nil
}

// initSchema creates the necessary tables if they don't exist
func (q *SQLQueue) initSchema() error {
	return q.driver.InitSchema(q.db)
}

// Publish adds a new job to the queue
func (q *SQLQueue) Publish(ctx context.Context, jobType string, payload any, opts ...PublishOption) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	if err := q.PublishTx(ctx, tx, jobType, payload, opts...); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// PublishTx adds a new job to the queue within an existing transaction
func (q *SQLQueue) PublishTx(ctx context.Context, tx *sql.Tx, jobType string, payload any, opts ...PublishOption) error {
	options := publishOptions{
		maxRetries: defaultMaxRetries,
	}

	// Apply provided options
	for _, opt := range opts {
		opt(&options)
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	if options.delay > 0 {
		// Get current database time and calculate scheduled time
		currentTime, err := q.driver.GetCurrentTime(q.db)
		if err != nil {
			return fmt.Errorf("failed to get database time: %w", err)
		}
		scheduledAt := currentTime.Add(options.delay)

		err = q.driver.InsertDelayedJob(q.db, jobType, payloadBytes, scheduledAt, options.maxRetries)
		if err != nil {
			return fmt.Errorf("failed to insert delayed job: %w", err)
		}
	} else {
		err = q.driver.InsertJob(q.db, jobType, payloadBytes, options.maxRetries)
		if err != nil {
			return fmt.Errorf("failed to insert job: %w", err)
		}
	}

	return nil
}

// Subscribe registers a handler for a specific job type
func (q *SQLQueue) Subscribe(
	ctx context.Context,
	jobType string,
	consumerName string,
	f func(ctx context.Context, tx *sql.Tx, payloadBytes []byte) error,
	opts ...SubscriptionOption,
) {
	q.subscribersMutex.Lock()
	defer q.subscribersMutex.Unlock()

	// Default options
	options := subscriptionOptions{
		concurrency:   defaultConcurrency,
		prefetchCount: defaultPrefetch,
	}
	// Apply provided options
	for _, opt := range opts {
		opt(&options)
	}
	// Options sanity adjustments
	if options.prefetchCount < options.concurrency {
		options.prefetchCount = options.concurrency
	}

	subCtx, cancel := context.WithCancel(ctx)

	sub := &subscriber{
		jobType:        jobType,
		consumerName:   consumerName,
		handler:        f,
		concurrency:    options.concurrency,
		prefetchCount:  options.prefetchCount,
		processingJobs: make(chan job, options.prefetchCount),
		ctx:            subCtx,
		cancel:         cancel,
	}

	// Start worker goroutines
	for range options.concurrency {
		sub.workerWg.Add(1)
		go q.workerLoop(sub)
	}

	q.subscribers[jobType] = append(q.subscribers[jobType], sub)
}

// workerLoop processes jobs from the processingJobs channel
func (q *SQLQueue) workerLoop(sub *subscriber) {
	defer sub.workerWg.Done()

	for {
		select {
		case <-sub.ctx.Done():
			return
		case job, ok := <-sub.processingJobs:
			if !ok {
				return
			}

			tx, err := q.db.BeginTx(sub.ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
			if err != nil {
				log.Printf("Failed to begin transaction for job %d: %v", job.ID, err)
				continue
			}

			// Process the job
			err = sub.handler(sub.ctx, tx, job.Payload)
			if err != nil {
				log.Printf("Job handler failed for job %d: %v", job.ID, err)
				_ = tx.Rollback()

				// Handle retry logic
				if job.RetryCount < job.MaxRetries {
					if err := q.retryJob(sub.ctx, job, err.Error()); err != nil {
						log.Printf("Failed to schedule retry for job %d: %v", job.ID, err)
					}
				} else {
					log.Printf("Job %d exceeded maximum retries (%d): %v", job.ID, job.MaxRetries, err)
				}
				continue
			}

			// Mark as processed
			err = q.driver.MarkJobProcessed(q.db, job.ID, sub.consumerName)
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("Failed to mark job %d as processed: %v", job.ID, err)
				_ = tx.Rollback()
				continue
			}

			if err := tx.Commit(); err != nil {
				log.Printf("Failed to commit transaction for job %d: %v", job.ID, err)
			}
		}
	}
}

// retryJob increments the retry count and schedules the job to be retried after a backoff period
func (q *SQLQueue) retryJob(ctx context.Context, job job, errorMsg string) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Mark job as failed and increment retry count
	err = q.driver.MarkJobFailed(q.db, job.ID, errorMsg)
	if err != nil {
		return fmt.Errorf("failed to mark job as failed: %w", err)
	}

	// Calculate exponential backoff
	backoff := time.Duration(math.Pow(2, float64(job.RetryCount))) * time.Second

	// Get current database time and calculate next run time with backoff
	currentTime, err := q.driver.GetCurrentTime(q.db)
	if err != nil {
		return fmt.Errorf("failed to get database time: %w", err)
	}
	nextRunTime := currentTime.Add(backoff)

	err = q.driver.RescheduleJob(q.db, job.ID, nextRunTime)
	if err != nil {
		return fmt.Errorf("failed to reschedule job: %w", err)
	}

	return tx.Commit()
}

// Run starts the job processing loop
func (q *SQLQueue) Run() {
	q.wg.Add(1)

	go func() {
		defer q.wg.Done()
		ticker := time.NewTicker(q.pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-q.shutdown:
				return
			case <-ticker.C:
				if err := q.processJobs(); err != nil {
					log.Printf("Error processing jobs: %v", err)
				}
			}
		}
	}()
}

// Shutdown stops the job processing
func (q *SQLQueue) Shutdown() {
	close(q.shutdown)

	// Cancel all subscriber contexts and wait for workers to finish
	q.subscribersMutex.Lock()
	for _, subs := range q.subscribers {
		for _, sub := range subs {
			sub.cancel()
			close(sub.processingJobs)
			sub.workerWg.Wait()
		}
	}
	q.subscribersMutex.Unlock()

	q.wg.Wait()
}

// processJobs polls for and processes available jobs
func (q *SQLQueue) processJobs() error {
	q.subscribersMutex.RLock()
	defer q.subscribersMutex.RUnlock()

	// If no subscribers, nothing to do
	if len(q.subscribers) == 0 {
		return nil
	}

	// Process for each job type with subscribers
	for jobType, subs := range q.subscribers {
		for _, sub := range subs {
			if err := q.processJobsForSubscriber(sub); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf(
					"Error processing jobs for subscriber %s of type %s: %v",
					sub.consumerName, jobType, err,
				)
			}
		}
	}

	return nil
}

// processJobsForSubscriber fetches jobs for a specific subscriber and sends them to worker goroutines
func (q *SQLQueue) processJobsForSubscriber(sub *subscriber) error {
	// Find jobs that haven't been processed by this consumer
	jobs, err := q.driver.GetJobsForConsumer(q.db, sub.consumerName, sub.jobType, sub.prefetchCount)
	if err != nil {
		return fmt.Errorf("failed to query jobs: %w", err)
	}

	for _, job := range jobs {
		select {
		case <-sub.ctx.Done():
			return nil
		case sub.processingJobs <- job:
		}
	}

	return nil
}
