package sqlq

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

type JobsQueue interface {
	Publish(ctx context.Context, jobType string, payload any, opts ...PublishOption) error
	PublishTx(ctx context.Context, tx *sql.Tx, jobType string, payload any, opts ...PublishOption) error
	Subscribe(ctx context.Context, jobType string, consumerName string, f func(ctx context.Context, tx *sql.Tx, payloadBytes []byte) error, opts ...SubscriptionOption)
	GetDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]deadLetterJob, error)
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

var (
	defaultConcurrency = min(runtime.NumCPU(), runtime.GOMAXPROCS(0))
	defaultPrefetch    = defaultConcurrency
	defaultMaxRetries  = 3
)

// sqlq implements the JobsQueue interface using SQL databases
type sqlq struct {
	db               *sql.DB
	dbType           DBType
	driver           Driver
	subscribers      map[string][]*subscriber
	subscribersMutex sync.RWMutex
	shutdown         chan struct{}
	wg               sync.WaitGroup
}

type subscriber struct {
	jobType        string
	consumerName   string
	handler        func(ctx context.Context, tx *sql.Tx, payloadBytes []byte) error
	concurrency    int
	prefetchCount  int
	pollInterval   time.Duration
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

// deadLetterJob represents a job that has been moved to the dead letter queue
type deadLetterJob struct {
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
func New(db *sql.DB, dbType DBType) (JobsQueue, error) {
	driver, err := GetDriver(db, dbType)
	if err != nil {
		return nil, err
	}

	q := &sqlq{
		db:          db,
		dbType:      dbType,
		driver:      driver,
		subscribers: make(map[string][]*subscriber),
		shutdown:    make(chan struct{}),
	}

	// Initialize the database schema
	if err := q.driver.InitSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return q, nil
}

// Publish adds a new job to the queue
func (q *sqlq) Publish(ctx context.Context, jobType string, payload any, opts ...PublishOption) error {
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
func (q *sqlq) PublishTx(ctx context.Context, tx *sql.Tx, jobType string, payload any, opts ...PublishOption) error {
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
		currentTime, err := q.driver.GetCurrentTime()
		if err != nil {
			return fmt.Errorf("failed to get database time: %w", err)
		}
		scheduledAt := currentTime.Add(options.delay)

		err = q.driver.InsertDelayedJob(jobType, payloadBytes, scheduledAt, options.maxRetries)
		if err != nil {
			return fmt.Errorf("failed to insert delayed job: %w", err)
		}
	} else {
		err = q.driver.InsertJob(jobType, payloadBytes, options.maxRetries)
		if err != nil {
			return fmt.Errorf("failed to insert job: %w", err)
		}
	}

	return nil
}

// Subscribe registers a handler for a specific job type
func (q *sqlq) Subscribe(
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
		pollInterval:  1 * time.Second, // Default poll interval
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
		pollInterval:   options.pollInterval,
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
func (q *sqlq) workerLoop(sub *subscriber) {
	defer sub.workerWg.Done()

	for {
		select {
		case <-sub.ctx.Done():
			return
		case job, ok := <-sub.processingJobs:
			if !ok {
				return
			}

			log.Printf("Took job from the channel: %d", job.ID)

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
					// Move to dead letter queue
					if moveErr := q.driver.MoveToDeadLetterQueue(job.ID, err.Error()); moveErr != nil {
						log.Printf("Failed to move job %d to dead letter queue: %v", job.ID, moveErr)
					} else {
						log.Printf("Job %d exceeded maximum retries (%d) and was moved to dead letter queue: %v",
							job.ID, job.MaxRetries, err)
					}
				}
				continue
			}

			// Mark as processed
			err = q.driver.MarkJobProcessed(job.ID, sub.consumerName)
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
func (q *sqlq) retryJob(ctx context.Context, job job, errorMsg string) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Mark job as failed and increment retry count
	err = q.driver.MarkJobFailed(job.ID, errorMsg)
	if err != nil {
		return fmt.Errorf("failed to mark job as failed: %w", err)
	}

	// Calculate exponential backoff
	backoff := time.Duration(math.Pow(2, float64(job.RetryCount))) * time.Second

	// Get current database time and calculate next run time with backoff
	currentTime, err := q.driver.GetCurrentTime()
	if err != nil {
		return fmt.Errorf("failed to get database time: %w", err)
	}
	nextRunTime := currentTime.Add(backoff)

	err = q.driver.RescheduleJob(job.ID, nextRunTime)
	if err != nil {
		return fmt.Errorf("failed to reschedule job: %w", err)
	}

	return tx.Commit()
}

// Run starts the job processing loop
func (q *sqlq) Run() {
	q.wg.Add(1)

	go func() {
		defer q.wg.Done()
		
		// Each subscriber will be polled at their own interval
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
func (q *sqlq) GetDeadLetterJobs(ctx context.Context, jobType string, limit int) ([]deadLetterJob, error) {
	return q.driver.GetDeadLetterJobs(jobType, limit)
}

// RequeueDeadLetterJob moves a job from the dead letter queue back to the main queue
func (q *sqlq) RequeueDeadLetterJob(ctx context.Context, dlqID int64) error {
	return q.driver.RequeueDeadLetterJob(dlqID)
}

// Shutdown stops the job processing
func (q *sqlq) Shutdown() {
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
func (q *sqlq) processJobs() error {
	q.subscribersMutex.RLock()
	defer q.subscribersMutex.RUnlock()

	// If no subscribers, nothing to do
	if len(q.subscribers) == 0 {
		return nil
	}

	// Process for each job type with subscribers
	for jobType, subs := range q.subscribers {
		for _, sub := range subs {
			// Check if it's time to poll for this subscriber
			now := time.Now()
			
			// Use a separate goroutine for each subscriber to respect their individual poll intervals
			go func(sub *subscriber, jobType string) {
				if err := q.processJobsForSubscriber(sub); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf(
						"Error processing jobs for subscriber %s of type %s: %v",
						sub.consumerName, jobType, err,
					)
				}
				
				// Sleep for the poll interval
				time.Sleep(sub.pollInterval)
			}(sub, jobType)
		}
	}

	return nil
}

// processJobsForSubscriber fetches jobs for a specific subscriber and sends them to worker goroutines
func (q *sqlq) processJobsForSubscriber(sub *subscriber) error {
	// Find jobs that haven't been processed by this consumer
	jobs, err := q.driver.GetJobsForConsumer(sub.consumerName, sub.jobType, sub.prefetchCount)
	if err != nil {
		return fmt.Errorf("failed to query jobs: %w", err)
	}

	for _, job := range jobs {
		select {
		case <-sub.ctx.Done():
			return nil
		case sub.processingJobs <- job:
			log.Printf("Put job %d for %s in the channel", job.ID, sub.consumerName)
		}
	}

	return nil
}
