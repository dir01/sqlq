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
)

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
	ID         int64
	JobType    string
	Payload    []byte
	CreatedAt  time.Time
	RetryCount int
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
	}

	for _, o := range opts {
		o(q)
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

	defer tx.Rollback()

	if err := q.PublishTx(ctx, tx, jobType, payload, opts...); err != nil {
		return err
	}

	return tx.Commit()
}

// PublishTx adds a new job to the queue within an existing transaction
func (q *sqlq) PublishTx(ctx context.Context, tx *sql.Tx, jobType string, payload any, opts ...PublishOption) error {
	options := &publishOptions{}

	// Apply provided options
	for _, o := range opts {
		o(options)
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	var scheduledAt time.Time

	if options.delay > 0 {
		// Use a non-zero time to indicate a delay is needed
		scheduledAt = time.Now().Add(options.delay)
	}

	err = q.driver.InsertJob(jobType, payloadBytes, scheduledAt)
	if err != nil {
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

	// Start worker goroutines
	for range cons.concurrency {
		cons.workerWg.Add(1)
		go q.workerLoop(cons)
	}

	q.consumersMap[jobType] = append(q.consumersMap[jobType], cons)
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
				return
			}

			tx, err := q.db.BeginTx(cons.ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
			if err != nil {
				log.Printf("failed to begin transaction")
				continue
			}

			// Process the job
			log.Printf("[%s] Pre handler of job %d", time.Now().String(), job.ID)
			consErr := cons.handler(cons.ctx, tx, job.Payload)
			log.Printf("[%s] Post handler of job %d", time.Now().String(), job.ID)

			if consErr == nil {
				if err = q.driver.MarkJobProcessed(job.ID, cons.consumerName); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("Failed to mark job %d as processed: %v", job.ID, err)
					_ = tx.Rollback()
					continue
				}

				err := tx.Commit()
				if err != nil && !errors.Is(err, sql.ErrTxDone) {
					log.Printf("[%s] Failed to commit transaction for job %d: %v", time.Now().String(), job.ID, err)
				}
			} else {
				_ = tx.Rollback()

				// Handle retry logic
				if job.RetryCount < cons.maxRetries {
					// FIXME: delete
					//log.Printf("Retrying job %d, RetryCount: %d", job.ID, job.RetryCount)
					if err := q.retryJob(cons, job, consErr.Error()); err != nil {
						log.Printf("Failed to schedule retry for job %d: %v", job.ID, err)
					}
					// FIXME: delete
					//log.Printf("Retrying done for job %d", job.ID)
				} else {
					// FIXME: delete
					//log.Printf("Moving job %d to DLQ, RetryCount %d", job.ID, job.RetryCount)
					if moveErr := q.driver.MoveToDeadLetterQueue(job.ID, consErr.Error()); moveErr != nil {
						log.Printf("Failed to move job %d to dead letter queue: %v", job.ID, moveErr)
					} else {
						// FIXME: delete
						// log.Printf("Job %d exceeded maximum retries (%d) and was moved to dead letter queue: %v", job.ID, cons.maxRetries, err)
					}
				}
			}
		}
	}
}

// retryJob increments the retry count and schedules the job to be retried after a backoff period
func (q *sqlq) retryJob(consumer *consumer, job job, errorMsg string) error {
	// Calculate exponential backoff
	bFn := q.backoffFunc
	if consumer.backoffFunc != nil {
		bFn = consumer.backoffFunc
	}
	backoff := bFn(job.RetryCount)

	// Mark job as failed and reschedule in a single operation
	if err := q.driver.MarkJobFailedAndReschedule(job.ID, errorMsg, backoff); err != nil {
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
	return q.driver.GetDeadLetterJobs(jobType, limit)
}

// RequeueDeadLetterJob moves a job from the dead letter queue back to the main queue
func (q *sqlq) RequeueDeadLetterJob(ctx context.Context, dlqID int64) error {
	return q.driver.RequeueDeadLetterJob(dlqID)
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

				if err := q.fetchJobsForConsumer(cons); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf(
						"Error processing jobs for consumer %s of type %s: %v",
						cons.consumerName, jobType, err,
					)
				}

				// Wait for poll interval or context cancellation
				select {
				case <-timer.C:
					// Timer expired, continue to next poll
				case <-cons.ctx.Done():
					// Context was canceled
					return
				}
			}(cons, jobType)
		}
	}

	return nil
}

// fetchJobsForConsumer fetches jobs for a specific consumer and sends them to worker goroutines
func (q *sqlq) fetchJobsForConsumer(cons *consumer) error {
	// FIXME: drop this
	// log.Printf("fetching jobs for consumer %s", cons.consumerName)

	// Find jobs that haven't been processed by this consumer
	jobs, err := q.driver.GetJobsForConsumer(cons.consumerName, cons.jobType, cons.prefetchCount)
	if err != nil {
		return fmt.Errorf("failed to query jobs: %w", err)
	}

	// FIXME: drop this
	// log.Printf("found %d jobs for consumer %s", len(jobs), cons.consumerName)

	for _, job := range jobs {
		select {
		case <-cons.ctx.Done():
			return nil
		case cons.processingJobs <- job:
		}
	}

	return nil
}
