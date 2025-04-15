package sqlq

import "time"

// NewOption is an option that is accepted by sqlq.New function
type NewOption func(*sqlq)

// WithPollInterval allows to configure default poll interval
// This value may be overriden per individual consumer
func WithPollInterval(tick time.Duration) NewOption {
	return func(q *sqlq) {
		q.defaultPollInterval = tick
	}
}

// WithBackoffFunc allows to configure default backoff function
func WithBackoffFunc(backoffFunc func(int) time.Duration) NewOption {
	return func(q *sqlq) {
		q.backoffFunc = backoffFunc
	}
}
