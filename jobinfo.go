package sqlq

import (
	"context"
	"time"
)

// JobInfo describes the job currently being handled. It is available to handlers and
// dead-letter hooks via JobInfoFromContext, and is also passed to dead-letter hooks directly.
type JobInfo struct {
	CreatedAt  time.Time
	JobType    string
	ID         int64
	MaxRetries int32 // -1 means unlimited
	RetryCount uint16
}

// IsFinalAttempt reports whether a failure of the current attempt will move the job
// to the dead letter queue instead of scheduling another retry.
func (ji JobInfo) IsFinalAttempt() bool {
	return ji.MaxRetries != infiniteRetries && int32(ji.RetryCount) >= ji.MaxRetries
}

type jobInfoCtxKey struct{}

// JobInfoFromContext returns information about the job being handled.
// ok is false if ctx does not belong to a job handler or dead-letter hook invocation.
func JobInfoFromContext(ctx context.Context) (JobInfo, bool) {
	info, ok := ctx.Value(jobInfoCtxKey{}).(JobInfo)
	return info, ok
}

func contextWithJobInfo(ctx context.Context, info JobInfo) context.Context {
	return context.WithValue(ctx, jobInfoCtxKey{}, info)
}

func (cons *consumer) jobInfo(j *job) JobInfo {
	return JobInfo{
		JobType:    cons.jobType,
		CreatedAt:  j.CreatedAt,
		ID:         j.ID,
		MaxRetries: cons.maxRetries,
		RetryCount: j.RetryCount,
	}
}
