package sqlq

import (
	"context"
	"time"
)

// JobInfo describes the job currently being handled. It is available to handlers
// via JobInfoFromContext and is passed to dead-letter hooks.
type JobInfo struct {
	JobType    string
	CreatedAt  time.Time
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
// ok is false if ctx does not belong to a job handler invocation.
func JobInfoFromContext(ctx context.Context) (info JobInfo, ok bool) {
	info, ok = ctx.Value(jobInfoCtxKey{}).(JobInfo)
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
