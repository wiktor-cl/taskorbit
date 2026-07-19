// Package execution runs the actual work behind a job — this project's
// two demo job types, "log" (write a message to the structured logger)
// and "http" (call an HTTP endpoint), plus the Registry that dispatches
// to the right one by job type.
package execution

import (
	"context"
	"fmt"

	"github.com/wiktor-cl/taskorbit/internal/store"
)

// Request is everything an Executor needs: which task run this is (used
// to make outbound calls idempotent — see HTTPExecutor), what kind of job,
// and its payload.
type Request struct {
	TaskRunID int64
	JobType   store.JobType
	Payload   []byte
}

type Executor interface {
	Execute(ctx context.Context, req Request) error
}

// Registry dispatches a Request to the Executor registered for its
// JobType. It is not safe for concurrent Register calls, but Execute is
// safe to call concurrently once setup is complete — the underlying map
// is never mutated after construction in normal use.
type Registry struct {
	executors map[store.JobType]Executor
}

func NewRegistry() *Registry {
	return &Registry{executors: make(map[store.JobType]Executor)}
}

func (r *Registry) Register(jobType store.JobType, executor Executor) {
	r.executors[jobType] = executor
}

func (r *Registry) Execute(ctx context.Context, req Request) error {
	executor, ok := r.executors[req.JobType]
	if !ok {
		return fmt.Errorf("execution: no executor registered for job type %q", req.JobType)
	}
	return executor.Execute(ctx, req)
}
