// Package worker implements the claim -> execute -> complete/retry loop,
// including lease renewal for long-running tasks, periodic heartbeating,
// and graceful shutdown.
package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/wiktor-cl/taskorbit/internal/backoff"
	"github.com/wiktor-cl/taskorbit/internal/execution"
	"github.com/wiktor-cl/taskorbit/internal/store"
)

type Config struct {
	ID                string
	Hostname          string
	Concurrency       int
	PollInterval      time.Duration
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	Backoff           backoff.Config
}

func (c Config) withDefaults() Config {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	if c.PollInterval <= 0 {
		c.PollInterval = time.Second
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = 30 * time.Second
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 10 * time.Second
	}
	if c.Backoff == (backoff.Config{}) {
		c.Backoff = backoff.Default()
	}
	return c
}

type Worker struct {
	cfg       Config
	store     *store.Store
	executors *execution.Registry
	logger    *slog.Logger
}

func New(cfg Config, s *store.Store, executors *execution.Registry, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{cfg: cfg.withDefaults(), store: s, executors: executors, logger: logger}
}

// Run blocks until ctx is cancelled. On cancellation, claim loops stop
// picking up new work but let any task already in progress finish —
// "finish the current task and give up the lease" from a graceful
// shutdown signal's point of view.
func (w *Worker) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.heartbeatLoop(ctx)
	}()

	for i := 0; i < w.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.claimLoop(ctx)
		}()
	}

	wg.Wait()
	return nil
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	if err := w.store.UpsertHeartbeat(ctx, w.cfg.ID, w.cfg.Hostname); err != nil {
		w.logger.Error("initial heartbeat", "worker_id", w.cfg.ID, "error", err)
	}

	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.store.UpsertHeartbeat(ctx, w.cfg.ID, w.cfg.Hostname); err != nil {
				w.logger.Error("heartbeat", "worker_id", w.cfg.ID, "error", err)
			}
		}
	}
}

func (w *Worker) claimLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		task, err := w.store.ClaimNext(ctx, w.cfg.ID, w.cfg.LeaseDuration)
		if err != nil {
			if ctx.Err() != nil {
				// Shutdown cancelled this in-flight claim attempt — expected,
				// not a real failure. The next loop iteration's ctx.Done()
				// case will exit.
				continue
			}
			w.logger.Error("claim next task", "worker_id", w.cfg.ID, "error", err)
			continue
		}
		if task == nil {
			continue
		}

		// Deliberately not derived from ctx: once a task is claimed, it
		// must run to completion (and be reported success/failure) even
		// if a shutdown signal arrives mid-execution, or its lease would
		// be abandoned without ever being released cleanly. It's still
		// bounded, by the lease duration, so a hung execution doesn't
		// block shutdown forever.
		execCtx, cancel := context.WithTimeout(context.Background(), w.cfg.LeaseDuration)
		w.runTask(execCtx, task)
		cancel()
	}
}

func (w *Worker) runTask(ctx context.Context, task *store.TaskRun) {
	renewCtx, cancelRenew := context.WithCancel(context.Background())
	var renewWg sync.WaitGroup
	renewWg.Add(1)
	go func() {
		defer renewWg.Done()
		w.renewLeaseLoop(renewCtx, task.ID)
	}()
	defer func() {
		cancelRenew()
		renewWg.Wait()
	}()

	job, err := w.store.GetJob(ctx, task.JobID)
	var execErr error
	if err != nil {
		execErr = err
	} else {
		execErr = w.executors.Execute(ctx, execution.Request{
			TaskRunID: task.ID,
			JobType:   job.JobType,
			Payload:   job.Payload,
		})
	}

	if execErr != nil {
		delay := w.cfg.Backoff.Delay(task.Attempt)
		if err := w.store.FailTask(ctx, task.ID, w.cfg.ID, execErr.Error(), delay); err != nil {
			w.logger.Error("record task failure", "task_id", task.ID, "error", err)
			return
		}
		w.logger.Warn("task execution failed", "task_id", task.ID, "job_id", task.JobID,
			"attempt", task.Attempt, "error", execErr, "retry_delay", delay)
		return
	}

	if err := w.store.CompleteTask(ctx, task.ID, w.cfg.ID); err != nil {
		w.logger.Error("record task completion", "task_id", task.ID, "error", err)
		return
	}
	w.logger.Info("task completed", "task_id", task.ID, "job_id", task.JobID)
}

// renewLeaseLoop keeps a claimed task's lease alive while it's still being
// worked on. Renewing at half the lease duration leaves margin for the
// renewal call itself to fail once or twice without the lease actually
// expiring underneath the still-running task.
func (w *Worker) renewLeaseLoop(ctx context.Context, taskID int64) {
	interval := w.cfg.LeaseDuration / 2
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := w.store.RenewLease(context.Background(), taskID, w.cfg.ID, w.cfg.LeaseDuration)
			if err != nil {
				w.logger.Error("renew lease", "task_id", taskID, "error", err)
				continue
			}
			if !ok {
				w.logger.Warn("lost lease on task, it was likely reclaimed by another worker", "task_id", taskID)
				return
			}
		}
	}
}
