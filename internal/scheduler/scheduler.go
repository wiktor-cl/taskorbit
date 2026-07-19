// Package scheduler turns job definitions into claimable task runs: it
// notices when a one-off job's run_at or a cron job's next_run_at has
// arrived, enqueues a task_run for it, and (for cron) advances the job to
// its next firing time. It also reaps workers that have stopped sending
// heartbeats.
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/wiktor-cl/taskorbit/internal/cronparse"
	"github.com/wiktor-cl/taskorbit/internal/store"
)

type Scheduler struct {
	store            *store.Store
	logger           *slog.Logger
	pollInterval     time.Duration
	workerStaleAfter time.Duration
}

func New(s *store.Store, logger *slog.Logger, pollInterval, workerStaleAfter time.Duration) *Scheduler {
	return &Scheduler{
		store:            s,
		logger:           logger,
		pollInterval:     pollInterval,
		workerStaleAfter: workerStaleAfter,
	}
}

// Run polls on a fixed interval until ctx is cancelled.
func (sch *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(sch.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			sch.Tick(ctx)
		}
	}
}

// Tick runs one scheduling pass: enqueue due jobs, reap dead workers.
// Exported directly so tests can drive it deterministically instead of
// waiting on a real ticker.
func (sch *Scheduler) Tick(ctx context.Context) {
	if err := sch.enqueueDueOnceJobs(ctx); err != nil {
		sch.logf("enqueue due one-off jobs: %v", err)
	}
	if err := sch.enqueueDueCronJobs(ctx); err != nil {
		sch.logf("enqueue due cron jobs: %v", err)
	}
	if err := sch.reapDeadWorkers(ctx); err != nil {
		sch.logf("reap dead workers: %v", err)
	}
}

func (sch *Scheduler) enqueueDueOnceJobs(ctx context.Context) error {
	now := time.Now().UTC()
	jobs, err := sch.store.DueOnceJobs(ctx, now)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		created, err := sch.store.EnqueueTaskRun(ctx, job.ID, *job.RunAt, job.MaxRetries)
		if err != nil {
			sch.logf("enqueue one-off job %d: %v", job.ID, err)
			continue
		}
		if created && sch.logger != nil {
			sch.logger.Info("enqueued one-off job", "job_id", job.ID, "run_at", job.RunAt)
		}
	}
	return nil
}

func (sch *Scheduler) enqueueDueCronJobs(ctx context.Context) error {
	now := time.Now().UTC()
	jobs, err := sch.store.DueCronJobs(ctx, now)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		firing := *job.NextRunAt
		created, err := sch.store.EnqueueTaskRun(ctx, job.ID, firing, job.MaxRetries)
		if err != nil {
			sch.logf("enqueue cron job %d: %v", job.ID, err)
			continue
		}
		if created && sch.logger != nil {
			sch.logger.Info("enqueued cron job firing", "job_id", job.ID, "scheduled_for", firing)
		}

		schedule, err := cronparse.Parse(*job.CronExpr)
		if err != nil {
			sch.logf("parse cron expression for job %d: %v", job.ID, err)
			continue
		}
		next, err := schedule.Next(firing)
		if err != nil {
			sch.logf("compute next run for job %d: %v", job.ID, err)
			continue
		}
		if err := sch.store.UpdateJobNextRun(ctx, job.ID, next); err != nil {
			sch.logf("advance next_run_at for job %d: %v", job.ID, err)
		}
	}
	return nil
}

func (sch *Scheduler) reapDeadWorkers(ctx context.Context) error {
	deadIDs, err := sch.store.ReapDeadWorkers(ctx, sch.workerStaleAfter)
	if err != nil {
		return err
	}
	if sch.logger != nil {
		for _, id := range deadIDs {
			sch.logger.Warn("reaped dead worker, force-expired its leases", "worker_id", id)
		}
	}
	return nil
}

func (sch *Scheduler) logf(format string, args ...any) {
	if sch.logger != nil {
		sch.logger.Error("scheduler tick error: " + fmt.Sprintf(format, args...))
	}
}
