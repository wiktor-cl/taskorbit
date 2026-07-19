package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/wiktor-cl/taskorbit/internal/scheduler"
	"github.com/wiktor-cl/taskorbit/internal/store"
	"github.com/wiktor-cl/taskorbit/internal/testsupport"
)

func ptr[T any](v T) *T { return &v }

func TestScheduler_EnqueuesDueOnceJobExactlyOnce(t *testing.T) {
	ctx := context.Background()
	pool := testsupport.NewTestPool(t)
	s := store.New(pool)
	sch := scheduler.New(s, nil, time.Second, time.Minute)

	jobID, err := s.CreateJob(ctx, store.Job{
		Name:         "one-off",
		JobType:      store.JobTypeLog,
		Payload:      []byte(`{}`),
		ScheduleType: store.ScheduleOnce,
		RunAt:        ptr(time.Now().UTC().Add(-time.Minute)),
		Enabled:      true,
		MaxRetries:   3,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	sch.Tick(ctx)
	sch.Tick(ctx) // second tick must not double-enqueue

	runs, err := s.ListTaskRunsForJob(ctx, jobID)
	if err != nil {
		t.Fatalf("list task runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected exactly 1 task run after two ticks, got %d", len(runs))
	}
}

func TestScheduler_EnqueuesCronJobAndAdvancesNextRun(t *testing.T) {
	ctx := context.Background()
	pool := testsupport.NewTestPool(t)
	s := store.New(pool)
	sch := scheduler.New(s, nil, time.Second, time.Minute)

	// Truncating "now" down to the minute boundary guarantees firstFiring
	// <= now (so it's due on the first tick) while next_run_at
	// (firstFiring + 1 minute) lands somewhere in (now, now+1min] — always
	// still in the future, so the second tick's "no re-fire yet" check
	// below isn't a coin flip depending on when in the current minute the
	// test happens to run.
	firstFiring := time.Now().UTC().Truncate(time.Minute)
	jobID, err := s.CreateJob(ctx, store.Job{
		Name:         "every-minute",
		JobType:      store.JobTypeLog,
		Payload:      []byte(`{}`),
		ScheduleType: store.ScheduleCron,
		CronExpr:     ptr("* * * * *"),
		NextRunAt:    ptr(firstFiring),
		Enabled:      true,
		MaxRetries:   3,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	sch.Tick(ctx)

	runs, err := s.ListTaskRunsForJob(ctx, jobID)
	if err != nil {
		t.Fatalf("list task runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected exactly 1 task run after first tick, got %d", len(runs))
	}
	if !runs[0].ScheduledFor.Equal(firstFiring) {
		t.Fatalf("expected task run scheduled_for %v, got %v", firstFiring, runs[0].ScheduledFor)
	}

	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	wantNext := firstFiring.Add(time.Minute)
	if !job.NextRunAt.Equal(wantNext) {
		t.Fatalf("expected next_run_at advanced to %v, got %v", wantNext, *job.NextRunAt)
	}

	// A second tick shouldn't fire again yet, since next_run_at is now in
	// the future (unless the clock has genuinely moved a full minute).
	sch.Tick(ctx)
	runs2, err := s.ListTaskRunsForJob(ctx, jobID)
	if err != nil {
		t.Fatalf("list task runs after second tick: %v", err)
	}
	if len(runs2) != 1 {
		t.Fatalf("expected still exactly 1 task run after second tick (next_run_at in future), got %d", len(runs2))
	}
}

func TestScheduler_ReapsDeadWorkersAndExpiresTheirLeases(t *testing.T) {
	ctx := context.Background()
	pool := testsupport.NewTestPool(t)
	s := store.New(pool)
	sch := scheduler.New(s, nil, time.Second, 30*time.Second)

	if err := s.UpsertHeartbeat(ctx, "stale-worker", "host-a"); err != nil {
		t.Fatalf("upsert heartbeat: %v", err)
	}
	// Backdate the heartbeat well past the staleness threshold.
	if _, err := pool.Exec(ctx,
		`UPDATE workers SET last_heartbeat_at = now() - interval '5 minutes' WHERE id = $1`,
		"stale-worker"); err != nil {
		t.Fatalf("backdate heartbeat: %v", err)
	}

	jobID, err := s.CreateJob(ctx, store.Job{
		Name: "held-by-dead-worker", JobType: store.JobTypeLog, Payload: []byte(`{}`),
		ScheduleType: store.ScheduleOnce, RunAt: ptr(time.Now().UTC().Add(-time.Minute)),
		Enabled: true, MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if _, err := s.EnqueueTaskRun(ctx, jobID, *mustRunAt(ctx, t, s, jobID), 3); err != nil {
		t.Fatalf("enqueue task run: %v", err)
	}
	task, err := s.ClaimNext(ctx, "stale-worker", time.Hour) // long lease, only the reaper should force it to expire
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if task == nil {
		t.Fatal("expected a claimable task")
	}

	sch.Tick(ctx)

	// The reaper should have force-expired the lease; a fresh worker
	// should now be able to claim it immediately, without waiting an hour.
	reclaimed, err := s.ClaimNext(ctx, "fresh-worker", time.Minute)
	if err != nil {
		t.Fatalf("claim after reap: %v", err)
	}
	if reclaimed == nil {
		t.Fatal("expected the dead worker's task to be reclaimable after Tick reaped it")
	}
	if reclaimed.ID != task.ID {
		t.Fatalf("expected to reclaim task %d, got %d", task.ID, reclaimed.ID)
	}
}

func mustRunAt(ctx context.Context, t *testing.T, s *store.Store, jobID int64) *time.Time {
	t.Helper()
	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	return job.RunAt
}
