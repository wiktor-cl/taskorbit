package worker_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wiktor-cl/taskorbit/internal/execution"
	"github.com/wiktor-cl/taskorbit/internal/store"
	"github.com/wiktor-cl/taskorbit/internal/testsupport"
	"github.com/wiktor-cl/taskorbit/internal/worker"
)

func ptr[T any](v T) *T { return &v }

// countingLogExecutor wraps the real log behavior but also counts
// invocations, so the crash-recovery test can assert the job's actual
// work ran exactly once — not just that the row ended up SUCCEEDED.
type countingExecutor struct {
	count *atomic.Int32
}

func (e countingExecutor) Execute(_ context.Context, _ execution.Request) error {
	e.count.Add(1)
	return nil
}

// TestWorker_RecoversTaskFromCrashedWorker is the worker-level version of
// the core crash-recovery guarantee: a worker that claims a task and then
// stops entirely (simulating a crash — no renewal, no completion) must
// not lose that task. A second, healthy worker eventually reclaims it
// once the lease expires and runs it to completion, exactly once.
func TestWorker_RecoversTaskFromCrashedWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testsupport.NewTestPool(t)
	s := store.New(pool)

	jobID, err := s.CreateJob(ctx, store.Job{
		Name: "crash-recovery-job", JobType: store.JobTypeLog, Payload: []byte(`{"message":"hi"}`),
		ScheduleType: store.ScheduleOnce, RunAt: ptr(time.Now().UTC().Add(-time.Minute)),
		Enabled: true, MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if _, err := s.EnqueueTaskRun(ctx, jobID, *job.RunAt, 3); err != nil {
		t.Fatalf("enqueue task run: %v", err)
	}

	// Simulate worker-a crashing immediately after claiming: it takes the
	// lease and then simply never does anything else again — no renewal,
	// no completion, no failure report. A real process death looks exactly
	// like this from the database's point of view.
	shortLease := 300 * time.Millisecond
	crashedClaim, err := s.ClaimNext(ctx, "worker-a-about-to-crash", shortLease)
	if err != nil {
		t.Fatalf("worker-a claim: %v", err)
	}
	if crashedClaim == nil {
		t.Fatal("expected worker-a to claim the task")
	}

	var executionCount atomic.Int32
	registry := execution.NewRegistry()
	registry.Register(store.JobTypeLog, countingExecutor{count: &executionCount})

	w := worker.New(worker.Config{
		ID:                "worker-b-healthy",
		Hostname:          "test-host",
		Concurrency:       2,
		PollInterval:      50 * time.Millisecond,
		LeaseDuration:     5 * time.Second,
		HeartbeatInterval: time.Second,
	}, s, registry, nil)

	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(workerCtx)
	}()

	deadline := time.Now().Add(20 * time.Second)
	for {
		got, err := s.GetTaskRun(ctx, crashedClaim.ID)
		if err != nil {
			t.Fatalf("get task run: %v", err)
		}
		if got.Status == store.StatusSucceeded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("task never reached SUCCEEDED (still %s) after worker-a crashed and worker-b should have recovered it", got.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}

	stopWorker()
	<-done

	final, err := s.GetTaskRun(ctx, crashedClaim.ID)
	if err != nil {
		t.Fatalf("get final task run: %v", err)
	}
	if final.Status != store.StatusSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", final.Status)
	}
	// attempt 1 = worker-a's crashed claim, attempt 2 = worker-b's
	// successful reclaim.
	if final.Attempt != 2 {
		t.Fatalf("expected attempt 2 (one crashed claim + one successful reclaim), got %d", final.Attempt)
	}
	if got := executionCount.Load(); got != 1 {
		t.Fatalf("expected the job to actually execute exactly once, got %d executions", got)
	}
}

func TestWorker_GracefulShutdown_FinishesInFlightTaskBeforeExiting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testsupport.NewTestPool(t)
	s := store.New(pool)

	jobID, err := s.CreateJob(ctx, store.Job{
		Name: "graceful-shutdown-job", JobType: store.JobTypeLog, Payload: []byte(`{"message":"hi"}`),
		ScheduleType: store.ScheduleOnce, RunAt: ptr(time.Now().UTC().Add(-time.Minute)),
		Enabled: true, MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if _, err := s.EnqueueTaskRun(ctx, jobID, *job.RunAt, 3); err != nil {
		t.Fatalf("enqueue task run: %v", err)
	}

	registry := execution.NewRegistry()
	registry.Register(store.JobTypeLog, execution.NewLogExecutor(nil))

	w := worker.New(worker.Config{
		ID: "worker-graceful", Hostname: "test-host", Concurrency: 1,
		PollInterval: 20 * time.Millisecond, LeaseDuration: 5 * time.Second, HeartbeatInterval: time.Second,
	}, s, registry, nil)

	workerCtx, stopWorker := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(workerCtx)
	}()

	// Give it a moment to actually claim and run the (very fast) task,
	// then signal shutdown — Run must still return promptly since
	// nothing is in flight anymore.
	time.Sleep(200 * time.Millisecond)
	stopWorker()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not shut down within 5s of context cancellation")
	}

	runs, err := s.ListTaskRunsForJob(ctx, jobID)
	if err != nil {
		t.Fatalf("list task runs: %v", err)
	}
	if len(runs) != 1 || runs[0].Status != store.StatusSucceeded {
		t.Fatalf("expected the single task run to have succeeded before shutdown, got %+v", runs)
	}
}
