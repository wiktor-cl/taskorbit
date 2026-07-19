package store_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wiktor-cl/taskorbit/internal/store"
	"github.com/wiktor-cl/taskorbit/internal/testsupport"
)

func seedJob(t *testing.T, s *store.Store) int64 {
	t.Helper()
	id, err := s.CreateJob(context.Background(), store.Job{
		Name:         "test-job",
		JobType:      store.JobTypeLog,
		Payload:      []byte(`{}`),
		ScheduleType: store.ScheduleOnce,
		RunAt:        ptr(time.Now().UTC()),
		Enabled:      true,
		MaxRetries:   3,
	})
	if err != nil {
		t.Fatalf("seed job: %v", err)
	}
	return id
}

func ptr[T any](v T) *T { return &v }

// TestClaimNext_ConcurrentWorkers_ExactlyOnce is the core correctness
// proof for this whole project: with many goroutines hammering ClaimNext
// at the same time against a fixed pool of task runs, every single one
// must be claimed by exactly one goroutine — none claimed twice, none
// left behind. Run with -race.
func TestClaimNext_ConcurrentWorkers_ExactlyOnce(t *testing.T) {
	pool := testsupport.NewTestPool(t)
	s := store.New(pool)
	ctx := context.Background()

	jobID := seedJob(t, s)

	const numTasks = 300
	const numWorkers = 40

	base := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < numTasks; i++ {
		scheduledFor := base.Add(time.Duration(i) * time.Millisecond)
		created, err := s.EnqueueTaskRun(ctx, jobID, scheduledFor, 3)
		if err != nil {
			t.Fatalf("enqueue task run %d: %v", i, err)
		}
		if !created {
			t.Fatalf("task run %d unexpectedly already existed", i)
		}
	}

	var (
		mu      sync.Mutex
		claimed = make(map[int64]int) // task ID -> number of times claimed
	)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		workerID := fmt.Sprintf("worker-%d", w)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				task, err := s.ClaimNext(ctx, workerID, time.Minute)
				if err != nil {
					t.Errorf("claim next (worker %s): %v", workerID, err)
					return
				}
				if task == nil {
					return // nothing left to claim
				}

				mu.Lock()
				claimed[task.ID]++
				mu.Unlock()

				if err := s.CompleteTask(ctx, task.ID, workerID); err != nil {
					t.Errorf("complete task %d (worker %s): %v", task.ID, workerID, err)
				}
			}
		}()
	}
	wg.Wait()

	if len(claimed) != numTasks {
		t.Fatalf("expected %d distinct tasks claimed, got %d", numTasks, len(claimed))
	}
	for id, count := range claimed {
		if count != 1 {
			t.Errorf("task %d was claimed %d times, want exactly 1", id, count)
		}
	}
}

// TestClaimNext_ReclaimsExpiredLease proves the crash-recovery mechanism
// at the store level: a task claimed with a short lease that's never
// renewed becomes claimable again once the lease expires, and the second
// claimant sees an incremented attempt count.
func TestClaimNext_ReclaimsExpiredLease(t *testing.T) {
	pool := testsupport.NewTestPool(t)
	s := store.New(pool)
	ctx := context.Background()

	jobID := seedJob(t, s)
	_, err := s.EnqueueTaskRun(ctx, jobID, time.Now().UTC().Add(-time.Minute), 3)
	if err != nil {
		t.Fatalf("enqueue task run: %v", err)
	}

	first, err := s.ClaimNext(ctx, "worker-a", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if first == nil {
		t.Fatal("expected a task to be claimable")
		return
	}
	if first.Attempt != 1 {
		t.Fatalf("expected attempt 1 on first claim, got %d", first.Attempt)
	}

	// worker-a "crashes": never renews the lease, never completes.
	// Nothing should be claimable until the lease actually expires.
	immediate, err := s.ClaimNext(ctx, "worker-b", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("immediate re-claim attempt: %v", err)
	}
	if immediate != nil {
		t.Fatalf("expected no claimable task before lease expiry, got task %d", immediate.ID)
	}

	time.Sleep(100 * time.Millisecond)

	second, err := s.ClaimNext(ctx, "worker-b", time.Minute)
	if err != nil {
		t.Fatalf("second claim after lease expiry: %v", err)
	}
	if second == nil {
		t.Fatal("expected the task to be reclaimable after lease expiry")
		return
	}
	if second.ID != first.ID {
		t.Fatalf("expected to reclaim the same task %d, got %d", first.ID, second.ID)
	}
	if second.Attempt != 2 {
		t.Fatalf("expected attempt 2 after reclaim, got %d", second.Attempt)
	}

	if err := s.CompleteTask(ctx, second.ID, "worker-b"); err != nil {
		t.Fatalf("complete reclaimed task: %v", err)
	}
}

func TestFailTask_RetriesThenDeadLetters(t *testing.T) {
	pool := testsupport.NewTestPool(t)
	s := store.New(pool)
	ctx := context.Background()

	jobID := seedJob(t, s)
	_, err := s.EnqueueTaskRun(ctx, jobID, time.Now().UTC().Add(-time.Minute), 2)
	if err != nil {
		t.Fatalf("enqueue task run: %v", err)
	}

	// maxRetries is 2, so: attempt 1 fails -> retry (PENDING); attempt 2
	// fails -> exhausted -> DEAD_LETTER.
	var taskID int64
	for attempt := 1; attempt <= 2; attempt++ {
		task, err := s.ClaimNext(ctx, "worker-a", time.Minute)
		if err != nil {
			t.Fatalf("claim attempt %d: %v", attempt, err)
		}
		if task == nil {
			t.Fatalf("expected a claimable task on attempt %d", attempt)
			return
		}
		taskID = task.ID

		if err := s.FailTask(ctx, task.ID, "worker-a", "boom", 0); err != nil {
			t.Fatalf("fail task attempt %d: %v", attempt, err)
		}

		got, err := s.GetTaskRun(ctx, taskID)
		if err != nil {
			t.Fatalf("get task run after failure %d: %v", attempt, err)
		}

		if attempt < 2 {
			if got.Status != store.StatusPending {
				t.Fatalf("attempt %d: expected PENDING (retry remaining), got %s", attempt, got.Status)
			}
		} else {
			if got.Status != store.StatusDeadLetter {
				t.Fatalf("attempt %d: expected DEAD_LETTER after exhausting retries, got %s", attempt, got.Status)
			}
		}
	}

	final, err := s.ClaimNext(ctx, "worker-b", time.Minute)
	if err != nil {
		t.Fatalf("claim after dead-letter: %v", err)
	}
	if final != nil {
		t.Fatalf("dead-lettered task must not be claimable again, but claimed %d", final.ID)
	}
}
