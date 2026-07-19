// Package store is the only part of taskorbit that talks to Postgres. It
// is the queue: task_runs rows are claimed, executed, completed or failed
// through the methods here, and every guarantee the rest of the system
// relies on (exactly-once claiming, crash recovery) is enforced by the SQL
// in this package, not by application-level coordination.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("store: not found")

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// CreateJob inserts a new job definition. For 'cron' jobs, NextRunAt must
// already be computed by the caller (see internal/cronparse) so the
// scheduler has an immediate starting point.
func (s *Store) CreateJob(ctx context.Context, job Job) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO jobs (name, job_type, payload, schedule_type, cron_expr, run_at, next_run_at, enabled, max_retries)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id`,
		job.Name, job.JobType, job.Payload, job.ScheduleType, job.CronExpr, job.RunAt, job.NextRunAt,
		job.Enabled, job.MaxRetries,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert job: %w", err)
	}
	return id, nil
}

func (s *Store) GetJob(ctx context.Context, id int64) (*Job, error) {
	var j Job
	err := s.pool.QueryRow(ctx, `
		SELECT id, name, job_type, payload, schedule_type, cron_expr, run_at, next_run_at,
		       enabled, max_retries, created_at
		FROM jobs WHERE id = $1`, id,
	).Scan(&j.ID, &j.Name, &j.JobType, &j.Payload, &j.ScheduleType, &j.CronExpr, &j.RunAt,
		&j.NextRunAt, &j.Enabled, &j.MaxRetries, &j.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job %d: %w", id, err)
	}
	return &j, nil
}

func (s *Store) ListJobs(ctx context.Context) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, job_type, payload, schedule_type, cron_expr, run_at, next_run_at,
		       enabled, max_retries, created_at
		FROM jobs ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.Name, &j.JobType, &j.Payload, &j.ScheduleType, &j.CronExpr,
			&j.RunAt, &j.NextRunAt, &j.Enabled, &j.MaxRetries, &j.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan job row: %w", err)
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// DueOnceJobs returns enabled one-off jobs whose run_at has arrived and
// that don't yet have a task_run — i.e. haven't been enqueued yet.
func (s *Store) DueOnceJobs(ctx context.Context, now time.Time) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT j.id, j.name, j.job_type, j.payload, j.schedule_type, j.cron_expr, j.run_at,
		       j.next_run_at, j.enabled, j.max_retries, j.created_at
		FROM jobs j
		WHERE j.enabled AND j.schedule_type = 'once' AND j.run_at <= $1
		  AND NOT EXISTS (SELECT 1 FROM task_runs t WHERE t.job_id = j.id)`, now)
	if err != nil {
		return nil, fmt.Errorf("query due once jobs: %w", err)
	}
	defer rows.Close()
	return scanJobs(rows)
}

// DueCronJobs returns enabled cron jobs whose next_run_at has arrived.
func (s *Store) DueCronJobs(ctx context.Context, now time.Time) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, job_type, payload, schedule_type, cron_expr, run_at,
		       next_run_at, enabled, max_retries, created_at
		FROM jobs
		WHERE enabled AND schedule_type = 'cron' AND next_run_at <= $1`, now)
	if err != nil {
		return nil, fmt.Errorf("query due cron jobs: %w", err)
	}
	defer rows.Close()
	return scanJobs(rows)
}

func scanJobs(rows pgx.Rows) ([]Job, error) {
	var jobs []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.Name, &j.JobType, &j.Payload, &j.ScheduleType, &j.CronExpr,
			&j.RunAt, &j.NextRunAt, &j.Enabled, &j.MaxRetries, &j.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan job row: %w", err)
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// EnqueueTaskRun creates a task_run for the given firing time. The unique
// constraint on (job_id, scheduled_for) makes this safe to call more than
// once for the same firing — ON CONFLICT DO NOTHING means a duplicate
// scheduler tick, or a second scheduler replica, can never enqueue the
// same firing twice. Returns whether a new row was actually inserted.
func (s *Store) EnqueueTaskRun(ctx context.Context, jobID int64, scheduledFor time.Time, maxRetries int) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO task_runs (job_id, scheduled_for, max_retries)
		VALUES ($1, $2, $3)
		ON CONFLICT (job_id, scheduled_for) DO NOTHING`,
		jobID, scheduledFor, maxRetries)
	if err != nil {
		return false, fmt.Errorf("enqueue task run for job %d: %w", jobID, err)
	}
	return tag.RowsAffected() > 0, nil
}

func (s *Store) UpdateJobNextRun(ctx context.Context, jobID int64, nextRunAt time.Time) error {
	_, err := s.pool.Exec(ctx, `UPDATE jobs SET next_run_at = $2 WHERE id = $1`, jobID, nextRunAt)
	if err != nil {
		return fmt.Errorf("update next_run_at for job %d: %w", jobID, err)
	}
	return nil
}

// ClaimNext atomically claims the single best task_run to work on: either
// a new PENDING one that's due, or a RUNNING one whose lease expired
// (meaning whoever held it before is presumed dead). FOR UPDATE SKIP
// LOCKED means concurrent callers never block on each other and never
// see the same row — see ARCHITECTURE.md for why this is airtight under
// concurrency. Returns (nil, nil) if there's nothing to claim.
func (s *Store) ClaimNext(ctx context.Context, workerID string, leaseDuration time.Duration) (*TaskRun, error) {
	now := time.Now().UTC()
	leaseExpiresAt := now.Add(leaseDuration)

	row := s.pool.QueryRow(ctx, `
		WITH claimable AS (
			SELECT id FROM task_runs
			WHERE (status = 'PENDING' AND scheduled_for <= $3)
			   OR (status = 'RUNNING' AND lease_expires_at < $3)
			ORDER BY scheduled_for
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE task_runs
		SET status = 'RUNNING', claimed_by = $1, lease_expires_at = $2,
		    attempt = attempt + 1, updated_at = $3
		FROM claimable
		WHERE task_runs.id = claimable.id
		RETURNING task_runs.id, task_runs.job_id, task_runs.scheduled_for, task_runs.status,
		          task_runs.claimed_by, task_runs.lease_expires_at, task_runs.attempt,
		          task_runs.max_retries, task_runs.last_error, task_runs.created_at, task_runs.updated_at`,
		workerID, leaseExpiresAt, now)

	var t TaskRun
	err := row.Scan(&t.ID, &t.JobID, &t.ScheduledFor, &t.Status, &t.ClaimedBy, &t.LeaseExpiresAt,
		&t.Attempt, &t.MaxRetries, &t.LastError, &t.CreatedAt, &t.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim next task run: %w", err)
	}
	return &t, nil
}

// RenewLease extends a held lease. ok is false if the row no longer
// belongs to this worker (e.g. its lease already expired and got reclaimed
// by someone else) — the caller must treat that as "stop working on this,
// you no longer own it."
func (s *Store) RenewLease(ctx context.Context, taskID int64, workerID string, leaseDuration time.Duration) (bool, error) {
	newExpiry := time.Now().UTC().Add(leaseDuration)
	tag, err := s.pool.Exec(ctx, `
		UPDATE task_runs
		SET lease_expires_at = $3, updated_at = now()
		WHERE id = $1 AND claimed_by = $2 AND status = 'RUNNING'`,
		taskID, workerID, newExpiry)
	if err != nil {
		return false, fmt.Errorf("renew lease for task %d: %w", taskID, err)
	}
	return tag.RowsAffected() > 0, nil
}

func (s *Store) CompleteTask(ctx context.Context, taskID int64, workerID string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE task_runs
		SET status = 'SUCCEEDED', lease_expires_at = NULL, updated_at = now()
		WHERE id = $1 AND claimed_by = $2`, taskID, workerID)
	if err != nil {
		return fmt.Errorf("complete task %d: %w", taskID, err)
	}
	return nil
}

// FailTask records a failed attempt. If attempts remain, the task goes
// back to PENDING with scheduled_for pushed out by retryDelay (the
// backoff), ready for anyone to claim again. Once attempts are exhausted
// it moves to DEAD_LETTER and stays there.
func (s *Store) FailTask(ctx context.Context, taskID int64, workerID string, errMsg string, retryDelay time.Duration) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin fail-task tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var attempt, maxRetries int
	err = tx.QueryRow(ctx, `
		SELECT attempt, max_retries FROM task_runs
		WHERE id = $1 AND claimed_by = $2 FOR UPDATE`, taskID, workerID,
	).Scan(&attempt, &maxRetries)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("read task %d for failure handling: %w", taskID, err)
	}

	if attempt >= maxRetries {
		_, err = tx.Exec(ctx, `
			UPDATE task_runs
			SET status = 'DEAD_LETTER', last_error = $3, lease_expires_at = NULL, updated_at = now()
			WHERE id = $1 AND claimed_by = $2`, taskID, workerID, errMsg)
	} else {
		nextAttemptAt := time.Now().UTC().Add(retryDelay)
		_, err = tx.Exec(ctx, `
			UPDATE task_runs
			SET status = 'PENDING', last_error = $3, scheduled_for = $4,
			    claimed_by = NULL, lease_expires_at = NULL, updated_at = now()
			WHERE id = $1 AND claimed_by = $2`, taskID, workerID, errMsg, nextAttemptAt)
	}
	if err != nil {
		return fmt.Errorf("update task %d after failure: %w", taskID, err)
	}

	return tx.Commit(ctx)
}

func (s *Store) GetTaskRun(ctx context.Context, id int64) (*TaskRun, error) {
	var t TaskRun
	err := s.pool.QueryRow(ctx, `
		SELECT id, job_id, scheduled_for, status, claimed_by, lease_expires_at, attempt,
		       max_retries, last_error, created_at, updated_at
		FROM task_runs WHERE id = $1`, id,
	).Scan(&t.ID, &t.JobID, &t.ScheduledFor, &t.Status, &t.ClaimedBy, &t.LeaseExpiresAt,
		&t.Attempt, &t.MaxRetries, &t.LastError, &t.CreatedAt, &t.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get task run %d: %w", id, err)
	}
	return &t, nil
}

func (s *Store) ListTaskRunsForJob(ctx context.Context, jobID int64) ([]TaskRun, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, job_id, scheduled_for, status, claimed_by, lease_expires_at, attempt,
		       max_retries, last_error, created_at, updated_at
		FROM task_runs WHERE job_id = $1 ORDER BY scheduled_for DESC`, jobID)
	if err != nil {
		return nil, fmt.Errorf("list task runs for job %d: %w", jobID, err)
	}
	defer rows.Close()

	var runs []TaskRun
	for rows.Next() {
		var t TaskRun
		if err := rows.Scan(&t.ID, &t.JobID, &t.ScheduledFor, &t.Status, &t.ClaimedBy,
			&t.LeaseExpiresAt, &t.Attempt, &t.MaxRetries, &t.LastError, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan task run row: %w", err)
		}
		runs = append(runs, t)
	}
	return runs, rows.Err()
}

// UpsertHeartbeat records that a worker is alive. Called periodically by
// every worker process, independent of which (if any) task it currently
// holds a lease on.
func (s *Store) UpsertHeartbeat(ctx context.Context, workerID, hostname string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO workers (id, hostname, last_heartbeat_at, status)
		VALUES ($1, $2, now(), 'ALIVE')
		ON CONFLICT (id) DO UPDATE SET last_heartbeat_at = now(), status = 'ALIVE'`,
		workerID, hostname)
	if err != nil {
		return fmt.Errorf("upsert heartbeat for worker %s: %w", workerID, err)
	}
	return nil
}

// ReapDeadWorkers marks workers whose heartbeat has gone stale as DEAD and
// force-expires the lease on anything they were running, so the next
// ClaimNext call reclaims it immediately rather than waiting out the full
// lease duration. This is a belt-and-suspenders optimization on top of
// per-task lease expiry, which is what actually guarantees recovery even
// if this reaper never ran at all.
func (s *Store) ReapDeadWorkers(ctx context.Context, staleAfter time.Duration) ([]string, error) {
	cutoff := time.Now().UTC().Add(-staleAfter)

	rows, err := s.pool.Query(ctx, `
		UPDATE workers SET status = 'DEAD'
		WHERE status = 'ALIVE' AND last_heartbeat_at < $1
		RETURNING id`, cutoff)
	if err != nil {
		return nil, fmt.Errorf("reap dead workers: %w", err)
	}
	var deadIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan dead worker id: %w", err)
		}
		deadIDs = append(deadIDs, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(deadIDs) > 0 {
		_, err = s.pool.Exec(ctx, `
			UPDATE task_runs SET lease_expires_at = now()
			WHERE claimed_by = ANY($1) AND status = 'RUNNING'`, deadIDs)
		if err != nil {
			return nil, fmt.Errorf("force-expire leases for dead workers: %w", err)
		}
	}

	return deadIDs, nil
}
