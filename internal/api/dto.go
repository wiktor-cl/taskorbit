package api

import (
	"encoding/json"
	"time"

	"github.com/wiktor-cl/taskorbit/internal/store"
)

type CreateJobRequest struct {
	Name         string          `json:"name"`
	JobType      string          `json:"job_type"`
	Payload      json.RawMessage `json:"payload"`
	ScheduleType string          `json:"schedule_type"`
	CronExpr     string          `json:"cron_expr,omitempty"`
	RunAt        *time.Time      `json:"run_at,omitempty"`
	MaxRetries   int             `json:"max_retries,omitempty"`
}

type JobResponse struct {
	ID           int64           `json:"id"`
	Name         string          `json:"name"`
	JobType      string          `json:"job_type"`
	Payload      json.RawMessage `json:"payload"`
	ScheduleType string          `json:"schedule_type"`
	CronExpr     *string         `json:"cron_expr,omitempty"`
	RunAt        *time.Time      `json:"run_at,omitempty"`
	NextRunAt    *time.Time      `json:"next_run_at,omitempty"`
	Enabled      bool            `json:"enabled"`
	MaxRetries   int             `json:"max_retries"`
	CreatedAt    time.Time       `json:"created_at"`
}

func jobToResponse(j store.Job) JobResponse {
	return JobResponse{
		ID:           j.ID,
		Name:         j.Name,
		JobType:      string(j.JobType),
		Payload:      j.Payload,
		ScheduleType: string(j.ScheduleType),
		CronExpr:     j.CronExpr,
		RunAt:        j.RunAt,
		NextRunAt:    j.NextRunAt,
		Enabled:      j.Enabled,
		MaxRetries:   j.MaxRetries,
		CreatedAt:    j.CreatedAt,
	}
}

type TaskRunResponse struct {
	ID             int64      `json:"id"`
	JobID          int64      `json:"job_id"`
	ScheduledFor   time.Time  `json:"scheduled_for"`
	Status         string     `json:"status"`
	ClaimedBy      *string    `json:"claimed_by,omitempty"`
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`
	Attempt        int        `json:"attempt"`
	MaxRetries     int        `json:"max_retries"`
	LastError      *string    `json:"last_error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

func taskRunToResponse(t store.TaskRun) TaskRunResponse {
	return TaskRunResponse{
		ID:             t.ID,
		JobID:          t.JobID,
		ScheduledFor:   t.ScheduledFor,
		Status:         string(t.Status),
		ClaimedBy:      t.ClaimedBy,
		LeaseExpiresAt: t.LeaseExpiresAt,
		Attempt:        t.Attempt,
		MaxRetries:     t.MaxRetries,
		LastError:      t.LastError,
		CreatedAt:      t.CreatedAt,
		UpdatedAt:      t.UpdatedAt,
	}
}
