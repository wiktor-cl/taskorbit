package store

import "time"

type JobType string

const (
	JobTypeLog  JobType = "log"
	JobTypeHTTP JobType = "http"
)

type ScheduleType string

const (
	ScheduleOnce ScheduleType = "once"
	ScheduleCron ScheduleType = "cron"
)

type TaskStatus string

const (
	StatusPending    TaskStatus = "PENDING"
	StatusRunning    TaskStatus = "RUNNING"
	StatusSucceeded  TaskStatus = "SUCCEEDED"
	StatusFailed     TaskStatus = "FAILED"
	StatusDeadLetter TaskStatus = "DEAD_LETTER"
)

// Job is a scheduling definition: what to run, and when. It never holds
// execution state — that lives entirely in TaskRun.
type Job struct {
	ID           int64
	Name         string
	JobType      JobType
	Payload      []byte
	ScheduleType ScheduleType
	CronExpr     *string
	RunAt        *time.Time
	NextRunAt    *time.Time
	Enabled      bool
	MaxRetries   int
	CreatedAt    time.Time
}

// TaskRun is one firing of a Job — the unit workers actually claim and
// execute. A cron Job accumulates many TaskRuns over time; a one-off Job
// gets exactly one.
type TaskRun struct {
	ID             int64
	JobID          int64
	ScheduledFor   time.Time
	Status         TaskStatus
	ClaimedBy      *string
	LeaseExpiresAt *time.Time
	Attempt        int
	MaxRetries     int
	LastError      *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
