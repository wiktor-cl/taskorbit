// Package api implements the REST surface for managing jobs and viewing
// task run status, using only net/http (Go 1.22+'s ServeMux method/path
// pattern routing needs no third-party router).
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/wiktor-cl/taskorbit/internal/cronparse"
	"github.com/wiktor-cl/taskorbit/internal/observability"
	"github.com/wiktor-cl/taskorbit/internal/store"
)

type API struct {
	store  *store.Store
	pool   *pgxpool.Pool
	logger *slog.Logger
}

func New(s *store.Store, pool *pgxpool.Pool, logger *slog.Logger) *API {
	if logger == nil {
		logger = slog.Default()
	}
	return &API{store: s, pool: pool, logger: logger}
}

func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", a.createJob)
	mux.HandleFunc("GET /jobs", a.listJobs)
	mux.HandleFunc("GET /jobs/{id}", a.getJob)
	mux.HandleFunc("GET /jobs/{id}/runs", a.listTaskRuns)
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.HandleFunc("GET /readyz", a.readyz)
	mux.Handle("GET /metrics", promhttp.Handler())

	var handler http.Handler = mux
	handler = observability.MetricsMiddleware(handler)
	handler = observability.LoggingMiddleware(a.logger)(handler)
	handler = observability.CorrelationIDMiddleware(handler)
	return handler
}

func (a *API) createJob(w http.ResponseWriter, r *http.Request) {
	var req CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}

	jobType := store.JobType(req.JobType)
	if jobType != store.JobTypeLog && jobType != store.JobTypeHTTP {
		writeError(w, http.StatusBadRequest, "job_type must be 'log' or 'http'")
		return
	}

	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}

	maxRetries := req.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}

	job := store.Job{
		Name:       req.Name,
		JobType:    jobType,
		Payload:    payload,
		Enabled:    true,
		MaxRetries: maxRetries,
	}

	switch store.ScheduleType(req.ScheduleType) {
	case store.ScheduleOnce:
		if req.RunAt == nil {
			writeError(w, http.StatusBadRequest, "run_at is required for schedule_type 'once'")
			return
		}
		runAt := req.RunAt.UTC()
		job.ScheduleType = store.ScheduleOnce
		job.RunAt = &runAt

	case store.ScheduleCron:
		if req.CronExpr == "" {
			writeError(w, http.StatusBadRequest, "cron_expr is required for schedule_type 'cron'")
			return
		}
		schedule, err := cronparse.Parse(req.CronExpr)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid cron_expr: "+err.Error())
			return
		}
		next, err := schedule.Next(time.Now().UTC())
		if err != nil {
			writeError(w, http.StatusBadRequest, "cron_expr: "+err.Error())
			return
		}
		cronExpr := req.CronExpr
		job.ScheduleType = store.ScheduleCron
		job.CronExpr = &cronExpr
		job.NextRunAt = &next

	default:
		writeError(w, http.StatusBadRequest, "schedule_type must be 'once' or 'cron'")
		return
	}

	id, err := a.store.CreateJob(r.Context(), job)
	if err != nil {
		a.logger.Error("create job", "error", err, "correlation_id", observability.CorrelationID(r.Context()))
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}

	created, err := a.store.GetJob(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "job created but failed to load it back")
		return
	}
	writeJSON(w, http.StatusCreated, jobToResponse(*created))
}

func (a *API) listJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := a.store.ListJobs(r.Context())
	if err != nil {
		a.logger.Error("list jobs", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list jobs")
		return
	}
	responses := make([]JobResponse, len(jobs))
	for i, j := range jobs {
		responses[i] = jobToResponse(j)
	}
	writeJSON(w, http.StatusOK, responses)
}

func (a *API) getJob(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	job, err := a.store.GetJob(r.Context(), id)
	if err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		a.logger.Error("get job", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to load job")
		return
	}
	writeJSON(w, http.StatusOK, jobToResponse(*job))
}

func (a *API) listTaskRuns(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	if _, err := a.store.GetJob(r.Context(), id); err == store.ErrNotFound {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}

	runs, err := a.store.ListTaskRunsForJob(r.Context(), id)
	if err != nil {
		a.logger.Error("list task runs", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list task runs")
		return
	}
	responses := make([]TaskRunResponse, len(runs))
	for i, run := range runs {
		responses[i] = taskRunToResponse(run)
	}
	writeJSON(w, http.StatusOK, responses)
}

func (a *API) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) readyz(w http.ResponseWriter, r *http.Request) {
	if err := a.pool.Ping(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "database unreachable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func parseID(raw string) (int64, error) {
	return strconv.ParseInt(raw, 10, 64)
}
