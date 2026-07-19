package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/wiktor-cl/taskorbit/internal/api"
	"github.com/wiktor-cl/taskorbit/internal/store"
	"github.com/wiktor-cl/taskorbit/internal/testsupport"
)

func newServer(t *testing.T) *httptest.Server {
	t.Helper()
	pool := testsupport.NewTestPool(t)
	s := store.New(pool)
	a := api.New(s, pool, nil)
	server := httptest.NewServer(a.Routes())
	t.Cleanup(server.Close)
	return server
}

func TestCreateJob_OnceSchedule(t *testing.T) {
	server := newServer(t)

	body := `{"name":"say-hi","job_type":"log","payload":{"message":"hi"},"schedule_type":"once","run_at":"` +
		time.Now().Add(time.Hour).UTC().Format(time.RFC3339) + `"}`

	resp, err := http.Post(server.URL+"/jobs", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /jobs: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	var created api.JobResponse
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if created.Name != "say-hi" || created.ScheduleType != "once" {
		t.Fatalf("unexpected job response: %+v", created)
	}
}

func TestCreateJob_CronSchedule_ComputesNextRunAt(t *testing.T) {
	server := newServer(t)

	body := `{"name":"every-minute","job_type":"log","payload":{"message":"tick"},"schedule_type":"cron","cron_expr":"* * * * *"}`
	resp, err := http.Post(server.URL+"/jobs", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /jobs: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	var created api.JobResponse
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if created.NextRunAt == nil {
		t.Fatal("expected next_run_at to be computed for a cron job")
	}
}

func TestCreateJob_InvalidCronExpr(t *testing.T) {
	server := newServer(t)

	body := `{"name":"bad","job_type":"log","schedule_type":"cron","cron_expr":"not a cron expr"}`
	resp, err := http.Post(server.URL+"/jobs", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /jobs: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid cron_expr, got %d", resp.StatusCode)
	}
}

func TestCreateJob_MissingRunAtForOnceSchedule(t *testing.T) {
	server := newServer(t)

	body := `{"name":"bad","job_type":"log","schedule_type":"once"}`
	resp, err := http.Post(server.URL+"/jobs", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /jobs: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing run_at, got %d", resp.StatusCode)
	}
}

func TestGetJob_NotFound(t *testing.T) {
	server := newServer(t)

	resp, err := http.Get(server.URL + "/jobs/999999")
	if err != nil {
		t.Fatalf("GET /jobs/999999: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestListJobsAndTaskRuns(t *testing.T) {
	server := newServer(t)

	body := `{"name":"list-me","job_type":"log","payload":{"message":"x"},"schedule_type":"once","run_at":"` +
		time.Now().Add(time.Hour).UTC().Format(time.RFC3339) + `"}`
	resp, err := http.Post(server.URL+"/jobs", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /jobs: %v", err)
	}
	var created api.JobResponse
	_ = json.NewDecoder(resp.Body).Decode(&created)
	_ = resp.Body.Close()

	listResp, err := http.Get(server.URL + "/jobs")
	if err != nil {
		t.Fatalf("GET /jobs: %v", err)
	}
	defer func() { _ = listResp.Body.Close() }()
	var jobs []api.JobResponse
	if err := json.NewDecoder(listResp.Body).Decode(&jobs); err != nil {
		t.Fatalf("decode job list: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}

	runsResp, err := http.Get(server.URL + "/jobs/" + strconv.FormatInt(created.ID, 10) + "/runs")
	if err != nil {
		t.Fatalf("GET runs: %v", err)
	}
	defer func() { _ = runsResp.Body.Close() }()
	if runsResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", runsResp.StatusCode)
	}
	var runs []api.TaskRunResponse
	if err := json.NewDecoder(runsResp.Body).Decode(&runs); err != nil {
		t.Fatalf("decode runs: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("expected no task runs yet for an unenqueued one-off job, got %d", len(runs))
	}
}

func TestHealthAndReadiness(t *testing.T) {
	server := newServer(t)

	healthResp, err := http.Get(server.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer func() { _ = healthResp.Body.Close() }()
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", healthResp.StatusCode)
	}

	readyResp, err := http.Get(server.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	defer func() { _ = readyResp.Body.Close() }()
	if readyResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", readyResp.StatusCode)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	server := newServer(t)

	resp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}
