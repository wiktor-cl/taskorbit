package execution_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/wiktor-cl/taskorbit/internal/execution"
	"github.com/wiktor-cl/taskorbit/internal/store"
)

func TestLogExecutor_WritesMessage(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	executor := execution.NewLogExecutor(logger)

	err := executor.Execute(context.Background(), execution.Request{
		TaskRunID: 42,
		JobType:   store.JobTypeLog,
		Payload:   []byte(`{"message":"hello from a test"}`),
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(buf.String(), "hello from a test") {
		t.Errorf("expected log output to contain the message, got: %s", buf.String())
	}
}

func TestLogExecutor_InvalidPayload(t *testing.T) {
	executor := execution.NewLogExecutor(nil)
	err := executor.Execute(context.Background(), execution.Request{
		Payload: []byte(`not json`),
	})
	if err == nil {
		t.Fatal("expected an error for invalid payload, got nil")
	}
}

func TestHTTPExecutor_SuccessAndIdempotencyHeader(t *testing.T) {
	var gotIdempotencyKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotIdempotencyKey = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	executor := execution.NewHTTPExecutor(server.Client())
	err := executor.Execute(context.Background(), execution.Request{
		TaskRunID: 7,
		JobType:   store.JobTypeHTTP,
		Payload:   []byte(`{"url":"` + server.URL + `","method":"POST"}`),
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gotIdempotencyKey == "" {
		t.Error("expected an Idempotency-Key header to be sent")
	}
}

func TestHTTPExecutor_NonSuccessStatusIsAnError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	executor := execution.NewHTTPExecutor(server.Client())
	err := executor.Execute(context.Background(), execution.Request{
		Payload: []byte(`{"url":"` + server.URL + `"}`),
	})
	if err == nil {
		t.Fatal("expected an error for a 500 response, got nil")
	}
}

func TestRegistry_DispatchesByJobType(t *testing.T) {
	registry := execution.NewRegistry()
	var called bool
	registry.Register(store.JobTypeLog, fakeExecutor{onExecute: func() { called = true }})

	err := registry.Execute(context.Background(), execution.Request{JobType: store.JobTypeLog})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !called {
		t.Error("expected the registered executor to be called")
	}
}

func TestRegistry_UnknownJobTypeIsAnError(t *testing.T) {
	registry := execution.NewRegistry()
	err := registry.Execute(context.Background(), execution.Request{JobType: "unknown"})
	if err == nil {
		t.Fatal("expected an error for an unregistered job type, got nil")
	}
}

type fakeExecutor struct {
	onExecute func()
}

func (f fakeExecutor) Execute(context.Context, execution.Request) error {
	f.onExecute()
	return nil
}
