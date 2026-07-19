package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// HTTPExecutor calls an HTTP endpoint. Payload is
// {"url": "...", "method": "GET", "body": "..."} (method defaults to GET).
// Every call carries an Idempotency-Key derived from the task run ID, so a
// well-behaved downstream endpoint can dedupe an at-least-once retry the
// same way this project's own APIs expect callers to.
type HTTPExecutor struct {
	Client *http.Client
}

func NewHTTPExecutor(client *http.Client) HTTPExecutor {
	if client == nil {
		client = http.DefaultClient
	}
	return HTTPExecutor{Client: client}
}

type httpPayload struct {
	URL    string `json:"url"`
	Method string `json:"method"`
	Body   string `json:"body"`
}

func (e HTTPExecutor) Execute(ctx context.Context, req Request) error {
	var payload httpPayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return fmt.Errorf("http executor: invalid payload: %w", err)
	}
	if payload.URL == "" {
		return fmt.Errorf("http executor: payload missing url")
	}

	method := payload.Method
	if method == "" {
		method = http.MethodGet
	}

	httpReq, err := http.NewRequestWithContext(ctx, method, payload.URL, strings.NewReader(payload.Body))
	if err != nil {
		return fmt.Errorf("http executor: build request: %w", err)
	}
	httpReq.Header.Set("Idempotency-Key", fmt.Sprintf("taskorbit-task-%d", req.TaskRunID))

	resp, err := e.Client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("http executor: request failed: %w", err)
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("http executor: %s %s returned status %d", method, payload.URL, resp.StatusCode)
	}
	return nil
}
