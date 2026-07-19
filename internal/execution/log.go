package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
)

// LogExecutor is the simplest possible job: write a message to the
// structured logger. Its payload is {"message": "..."}.
type LogExecutor struct {
	Logger *slog.Logger
}

func NewLogExecutor(logger *slog.Logger) LogExecutor {
	if logger == nil {
		logger = slog.Default()
	}
	return LogExecutor{Logger: logger}
}

func (e LogExecutor) Execute(_ context.Context, req Request) error {
	var payload struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return fmt.Errorf("log executor: invalid payload: %w", err)
	}
	e.Logger.Info("log job executed", "task_run_id", req.TaskRunID, "message", payload.Message)
	return nil
}
