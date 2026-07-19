// Command worker claims task runs and executes them: log jobs and HTTP
// jobs out of the box.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/wiktor-cl/taskorbit/internal/config"
	"github.com/wiktor-cl/taskorbit/internal/execution"
	"github.com/wiktor-cl/taskorbit/internal/migrate"
	"github.com/wiktor-cl/taskorbit/internal/store"
	"github.com/wiktor-cl/taskorbit/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, config.PostgresDSN())
	if err != nil {
		logger.Error("create postgres pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := migrate.Apply(ctx, pool); err != nil {
		logger.Error("apply migrations", "error", err)
		os.Exit(1)
	}

	s := store.New(pool)

	registry := execution.NewRegistry()
	registry.Register(store.JobTypeLog, execution.NewLogExecutor(logger))
	registry.Register(store.JobTypeHTTP, execution.NewHTTPExecutor(http.DefaultClient))

	hostname, _ := os.Hostname()
	w := worker.New(worker.Config{
		ID:                config.WorkerID(),
		Hostname:          hostname,
		Concurrency:       config.WorkerConcurrency(),
		PollInterval:      config.WorkerPollInterval(),
		LeaseDuration:     config.WorkerLeaseDuration(),
		HeartbeatInterval: config.WorkerHeartbeatInterval(),
	}, s, registry, logger)

	logger.Info("worker starting", "worker_id", config.WorkerID(), "concurrency", config.WorkerConcurrency())

	if err := w.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error("worker stopped with error", "error", err)
		os.Exit(1)
	}
	logger.Info("worker shut down gracefully", "worker_id", config.WorkerID())
}
