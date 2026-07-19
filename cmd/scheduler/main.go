// Command scheduler polls for due jobs, enqueues task runs for them, and
// reaps workers that have stopped sending heartbeats.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/wiktor-cl/taskorbit/internal/config"
	"github.com/wiktor-cl/taskorbit/internal/migrate"
	"github.com/wiktor-cl/taskorbit/internal/scheduler"
	"github.com/wiktor-cl/taskorbit/internal/store"
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
	sch := scheduler.New(s, logger, config.SchedulerPollInterval(), config.WorkerStaleAfter())

	logger.Info("scheduler starting",
		"poll_interval", config.SchedulerPollInterval(),
		"worker_stale_after", config.WorkerStaleAfter())

	if err := sch.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error("scheduler stopped with error", "error", err)
		os.Exit(1)
	}
	logger.Info("scheduler shut down gracefully")
}
