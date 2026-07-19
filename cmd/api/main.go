// Command api serves the REST surface for managing jobs and viewing task
// run status.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/wiktor-cl/taskorbit/internal/api"
	"github.com/wiktor-cl/taskorbit/internal/config"
	"github.com/wiktor-cl/taskorbit/internal/migrate"
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
	a := api.New(s, pool, logger)

	server := &http.Server{
		Addr:    ":" + config.APIPort(),
		Handler: a.Routes(),
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("graceful shutdown", "error", err)
		}
	}()

	logger.Info("api starting", "port", config.APIPort())
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("api server stopped with error", "error", err)
		os.Exit(1)
	}
	logger.Info("api shut down gracefully")
}
