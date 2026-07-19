// Package config loads process configuration from environment variables,
// with sane local-dev defaults so nothing needs to be set to run any of
// the three binaries locally.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// PostgresDSN builds a libpq-style connection string from individual
// DB_* environment variables (all with local-dev defaults), rather than
// requiring one preformatted DSN string.
func PostgresDSN() string {
	host := getEnv("DB_HOST", "localhost")
	port := getEnv("DB_PORT", "5432")
	name := getEnv("DB_NAME", "taskorbit")
	user := getEnv("DB_USER", "taskorbit")
	password := getEnv("DB_PASSWORD", "taskorbit")
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, name)
}

func SchedulerPollInterval() time.Duration {
	return getEnvDuration("SCHEDULER_POLL_INTERVAL", 2*time.Second)
}

func WorkerStaleAfter() time.Duration {
	return getEnvDuration("WORKER_STALE_AFTER", 30*time.Second)
}

func WorkerPollInterval() time.Duration {
	return getEnvDuration("WORKER_POLL_INTERVAL", time.Second)
}

func WorkerLeaseDuration() time.Duration {
	return getEnvDuration("WORKER_LEASE_DURATION", 30*time.Second)
}

func WorkerHeartbeatInterval() time.Duration {
	return getEnvDuration("WORKER_HEARTBEAT_INTERVAL", 10*time.Second)
}

func WorkerConcurrency() int {
	return getEnvInt("WORKER_CONCURRENCY", 4)
}

func APIPort() string {
	return getEnv("API_PORT", "8080")
}

func WorkerID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-host"
	}
	return getEnv("WORKER_ID", fmt.Sprintf("%s-%d", hostname, os.Getpid()))
}
