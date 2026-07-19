// Package observability provides HTTP middleware for correlation-ID
// propagation and Prometheus request metrics, shared by the API server.
package observability

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

type contextKey string

const correlationIDKey contextKey = "correlation_id"

const correlationIDHeader = "X-Correlation-Id"

// CorrelationID extracts the correlation ID stored on the request context
// by CorrelationIDMiddleware, or "" if none is present.
func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationIDKey).(string)
	return id
}

// CorrelationIDMiddleware assigns every request an ID (reusing one
// supplied by the caller, if any), stores it on the request context, and
// echoes it back on the response — so a client can quote it and every log
// line for that request can carry it.
func CorrelationIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(correlationIDHeader)
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set(correlationIDHeader, id)
		ctx := context.WithValue(r.Context(), correlationIDKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

var (
	httpRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "taskorbit_http_requests_total",
		Help: "Total HTTP requests handled by the API, by route and status code.",
	}, []string{"route", "method", "status"})

	httpRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "taskorbit_http_request_duration_seconds",
		Help:    "HTTP request latency, by route and method.",
		Buckets: prometheus.DefBuckets,
	}, []string{"route", "method"})
)

func init() {
	prometheus.MustRegister(httpRequestsTotal, httpRequestDuration)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// MetricsMiddleware records request count and latency labeled by route
// pattern (r.Pattern, from Go's ServeMux — stable even with path
// parameters, unlike the raw URL path).
func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		route := r.Pattern
		if route == "" {
			route = r.URL.Path
		}
		httpRequestsTotal.WithLabelValues(route, r.Method, http.StatusText(rec.status)).Inc()
		httpRequestDuration.WithLabelValues(route, r.Method).Observe(time.Since(start).Seconds())
	})
}

// LoggingMiddleware logs one line per request, with the correlation ID
// attached so it can be grepped alongside whatever else that request
// touched (worker logs share the same convention).
func LoggingMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)

			logger.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"duration_ms", time.Since(start).Milliseconds(),
				"correlation_id", CorrelationID(r.Context()),
			)
		})
	}
}
